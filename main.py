# -*- coding: utf-8 -*-
"""
FastAPI – Shopee ShortLink + Redis + GCS + Geo (IP) + Predição (CatBoost) + Meta Ads (CAPI)
(versão ajustada: thresholds_cb + TE embutido no meta + cookies fbp/fbc/eid + external_id persistente; event_time/value/currency)
(Aprimorado: pré-filtro UTM; utm_original = base; utm_original_01 = COMPLETA (base + 'Axx' quando existir);
             utm_numbered e subId3 priorizam utm_original_01 completa)

Regras novas:
- Se a UTM terminar com 'A' + dígitos (ex.: 68novolinkA01):
    utm_original     = "68novolink"
    utm_original_01  = "68novolinkA01"   # COMPLETA
    utm_numbered     = "68novolinkA01N{counter}"
    subId3 (Shopee)  = "68novolinkA01"
- Caso não tenha sufixo:
    utm_original     = UTM inteira
    utm_original_01  = ""  (vazio)
    utm_numbered     = "{utm_original}N{counter}"
    subId3 (Shopee)  = utm_original
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json, random
from typing import Optional, Dict, Tuple, List, Any

import requests
import redis
from fastapi import FastAPI, Request, Query, Path, UploadFile, File, Header, HTTPException, Form
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse, HTMLResponse
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode, unquote
from datetime import datetime

# ===================== Config (env) =====================
DEFAULT_TIMEOUT       = float(os.getenv("HTTP_TIMEOUT", "10"))

# GCS
GCS_BUCKET            = os.getenv("GCS_BUCKET")              # ex: "meu-bucket"
GCS_PREFIX            = os.getenv("GCS_PREFIX", "logs/")     # ex: "logs/"
FLUSH_MAX_ROWS        = int(os.getenv("FLUSH_MAX_ROWS", "500"))
FLUSH_MAX_SECONDS     = int(os.getenv("FLUSH_MAX_SECONDS", "30"))

# Admin
ADMIN_TOKEN           = os.getenv("ADMIN_TOKEN", "12345678")

# Redis
REDIS_URL             = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COUNTER_KEY           = os.getenv("UTM_COUNTER_KEY", "utm_counter")
USERDATA_TTL_SECONDS  = int(os.getenv("USERDATA_TTL_SECONDS", "604800"))
USERDATA_KEY_PREFIX   = os.getenv("USERDATA_KEY_PREFIX", "ud:")
GEO_CACHE_TTL_SECONDS = int(os.getenv("GEO_CACHE_TTL_SECONDS", "259200"))  # 3 dias
GEO_CACHE_PREFIX      = os.getenv("GEO_CACHE_PREFIX", "geo:")

VIDEO_ID              = os.getenv("VIDEO_ID", "v15")

# Shopee Affiliate
SHOPEE_APP_ID         = os.getenv("SHOPEE_APP_ID", "")
SHOPEE_APP_SECRET     = os.getenv("SHOPEE_APP_SECRET", "")
SHOPEE_ENDPOINT       = os.getenv("SHOPEE_ENDPOINT", "https://open-api.affiliate.shopee.com.br/graphql")

# Meta Ads (CAPI)
META_PIXEL_ID         = os.getenv("META_PIXEL_ID") or os.getenv("FB_PIXEL_ID")
META_ACCESS_TOKEN     = os.getenv("META_ACCESS_TOKEN") or os.getenv("FB_ACCESS_TOKEN")
META_GRAPH_VERSION    = os.getenv("META_GRAPH_VERSION", "v17.0")
META_TEST_EVENT_CODE  = os.getenv("META_TEST_EVENT_CODE")  # opcional

# Modelos
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR      = os.path.join(BASE_DIR, "models")
os.makedirs(MODELS_DIR, exist_ok=True)
MODEL_PATH      = os.path.join(MODELS_DIR, "model_comprou.cbm")
MODEL_META_PATH = os.path.join(MODELS_DIR, "model_meta_comprou.json")
TE_STATS_PATH   = os.path.join(MODELS_DIR, "te_stats.json")  # opcional (fallback)

# ===================== App / Clients =====================
app = FastAPI(title="Shopee UTM → ShortLink + Geo + Predict + CAPI")

# Redis
r = redis.from_url(REDIS_URL)
try:
    r.persist(COUNTER_KEY)
except Exception:
    pass

# GCS (lazy: só importa client se GCS_BUCKET existir)
_bucket = None
if GCS_BUCKET:
    try:
        from google.cloud import storage
        _gcs_client = storage.Client()
        _bucket = _gcs_client.bucket(GCS_BUCKET)
    except Exception as e:
        print("[GCS] erro ao inicializar:", e)
        _bucket = None

# Sessão HTTP
session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=30, pool_maxsize=100)
session.mount("http://", _adapter)
session.mount("https://", _adapter)

# ===================== Helpers URL / Shopee =====================
SHORT_DOMAINS = ("s.shopee.com.br", "s.shopee.com")
LONG_ALLOWED  = (".shopee.com.br", ".shopee.com", ".xiapiapp.com")

def _fix_scheme(url: str) -> str:
    if url.startswith("https:/") and not url.startswith("https://"):
        return "https://" + url[len("https:/"):]
    if url.startswith("http:/") and not url.startswith("http://"):
        return "http://" + url[len("http:/"):]
    return url

def _is_short_domain(domain: str) -> bool:
    d = (domain or "").lower()
    return any(d.endswith(sd) for sd in SHORT_DOMAINS)

def _is_allowed_long(domain: str) -> bool:
    if not domain:
        return False
    d = domain.strip().lower()
    if ":" in d:
        d = d.split(":", 1)[0]
    if d.startswith("www."):
        d = d[4:]
    apex_ok = d in ("shopee.com.br", "shopee.com", "xiapiapp.com")
    suf_ok  = any(d.endswith(suf.lstrip(".")) or d.endswith(suf) for suf in LONG_ALLOWED)
    return apex_ok or suf_ok

def _resolve_short_follow(url: str) -> str:
    try:
        resp = session.head(url, allow_redirects=True, timeout=DEFAULT_TIMEOUT)
        final_url = resp.url
        if _is_short_domain(urlsplit(final_url).netloc):
            resp = session.get(url, allow_redirects=True, timeout=DEFAULT_TIMEOUT)
            final_url = resp.url
        return final_url
    except Exception:
        return url

def _set_utm_content_preserving_order(url: str, new_value: str) -> str:
    parts = urlsplit(url)
    items = (parts.query or "").split("&") if parts.query else []
    replaced = False
    for i, seg in enumerate(items):
        if seg.startswith("utm_content="):
            items[i] = "utm_content=" + new_value
            replaced = True
            break
    if not replaced:
        items.append("utm_content=" + new_value)
    new_qs = "&".join([s for s in items if s])
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_qs, parts.fragment))

def add_or_update_query_param(raw_url: str, key: str, value: str) -> str:
    parsed = urlsplit(raw_url)
    q = parse_qs(parsed.query, keep_blank_values=True)
    q[key] = [value]
    new_query = urlencode(q, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))

def _sha256_lower(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _extract_shopee_ids(u: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r"/i\.(\d+)\.(\d+)", u)
    if m:
        return m.group(1), m.group(2)
    return None, None

def _build_content_identifiers(origin_url: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    shop_id, item_id = _extract_shopee_ids(origin_url)
    if shop_id and item_id:
        cid = f"{shop_id}.{item_id}"
    else:
        parts = urlsplit(origin_url)
        base = (parts.path or "/") + ("?" + parts.query if parts.query else "")
        cid  = _sha256_lower(base)[:16]
    return [cid], [{"id": cid, "quantity": 1}]

# ===================== AM / IDs =====================
def _gen_fbp(ts: int) -> str:
    return f"fb.1.{ts}.{random.randint(10**15, 10**16 - 1)}"

def _gen_fbc_from_fbp(fbp: str, ts: int) -> str:
    return f"fb.1.{ts}.{_sha256_lower(fbp)[:16]}"

# ===================== CSV buffer =====================
_buffer_lock = threading.Lock()
_buffer_rows: List[List[str]] = []
_last_flush_ts = time.time()

CSV_HEADERS = [
    # tempos e request
    "timestamp","iso_time","ip","user_agent","referrer",
    # urls
    "short_link_in","resolved_url","final_url","category",
    # utm
    "utm_original","utm_original_01","utm_numbered","fbclid","fbp","fbc",
    # device
    "device_name","os_family","os_version",
    # geo
    "geo_status","geo_country","geo_region","geo_state","geo_city","geo_zip","geo_lat","geo_lon","geo_isp","geo_org","geo_asn",
    # features básicas calculadas
    "part_of_day","hour","dow","ref_domain","os_version_num","is_android","is_ios",
    # predição
    "pred_label","p_comprou","p_quase","p_desinteressado",
    # thresholds (do meta do modelo) e flags
    "thr_qvc","thr_atc","is_qvc","is_atc",
    # métricas derivadas
    "pcomprou_pct","thr_qvc_pct","thr_atc_pct","gap_qvc","gap_atc","pct_of_atc",
    # eventos meta
    "meta_event_sent","meta_view_sent"
]

# ===================== Utils =====================
def incr_counter() -> int:
    pipe = r.pipeline()
    pipe.incr(COUNTER_KEY)
    pipe.persist(COUNTER_KEY)
    val, _ = pipe.execute()
    return int(val)

def get_cookie_value(cookie_header: Optional[str], name: str) -> Optional[str]:
    if not cookie_header:
        return None
    try:
        for it in [c.strip() for c in cookie_header.split(";")]:
            if it.startswith(name + "="):
                return it.split("=", 1)[1]
    except Exception:
        pass
    return None

def build_fbc_from_fbclid(fbclid: Optional[str], creation_ts: Optional[int] = None) -> Optional[str]:
    if not fbclid:
        return None
    if creation_ts is None:
        creation_ts = int(time.time())
    return f"fb.1.{creation_ts}.{fbclid}"

def parse_device_info(ua: str):
    ua = ua or "-"
    ua_l = ua.lower()
    os_family, os_version = "Other", "-"
    if re.search(r"iPhone|iPad|iOS", ua, re.I):
        m = re.search(r"(?:iPhone OS|CPU iPhone OS)\s(\d+)", ua)
        os_family, os_version = "iOS", (m.group(1) if m else "-")
    elif re.search(r"Android", ua, re.I):
        m = re.search(r"Android\s(\d+)", ua, re.I)
        os_family, os_version = "Android", (m.group(1) if m else "-")
    elif "Windows" in ua:
        os_family = "Windows"
    elif "Mac OS X" in ua or "Macintosh" in ua:
        os_family = "macOS"
    elif "Linux" in ua:
        os_family = "Linux"
    device_name = "iPhone" if "iphone" in ua_l else ("Android" if "android" in ua_l else "Desktop")
    return device_name, os_family, os_version

def extract_utm_content_from_url(u: str) -> str:
    try:
        qs = urlsplit(u).query
        if not qs:
            return ""
        q = parse_qs(qs, keep_blank_values=True)
        return (q.get("utm_content") or [""])[0]
    except:
        return ""

def resolve_short_link(short_url: str) -> Tuple[str, Dict[str, Optional[str]]]:
    current = short_url
    final_url = short_url
    subids = {}
    try:
        resp = session.get(current, allow_redirects=False, timeout=DEFAULT_TIMEOUT,
                           headers={"User-Agent": "Mozilla/5.0 (resolver/1.0)"})
        if 300 <= resp.status_code < 400 and "Location" in resp.headers:
            location = resp.headers["Location"]
            final_url = urllib.parse.urljoin(current, location)
    except Exception:
        pass
    return final_url, subids

# Shopee short-link
_SUBID_MAXLEN = int(os.getenv("SHOPEE_SUBID_MAXLEN", "50"))
_SUBID_REGEX  = re.compile(r"[^A-Za-z0-9]")
def sanitize_subid_for_shopee(value: str) -> str:
    if not value:
        return "na"
    cleaned = _SUBID_REGEX.sub("", value)
    return cleaned[:_SUBID_MAXLEN] if cleaned else "na"

def generate_short_link(origin_url: str, utm_content_for_api: str) -> str:
    if not SHOPEE_APP_ID or not SHOPEE_APP_SECRET:
        return origin_url  # fallback
    payload_obj = {
        "query": ("mutation{generateShortLink(input:{"
                  f"originUrl:\"{origin_url}\","
                  f"subIds:[\"\",\"\",\"{utm_content_for_api}\",\"\",\"\"]"
                  "}){shortLink}}")
    }
    payload = json.dumps(payload_obj, separators=(',', ':'), ensure_ascii=False)
    ts = str(int(time.time()))
    signature = hashlib.sha256((SHOPEE_APP_ID + ts + payload + SHOPEE_APP_SECRET).encode("utf-8")).hexdigest()
    headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID}, Timestamp={ts}, Signature={signature}",
               "Content-Type": "application/json"}
    try:
        resp = session.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=(2, 10))
        j = resp.json()
        short = (((j or {}).get("data") or {}).get("generateShortLink") or {}).get("shortLink")
        if not short:
            raise ValueError(f"Resposta Shopee sem shortLink: {j}")
        return short
    except Exception as e:
        print(f"[ShopeeShortLink] ERRO: {e}. Fallback para URL numerada.")
        return origin_url

# ===================== Geo por IP =====================
IPAPI_FIELDS = "status,country,region,regionName,city,zip,lat,lon,isp,org,as,query"
def geo_lookup(ip: str) -> Dict[str, Any]:
    if not ip:
        return {"geo_status":"fail"}
    cache_key = f"{GEO_CACHE_PREFIX}{ip}"
    try:
        cached = r.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass
    try:
        url = f"http://ip-api.com/json/{ip}?fields={IPAPI_FIELDS}&lang=pt-BR"
        resp = session.get(url, timeout=5)
        data = resp.json()
        norm = {
            "geo_status": data.get("status","fail"),
            "geo_country": data.get("country"),
            "geo_state": data.get("regionName"),
            "geo_region": data.get("region"),
            "geo_city": data.get("city"),
            "geo_zip": data.get("zip"),
            "geo_lat": data.get("lat"),
            "geo_lon": data.get("lon"),
            "geo_isp": data.get("isp"),
            "geo_org": data.get("org"),
            "geo_asn": data.get("as"),
        }
        r.setex(cache_key, GEO_CACHE_TTL_SECONDS, json.dumps(norm, ensure_ascii=False))
        return norm
    except Exception as e:
        print("[geo_lookup] erro:", e)
        return {"geo_status":"fail"}

# ===================== Predição (CatBoost) =====================
_model = None
_model_classes: List[str] = []
_thresholds: Dict[str, float] = {}
_thr_qvc = None
_thr_atc = None

_FEATURE_NAMES_FROM_META: List[str] = []
_CAT_IDX_FROM_META: List[int] = []

_TE_STATS = None
_TE_PRIORS = None

_POS_LABEL_NAME = "comprou"
_POS_LABEL_ID = 1
_POS_IDX = None

def _load_te_if_available():
    global _TE_STATS, _TE_PRIORS
    if os.path.exists(TE_STATS_PATH):
        try:
            obj = json.load(open(TE_STATS_PATH, "r", encoding="utf-8"))
            _TE_STATS  = obj.get("stats", {})
            _TE_PRIORS = obj.get("priors", {})
            print("[TE] stats carregadas de te_stats.json.")
        except Exception as e:
            print("[TE] erro ao carregar te_stats.json:", e)

def _norm_text(s: str) -> str:
    import unicodedata
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _device_bucket_from_name(name: str) -> str:
    s = _norm_text(name)
    if any(k in s for k in ["iphone 13","iphone 14","iphone 15","iphone 16","pro max","s24","s23","ultra","pixel 7","pixel 8","pixel 9"]): return "high"
    if any(k in s for k in ["iphone 11","iphone 12","s22","s21","pixel 6","a54","a53","m54","m53","redmi note 12"]): return "mid"
    if any(k in s for k in ["j2","j5","j7","moto e","moto g5","galaxy a10","a20","redmi 9","a01","k10","k11"]): return "low"
    if any(k in s for k in ["iphone","galaxy","moto","redmi","pixel"]): return "mid"
    return "unknown"

def _br_region_macro(uf_or_region: str) -> str:
    s = _norm_text(uf_or_region)
    macro = {
        "sp":"sudeste","rj":"sudeste","mg":"sudeste","es":"sudeste",
        "pr":"sul","sc":"sul","rs":"sul",
        "df":"centro-oeste","go":"centro-oeste","mt":"centro-oeste","ms":"centro-oeste",
        "ba":"nordeste","pe":"nordeste","ce":"nordeste","ma":"nordeste","pb":"nordeste","rn":"nordeste","al":"nordeste","se":"nordeste","pi":"nordeste",
        "am":"norte","pa":"norte","ro":"norte","rr":"norte","ap":"norte","ac":"norte","to":"norte"
    }
    for m in ["sudeste","sul","centro-oeste","nordeste","norte"]:
        if m in s:
            return m
    uf = s[-2:]
    return macro.get(uf, "unknown")

def _is_weekend_from_dow(dow: int) -> int:
    return 1 if int(dow) in (5,6) else 0

def _part_of_day_from_hour(h: int) -> str:
    try:
        h = int(h)
    except:
        return "unknown"
    if 0<=h<6: return "dawn"
    if 6<=h<12: return "morning"
    if 12<=h<18: return "afternoon"
    if 18<=h<=23: return "evening"
    return "unknown"

def _version_num_for_log(s):
    m = re.search(r"(\d+(\.\d+)*)", str(s or ""))
    if not m:
        return 0.0
    try:
        return float(m.group(1).split(".")[0])
    except:
        return 0.0

def _get_first_domain_for_log(url):
    try:
        from urllib.parse import urlparse
        netloc = urlparse(str(url)).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except:
        return ""

def _extract_hour_from_iso_for_log(t):
    try:
        return int(str(t).split("T")[1][:2])
    except:
        return 0

def _extract_dow_from_iso_for_log(t):
    try:
        dt = datetime.strptime(str(t)[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.weekday()
    except:
        return time.localtime().tm_wday

def _apply_te(colname: str, value: str) -> Tuple[float,float]:
    if not _TE_STATS or colname not in _TE_STATS:
        return 0.0, 0.0
    stats_map = _TE_STATS[colname]
    prior = float((_TE_PRIORS or {}).get(colname, 0.0))
    rec = stats_map.get(value)
    if rec:
        return float(rec.get("count", 0.0)), float(rec.get("buy_rate", prior))
    return 0.0, prior

def _features_full_dict(os_family, device_name, os_version, referrer, utm_original,
                        iso_time=None, geo: Optional[Dict[str,Any]]=None) -> Dict[str, Any]:
    hour = _extract_hour_from_iso_for_log(iso_time)
    dow  = _extract_dow_from_iso_for_log(iso_time)
    month = int(str(iso_time)[5:7]) if iso_time else time.localtime().tm_mon

    is_android = 1 if "android" in (os_family or "").lower() else 0
    is_ios     = 1 if re.search(r"ios|iphone|ipad", (os_family or ""), re.I) else 0
    os_ver_num = _version_num_for_log(os_version)
    ref_domain = _get_first_domain_for_log(referrer or "")
    device_bucket = _device_bucket_from_name(device_name)
    part_of_day = _part_of_day_from_hour(hour)
    is_weekend = _is_weekend_from_dow(dow)

    geo = geo or {}
    geo_region = (geo.get("geo_region") or "")
    geo_city   = (geo.get("geo_city") or "")
    geo_zip    = (geo.get("geo_zip") or "")
    geo_isp    = (geo.get("geo_isp") or "")
    geo_org    = (geo.get("geo_org") or "")
    geo_macro  = _br_region_macro(geo_region or geo.get("geo_state") or "")
    geo_fail_flag = 0 if (geo.get("geo_status") == "success") else 1

    device_city_combo   = (f"{device_bucket}__{geo_city}").lower()
    utm_partofday_combo = (f"{utm_original}__{part_of_day}").lower()

    utm_n,  utm_br  = _apply_te("utm_original", str(utm_original or ""))
    ref_n,  ref_br  = _apply_te("ref_domain",   str(ref_domain))
    devb_n, devb_br = _apply_te("device_bucket",str(device_bucket))
    city_n, city_br = _apply_te("geo_city",     str(geo_city))

    return {
        "device_bucket": device_bucket, "os_family": os_family or "", "part_of_day": part_of_day,
        "geo_macro": geo_macro, "geo_region": geo_region, "geo_city": geo_city, "geo_zip": geo_zip,
        "geo_isp": geo_isp, "geo_org": geo_org, "ref_domain": ref_domain, "utm_original": utm_original or "",
        "device_city_combo": device_city_combo, "utm_partofday_combo": utm_partofday_combo,
        "hour": float(hour), "dow": float(dow), "month": float(month),
        "is_weekend": float(is_weekend),
        "os_version_num": float(os_ver_num), "is_android": float(is_android), "is_ios": float(is_ios),
        "is_private_ip_flag": 0.0, "is_bot_flag": 0.0, "geo_fail_flag": float(geo_fail_flag), "has_utm_flag": float(1 if utm_original else 0),
        "utm_n": float(utm_n), "utm_br": float(utm_br),
        "ref_n": float(ref_n), "ref_br": float(ref_br),
        "devb_n": float(devb_n), "devb_br": float(devb_br),
        "city_n": float(city_n), "city_br": float(city_br),
    }

def _row_in_meta_order(feats_dict: Dict[str, Any]) -> Tuple[List[Any], List[int]]:
    numeric_defaults = {
        "hour","dow","month","is_weekend","os_version_num","is_android","is_ios",
        "utm_n","utm_br","ref_n","ref_br","devb_n","devb_br","city_n","city_br",
        "is_private_ip_flag","is_bot_flag","geo_fail_flag","has_utm_flag"
    }
    ordered = []
    for col in _FEATURE_NAMES_FROM_META:
        v = feats_dict.get(col)
        if v is None:
            v = 0.0 if col in numeric_defaults else ""
        ordered.append(v)
    return ordered, _CAT_IDX_FROM_META

def _load_model_if_available():
    global _model, _model_classes, _thresholds, _thr_qvc, _thr_atc
    global _FEATURE_NAMES_FROM_META, _CAT_IDX_FROM_META
    global _POS_LABEL_NAME, _POS_LABEL_ID, _POS_IDX
    global _TE_STATS, _TE_PRIORS

    try:
        from catboost import CatBoostClassifier
        if os.path.exists(MODEL_PATH):
            m = CatBoostClassifier()
            m.load_model(MODEL_PATH)
            _model = m

            _model_classes = [str(x) for x in getattr(m, "classes_", [])] or ["0", "1"]

            _POS_LABEL_NAME = "comprou"
            _POS_LABEL_ID = 1
            _POS_IDX = None

            meta = None
            if os.path.exists(MODEL_META_PATH):
                meta = json.load(open(MODEL_META_PATH, "r", encoding="utf-8"))

                _FEATURE_NAMES_FROM_META = list(meta.get("feature_names", []))
                _CAT_IDX_FROM_META = list(meta.get("cat_idx", []))

                thr = meta.get("thresholds_cb") or meta.get("thresholds") or {}
                _thresholds = {"qvc_mid": thr.get("qvc_mid"), "atc_high": thr.get("atc_high")}
                _thr_qvc = float(_thresholds.get("qvc_mid") or 0.5)
                _thr_atc = float(_thresholds.get("atc_high") or _thr_qvc or 0.5)

                _POS_LABEL_NAME = meta.get("positive_label") or "comprou"
                mapping = meta.get("mapping") or {}
                try:
                    _POS_LABEL_ID = int(mapping.get(_POS_LABEL_NAME, 1))
                except Exception:
                    _POS_LABEL_ID = 1

                te = meta.get("te_stats")
                if isinstance(te, dict):
                    _TE_STATS  = te.get("stats", {})
                    _TE_PRIORS = te.get("priors", {})
                    print("[TE] stats carregadas do model_meta_comprou.json.")
                else:
                    _load_te_if_available()
            else:
                _FEATURE_NAMES_FROM_META = []
                _CAT_IDX_FROM_META = []
                _thr_qvc = 0.5
                _thr_atc = 0.5
                _load_te_if_available()

            try:
                classes_raw = list(getattr(_model, "classes_", [0, 1]))
                _POS_IDX = classes_raw.index(_POS_LABEL_ID) if _POS_LABEL_ID in classes_raw else (1 if len(classes_raw) == 2 else None)
            except Exception:
                _POS_IDX = 1

            print(f"[model] carregado. thr_qvc={_thr_qvc} thr_atc={_thr_atc} n_feats={len(_FEATURE_NAMES_FROM_META)} pos_id={_POS_LABEL_ID} pos_idx={_POS_IDX} TE_loaded={'yes' if _TE_STATS else 'no'}")
        else:
            print("[model] arquivo não encontrado:", MODEL_PATH)
    except Exception as e:
        print("[model] erro ao carregar:", e)

def predict_proba_single(row_values: List[Any], cat_idx: List[int]) -> Dict[str, float]:
    try:
        from catboost import Pool
        import pandas as pd
        if not _model:
            return {}
        X_df  = pd.DataFrame([row_values], columns=_FEATURE_NAMES_FROM_META)
        pool  = Pool(X_df, cat_features=cat_idx)
        probs = _model.predict_proba(pool)[0]

        if len(probs) == 2:
            pos_idx = _POS_IDX if _POS_IDX is not None else 1
            p_pos = float(probs[pos_idx])
            p_neg = float(probs[1 - pos_idx])
            return {"comprou": p_pos, "quase": 0.0, "desinteressado": p_neg}

        out = {"comprou": 0.0, "quase": 0.0, "desinteressado": 0.0}
        for i, cls in enumerate([str(c).lower() for c in _model_classes]):
            if i >= len(probs):
                break
            if "comprou" in cls or cls in ("1",):
                out["comprou"] = float(probs[i])
            elif "quase" in cls:
                out["quase"] = float(probs[i])
            elif "desinteress" in cls or cls in ("0",):
                out["desinteressado"] = float(probs[i])
        return out
    except Exception as e:
        print("[predict] erro:", e)
        return {}

_load_model_if_available()

# ===================== UTM Pré-filtro =====================
_UTM_A_SUFFIX_PATTERN = re.compile(r"^(?P<base>.*?)(?P<suf>A\d+)$")

def split_utm_A_suffix(utm: str) -> Tuple[str, str]:
    """
    Se a UTM terminar com 'A' maiúsculo seguido de dígitos, retorna (base, 'Axx').
    Caso contrário, retorna (utm, '').
    """
    s = (utm or "").strip()
    m = _UTM_A_SUFFIX_PATTERN.match(s)
    if m:
        return m.group("base"), m.group("suf")
    return s, ""

# ===================== GCS flush =====================
def _day_key(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts))

def _gcs_object_name(ts: int, part: int) -> str:
    d = _day_key(ts)
    return f"{GCS_PREFIX}date={d}/clicks_{d}_part-{part:04d}.csv"

def _flush_buffer_to_gcs() -> int:
    global _buffer_rows, _last_flush_ts
    if not _bucket:
        return 0
    with _buffer_lock:
        rows = list(_buffer_rows)
        if len(rows) == 0:
            return 0
        _buffer_rows = []
        _last_flush_ts = time.time()
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(CSV_HEADERS)
    w.writerows(rows)
    data = output.getvalue().encode("utf-8")
    part = int(time.time() * 1000) % 10_000_000
    blob_name = _gcs_object_name(int(time.time()), part)
    _bucket.blob(blob_name).upload_from_string(data, content_type="text/csv")
    print(f"[FLUSH] {len(rows)} linha(s) → gs://{GCS_BUCKET}/{blob_name}")
    return len(rows)

def _background_flusher():
    while True:
        try:
            _flush_buffer_to_gcs()
        except Exception as e:
            print("[FLUSH-ERR]", e)
        time.sleep(FLUSH_MAX_SECONDS)

threading.Thread(target=_background_flusher, daemon=True).start()

# ===================== Rotas =====================
@app.get("/health")
def health():
    ttl = None
    try:
        ttl = r.ttl(COUNTER_KEY)
    except Exception:
        ttl = None
    return {
        "ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET, "prefix": GCS_PREFIX,
        "video_id": VIDEO_ID, "model_loaded": bool(_model), "classes": _model_classes,
        "thr_qvc": _thr_qvc, "thr_atc": _thr_atc, "n_features_meta": len(_FEATURE_NAMES_FROM_META),
        "cat_idx_meta": _CAT_IDX_FROM_META, "counter_ttl": ttl,
        "te_loaded": bool(_TE_STATS)
    }

@app.get("/robots.txt")
def robots():
    return PlainTextResponse("User-agent: *\nDisallow:\n", status_code=200)

@app.get("/")
def root():
    return PlainTextResponse("OK", status_code=200)

def _build_incoming_from_request(request: Request, full_path: str) -> Tuple[str, str]:
    link_q = request.query_params.get("link")
    if link_q:
        raw = unquote(link_q).strip()
        return _fix_scheme(raw), "query_link"
    raw_path = urllib.parse.unquote(full_path or "").strip()
    if not raw_path or raw_path == "favicon.ico":
        raise HTTPException(status_code=400, detail="missing_url")
    if re.fullmatch(r"[A-Za-z0-9]{5,20}", raw_path):
        qs = request.url.query
        url = f"https://s.shopee.com.br/{raw_path}"
        if qs:
            url += "?" + qs
        return url, "code_path"
    if raw_path.startswith("//"):
        raw_path = "https:" + raw_path
    return _fix_scheme(raw_path), "raw_path"

# ===================== Handler principal =====================
@app.get("/{full_path:path}")
def track_number_and_redirect(request: Request, full_path: str = Path(...), cat: Optional[str] = Query(None)):
    # ===== Entrada =====
    try:
        incoming_url, mode = _build_incoming_from_request(request, full_path)
    except HTTPException as he:
        return JSONResponse({"ok": False, "error": he.detail}, status_code=he.status_code)

    ts = int(time.time())
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts))
    headers = request.headers
    cookie_header = headers.get("cookie") or headers.get("Cookie")
    referrer = headers.get("referer") or "-"
    user_agent = headers.get("user-agent", "-")
    fbclid = request.query_params.get("fbclid")
    ip_addr = (request.client.host if request.client else "0.0.0.0")
    device_name, os_family, os_version = parse_device_info(user_agent)

    # ===== Resolve url =====
    parts_in = urlsplit(incoming_url)
    resolved_url = _resolve_short_follow(incoming_url) if _is_short_domain(parts_in.netloc) else _fix_scheme(incoming_url)

    parts_res = urlsplit(resolved_url)
    if not parts_res.scheme or not parts_res.netloc:
        return JSONResponse({"ok": False, "error": f"URL inválida: {resolved_url}"}, status_code=400)
    if not _is_allowed_long(parts_res.netloc) and not _is_short_domain(parts_res.netloc):
        return JSONResponse({"ok": False, "error": f"Domínio não permitido: {parts_res.netloc}"}, status_code=400)

    # ===== UTM / pré-filtro =====
    outer_uc = request.query_params.get("uc")
    utm_original_in = outer_uc or extract_utm_content_from_url(incoming_url) or extract_utm_content_from_url(resolved_url) or ""

    base, suf = split_utm_A_suffix(utm_original_in)
    utm_original = base
    utm_original_01 = (base + suf) if suf else ""   # COMPLETA quando existir

    # base para numbered e subId3
    primary_for_ops = utm_original_01 if utm_original_01 else utm_original

    # ===== utm_numbered =====
    clean_base = re.sub(r'[^A-Za-z0-9]', '', primary_for_ops or "") or "n"
    utm_numbered = f"{clean_base}N{incr_counter()}"

    # ===== origin_url com numbered =====
    url_with_utm = _set_utm_content_preserving_order(resolved_url, utm_numbered)
    origin_url = add_or_update_query_param(url_with_utm, "utm_numbered", utm_numbered)
    if _is_short_domain(urlsplit(origin_url).netloc):
        origin_url = _resolve_short_follow(origin_url)

    # ===== content_ids =====
    content_ids, contents_payload = _build_content_identifiers(origin_url)

    # ===== short oficial (subId3) =====
    sub_id_api = sanitize_subid_for_shopee(primary_for_ops) or "na"
    dest = generate_short_link(origin_url, sub_id_api)

    # ===== Cookies / IDs persistentes =====
    fbp_cookie = get_cookie_value(cookie_header, "_fbp") or _gen_fbp(ts)

    fbc_cookie = get_cookie_value(cookie_header, "_fbc")
    fbc_from_fbclid = build_fbc_from_fbclid(fbclid, creation_ts=ts) if fbclid else None
    if fbc_from_fbclid:
        fbc_cookie = fbc_from_fbclid
    if not fbc_cookie:
        fbc_cookie = _gen_fbc_from_fbp(fbp_cookie, ts)

    eid_cookie = get_cookie_value(cookie_header, "_eid") or _sha256_lower(fbp_cookie)

    # ===== snapshot no Redis =====
    r.setex(f"{USERDATA_KEY_PREFIX}{utm_numbered}", USERDATA_TTL_SECONDS, json.dumps({
        "ip": ip_addr, "ua": user_agent, "referrer": referrer,
        "event_source_url": origin_url, "short_link": dest,
        "vc_time": ts, "utm_original": utm_original, "utm_original_01": utm_original_01, "utm_numbered": utm_numbered,
        "fbclid": fbclid, "fbp": fbp_cookie, "fbc": fbc_cookie, "eid": eid_cookie, "mode": mode
    }))

    # ===== external_id para CAPI =====
    external_id = eid_cookie or _sha256_lower(f"{user_agent}|{ip_addr}")

    user_data_meta = {
        "client_ip_address": ip_addr,
        "client_user_agent": user_agent,
        "external_id": external_id,
        **({"fbp": fbp_cookie} if fbp_cookie else {}),
        **({"fbc": fbc_cookie} if fbc_cookie else {})
    }

    # ===== Geo + Features + Pred =====
    geo = geo_lookup(ip_addr)
    p_map = {"comprou": 0.0, "quase": 0.0, "desinteressado": 0.0}
    pred_label = "desinteressado"
    is_qvc_flag = 0
    is_atc_flag = 0

    try:
        feats_dict = _features_full_dict(os_family, device_name, os_version, referrer, utm_original, iso_time, geo)
        if _model and _FEATURE_NAMES_FROM_META:
            row_values, cat_idx = _row_in_meta_order(feats_dict)
            probs = predict_proba_single(row_values, cat_idx)
            for k in list(probs.keys()):
                p_map[k] = float(probs[k])

            p_c = float(p_map.get("comprou", 0.0))
            is_qvc_flag = 1 if p_c >= (_thr_qvc or 0.5) else 0
            is_atc_flag = 1 if p_c >= (_thr_atc or _thr_qvc or 0.5) else 0
            pred_label = "comprou" if is_atc_flag else ("comprou_qvc" if is_qvc_flag else "desinteressado")
    except Exception as e:
        print("[predict] erro:", e)

    # ===== Meta: ViewContent sempre =====
    meta_sent = ""
    meta_view = ""
    if META_PIXEL_ID and META_ACCESS_TOKEN:
        vc_payload = {
            "data": [{
                "event_name": "ViewContent",
                "event_time": ts,
                "event_id": utm_numbered,
                "action_source": "website",
                "event_source_url": origin_url,
                "user_data": user_data_meta,
                "custom_data": {
                    "currency": "BRL",
                    "value": 0,
                    "content_type": "product",
                    "content_ids": content_ids,
                    "contents": contents_payload
                }
            }]
        }
        try:
            url = f"https://graph.facebook.com/{META_GRAPH_VERSION}/{META_PIXEL_ID}/events"
            params = {"access_token": META_ACCESS_TOKEN}
            if META_TEST_EVENT_CODE:
                vc_payload["test_event_code"] = META_TEST_EVENT_CODE
            resp_vc = session.post(url, params=params, json=vc_payload, timeout=8)
            meta_view = "ViewContent" if resp_vc.status_code < 400 else f"error:{resp_vc.status_code}"
        except Exception as e:
            print("[meta] VC exceção:", e)
            meta_view = "error"

    # ===== Meta: AddToCart se p>=thr_atc =====
    if META_PIXEL_ID and META_ACCESS_TOKEN and is_atc_flag == 1:
        atc_payload = {
            "data": [{
                "event_name": "AddToCart",
                "event_time": ts,
                "event_id": utm_numbered,
                "action_source": "website",
                "event_source_url": origin_url,
                "user_data": user_data_meta,
                "custom_data": {
                    "currency": "BRL",
                    "value": 0,
                    "content_type": "product",
                    "content_ids": content_ids,
                    "contents": contents_payload
                }
            }]
        }
        try:
            url = f"https://graph.facebook.com/{META_GRAPH_VERSION}/{META_PIXEL_ID}/events"
            params = {"access_token": META_ACCESS_TOKEN}
            if META_TEST_EVENT_CODE:
                atc_payload["test_event_code"] = META_TEST_EVENT_CODE
            resp_atc = session.post(url, params=params, json=atc_payload, timeout=8)
            meta_sent = "AddToCart" if resp_atc.status_code < 400 else f"error:{resp_atc.status_code}"
        except Exception as e:
            print("[meta] ATC exceção:", e)
            meta_sent = "error"

    # ===== Log CSV =====
    hour_log = _extract_hour_from_iso_for_log(iso_time)
    dow_log = _extract_dow_from_iso_for_log(iso_time)
    ref_domain_log = _get_first_domain_for_log(referrer)
    os_version_num_log = _version_num_for_log(os_version)
    is_android_log = 1 if "android" in (os_family or "").lower() else 0
    is_ios_log     = 1 if re.search(r"ios|iphone|ipad", (os_family or ""), re.I) else 0
    part_of_day_log = _part_of_day_from_hour(hour_log)

    p_c = float(p_map.get("comprou", 0.0))
    thr_q = float(_thr_qvc or 0.5)
    thr_a = float(_thr_atc or thr_q or 0.5)

    pcomprou_pct = p_c * 100.0
    thr_qvc_pct  = thr_q * 100.0
    thr_atc_pct  = thr_a * 100.0
    gap_qvc      = p_c - thr_q
    gap_atc      = p_c - thr_a
    pct_of_atc   = (p_c / thr_a * 100.0) if thr_a > 0 else 0.0

    csv_row = [
        ts, iso_time, ip_addr, user_agent, referrer,
        incoming_url, resolved_url, origin_url, (cat or ""),
        utm_original, utm_original_01, utm_numbered, fbclid or "", fbp_cookie or "", fbc_cookie or "",
        device_name, os_family, os_version,
        geo.get("geo_status",""), geo.get("geo_country",""), geo.get("geo_region",""), geo.get("geo_state",""),
        geo.get("geo_city",""), geo.get("geo_zip",""), geo.get("geo_lat",""), geo.get("geo_lon",""),
        geo.get("geo_isp",""), geo.get("geo_org",""), geo.get("geo_asn",""),
        part_of_day_log, hour_log, dow_log, ref_domain_log, os_version_num_log, is_android_log, is_ios_log,
        pred_label, p_c, float(p_map.get("quase",0.0)), float(p_map.get("desinteressado",0.0)),
        thr_q, thr_a, int(is_qvc_flag), int(is_atc_flag),
        pcomprou_pct, thr_qvc_pct, thr_atc_pct, gap_qvc, gap_atc, pct_of_atc,
        meta_sent, meta_view
    ]
    with _buffer_lock:
        _buffer_rows.append(csv_row)

    # ===== Intersticial Pinterest =====
    def _is_pinterest(ua: str) -> bool:
        return "pinterest" in (ua or "").lower()

    set_cookie_headers = [
        f"_fbp={fbp_cookie}; Path=/; Max-Age=63072000; SameSite=Lax",
        f"_fbc={fbc_cookie}; Path=/; Max-Age=63072000; SameSite=Lax",
        f"_eid={eid_cookie}; Path=/; Max-Age=63072000; SameSite=Lax",
    ]

    if _is_pinterest(user_agent):
        html = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="0.2;url={dest}">
<title>Redirecionando…</title>
<script>setTimeout(function(){{ window.location.replace("{dest}"); }}, 200);</script>
</head><body>Redirecionando…</body></html>"""
        return HTMLResponse(content=html, status_code=200, headers={
            "Cache-Control": "no-store, max-age=0", "X-Content-Type-Options": "nosniff",
            "Set-Cookie": ", ".join(set_cookie_headers)
        })

    return RedirectResponse(dest, status_code=302, headers={
        "Cache-Control": "no-store, max-age=0",
        "Set-Cookie": ", ".join(set_cookie_headers)
    })

# ===================== Admin =====================
@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    return """
    <html>
      <head><meta charset="utf-8"><title>Upload do Modelo</title></head>
      <body style="font-family: sans-serif; max-width: 640px; margin: 40px auto;">
        <h2>Enviar modelo CatBoost (alinhado ao treino)</h2>
        <p><b>Arquivos esperados:</b> model_comprou.cbm e model_meta_comprou.json (opcional: te_stats.json)</p>
        <form method="post" action="/admin/upload_model_simple" enctype="multipart/form-data">
          <div>Token (X-Admin-Token): <input name="token" type="password" required /></div><br/>
          <div>model_comprou.cbm: <input name="model" type="file" required /></div><br/>
          <div>model_meta_comprou.json (opcional): <input name="meta" type="file" /></div><br/>
          <button type="submit">Enviar</button>
        </form>
        <p>Depois de enviar, abra <code>/health</code> para conferir <b>model_loaded: true</b>, thresholds e TE loaded.</p>
      </body>
    </html>
    """

@app.post("/admin/upload_model_simple")
async def admin_upload_model_simple(token: str = Form(...), model: UploadFile = File(...), meta: UploadFile = File(None)):
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    try:
        with open(MODEL_PATH, "wb") as out:
            out.write(await model.read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"falha ao salvar model: {e}")
    if meta:
        try:
            data = await meta.read()
            _ = json.loads(data.decode("utf-8"))
            with open(MODEL_META_PATH, "wb") as out:
                out.write(data)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"meta_file inválido: {e}")
    _load_model_if_available()
    return {"ok": True, "model_loaded": bool(_model), "thr_qvc": _thr_qvc, "thr_atc": _thr_atc, "te_loaded": bool(_TE_STATS)}

@app.post("/admin/upload_model")
async def admin_upload_model(
    model_file: UploadFile = File(None), meta_file: UploadFile = File(None),
    model: UploadFile = File(None), meta: UploadFile = File(None),
    x_admin_token: str = Header(None)
):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    up_model = model_file or model
    if not up_model:
        raise HTTPException(status_code=400, detail="model file ausente (campo model_file ou model)")
    try:
        with open(MODEL_PATH, "wb") as out:
            out.write(await up_model.read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"falha ao salvar model: {e}")
    up_meta = meta_file or meta
    if up_meta:
        try:
            data = await up_meta.read()
            _ = json.loads(data.decode("utf-8"))
            with open(MODEL_META_PATH, "wb") as out:
                out.write(data)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"meta_file inválido: {e}")
    _load_model_if_available()
    return {
        "ok": True,
        "saved": {"model": MODEL_PATH, "meta": (MODEL_META_PATH if up_meta else None)},
        "model_loaded": bool(_model),
        "thr_qvc": _thr_qvc,
        "thr_atc": _thr_atc,
        "te_loaded": bool(_TE_STATS)
    }

@app.post("/admin/reload_model")
def admin_reload_model(x_admin_token: str = Header(None)):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    _load_model_if_available()
    return {"ok": True, "model_loaded": bool(_model), "thr_qvc": _thr_qvc, "thr_atc": _thr_atc, "te_loaded": bool(_TE_STATS)}

@app.get("/admin/flush")
def admin_flush(token: str):
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    sent = _flush_buffer_to_gcs()
    return {"ok": True, "sent_rows": sent}

@app.get("/admin/counter")
def admin_counter(x_admin_token: str = Header(None)):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    try:
        v = r.get(COUNTER_KEY)
        ttl = r.ttl(COUNTER_KEY)
        return {"counter": int(v or 0), "ttl": ttl}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ===================== Flush no exit =====================
@atexit.register
def _flush_on_exit():
    try:
        _flush_buffer_to_gcs()
    except Exception:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
