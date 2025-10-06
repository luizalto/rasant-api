
# -*- coding: utf-8 -*-
"""
FastAPI – Shopee ShortLink + Redis + GCS + Geo (IP) + Predição (CatBoost/RF/XGB/STACK) + Meta Ads (CAPI) + TikTok Events API + Pinterest Conversions API
(versão consolidada) — UTMContent / UTMContent01 / UTMNumered com Janela Deslizante (1000) e Monotonicidade
"""
import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json, random, ipaddress, traceback
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
GCS_BUCKET            = os.getenv("GCS_BUCKET")
GCS_PREFIX            = os.getenv("GCS_PREFIX", "logs/")
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

# >>> Janela deslizante de UTM numerado (cap = 1000 por padrão)
UTM_RECENT_LIST_KEY   = os.getenv("UTM_RECENT_LIST_KEY", "utm_recent_list")
UTM_RECENT_SET_KEY    = os.getenv("UTM_RECENT_SET_KEY", "utm_recent_set")
UTM_RECENT_MAX        = int(os.getenv("UTM_RECENT_MAX", "1000"))

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

# TikTok Events API
TIKTOK_PIXEL_ID       = os.getenv("TIKTOK_PIXEL_ID", "")
TIKTOK_ACCESS_TOKEN   = os.getenv("TIKTOK_ACCESS_TOKEN", "")
TIKTOK_API_BASE       = os.getenv("TIKTOK_API_BASE", "https://business-api.tiktok.com/open_api")
TIKTOK_API_TRACK_URL  = f"{TIKTOK_API_BASE}/v1.3/pixel/track/"

# Pinterest Conversions API
PIN_AD_ACCOUNT_ID     = os.getenv("PIN_AD_ACCOUNT_ID", "")
PIN_ACCESS_TOKEN      = os.getenv("PIN_ACCESS_TOKEN", "")
PIN_TEST_MODE         = os.getenv("PIN_TEST_MODE", "0").lower() in ("1", "true", "yes", "on")
PIN_API_BASE          = "https://api.pinterest.com/v5"

# Seleção de modelo (server)
SERVING_MODE_ENV      = (os.getenv("SERVING_MODE", "auto") or "auto").strip().lower()  # auto|cb|rf|xgb|stack

# Comportamento de resolução de URL (1 = NÃO resolver short, default; 0 = resolver)
SKIP_RESOLVE          = os.getenv("SKIP_RESOLVE", "1").lower() in ("1","true","yes","on")

# Modelos
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR      = os.path.join(BASE_DIR, "models")
os.makedirs(MODELS_DIR, exist_ok=True)

# Caminhos principais (modelos – opcionais)
MODEL_CB_PATH        = os.path.join(MODELS_DIR, "model_comprou.cbm")
MODEL_META_PATH      = os.path.join(MODELS_DIR, "model_meta_comprou.json")
TE_STATS_PATH        = os.path.join(MODELS_DIR, "te_stats.json")
MODEL_RF_PATH        = os.path.join(MODELS_DIR, "rf.joblib")
MODEL_XGB_PATH       = os.path.join(MODELS_DIR, "xgb.json")
MODEL_STACK_PATH     = os.path.join(MODELS_DIR, "stack_model.joblib")
STACK_META_PATH      = os.path.join(MODELS_DIR, "stack_meta.json")

# ===================== App / Clients =====================
app = FastAPI(title="Shopee UTM → ShortLink + Geo + Predict (Multi-Model) + CAPI + TikTok + Pinterest")

# Redis
r = redis.from_url(REDIS_URL)
try:
    r.persist(COUNTER_KEY)
except Exception:
    pass

# GCS (lazy)
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
    # Pinterest: contents[].item_price deve ser string pelo schema atual
    return [str(cid)], [{"id": str(cid), "quantity": 1, "item_price": "0"}]

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
    "timestamp","iso_time","ip","user_agent","referrer",
    "short_link_in","resolved_url","final_url","category",
    "utm_original","utm_original_01","subid_base","subid_full","utm_numbered","fbclid","fbp","fbc",
    "device_name","os_family","os_version",
    "geo_status","geo_country","geo_region","geo_state","geo_city","geo_zip","geo_lat","geo_lon","geo_isp","geo_org","geo_asn",
    "part_of_day","hour","dow","ref_domain","os_version_num","is_android","is_ios",
    "pred_label","p_comprou","p_quase","p_desinteressado",
    "thr_qvc","thr_atc","is_qvc","is_atc",
    "pcomprou_pct","thr_qvc_pct","thr_atc_pct","gap_qvc","gap_atc","pct_of_atc",
    "meta_event_sent","meta_view_sent",
    "tiktok_event_sent","tiktok_view_sent",
    "pinterest_event_sent","pinterest_view_sent"
]

# ===================== Utils =====================
def incr_counter_raw() -> int:
    pipe = r.pipeline()
    pipe.incr(COUNTER_KEY)
    pipe.persist(COUNTER_KEY)
    val, _ = pipe.execute()
    return int(val)

_last_seen = {"v": 0}
_last_lock = threading.Lock()

def incr_counter_monotonic() -> int:
    n = incr_counter_raw()
    with _last_lock:
        if n <= _last_seen["v"]:
            delta = (_last_seen["v"] + 1) - n
            n = int(r.incrby(COUNTER_KEY, delta))
        _last_seen["v"] = n
    return n

def _parse_tail_number(utm: str) -> Optional[int]:
    if not utm:
        return None
    i = utm.rfind('N')
    if i == -1 or i == len(utm)-1:
        return None
    tail = utm[i+1:]
    return int(tail) if tail.isdigit() else None

def _realign_counter_from_recent_window():
    try:
        recent = r.lrange(UTM_RECENT_LIST_KEY, 0, min(UTM_RECENT_MAX-1, 200))
        max_n = 0
        for utm in (recent or []):
            try:
                n = _parse_tail_number(utm.decode() if isinstance(utm, bytes) else utm)
                if n and n > max_n:
                    max_n = n
            except Exception:
                pass
        current = r.get(COUNTER_KEY)
        cur_n = int(current or 0)
        target = max(cur_n, max_n)
        if target > cur_n:
            r.set(COUNTER_KEY, target)
        with _last_lock:
            _last_seen["v"] = target
        print(f"[counter] realigned to {target} (cur={cur_n}, max_recent={max_n})")
    except Exception as e:
        print("[counter] realign skipped:", e)

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

def extract_utm_content_from_url(u: str) -> str:
    try:
        qs = urlsplit(u).query
        if not qs:
            return ""
        q = parse_qs(qs, keep_blank_values=True)
        return (q.get("utm_content") or [""])[0]
    except:
        return ""

_URL_CUT_RE = re.compile(r'(https?:\/\/|www\.|[a-z0-9-]+\.(?:com|br|net|org)(?:\/|$))', re.I)

def _normalize_utm_content(raw: str) -> Tuple[str, str]:
    s = (raw or "").strip()
    m = _URL_CUT_RE.search(s)
    if m:
        s = s[:m.start()]
    idx_http = s.lower().find("http")
    if idx_http != -1:
        s = s[:idx_http]
    s = re.sub(r'^[\s\-_]+|[\s\-_]+$', '', s)
    s = re.sub(r'[^A-Za-z0-9\-]+', '', s)
    s = re.sub(r'-{2,}', '-', s)
    m2 = re.match(r'^(?P<base>[A-Za-z0-9\-]*?)(?:-?(?P<suf>A\d+))?$', s)
    if not m2:
        return s, ""
    base = (m2.group("base") or "").strip("-")
    suf  = m2.group("suf") or ""
    utm_content    = base
    utm_content_01 = f"{base}-{suf}" if suf else ""
    return utm_content, utm_content_01

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

_SUBID_MAXLEN = int(os.getenv("SHOPEE_SUBID_MAXLEN", "50"))
_SUBID_REGEX  = re.compile(r"[^A-Za-z0-9]")
def sanitize_subid_for_shopee(value: str) -> str:
    if not value:
        return "na"
    cleaned = _SUBID_REGEX.sub("", value)
    return cleaned[:_SUBID_MAXLEN] if cleaned else "na"

def generate_short_link(origin_url: str, utm_content_for_api: str) -> str:
    if not SHOPEE_APP_ID or not SHOPEE_APP_SECRET:
        raise HTTPException(status_code=502, detail="shortlink_unavailable: missing Shopee credentials")
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
    resp = session.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=(3, 12))
    try:
        j = resp.json()
    except Exception:
        raise HTTPException(status_code=502, detail=f"shortlink_error: non-json response status={resp.status_code}")
    short = (((j or {}).get("data") or {}).get("generateShortLink") or {}).get("shortLink")
    if not short:
        raise HTTPException(status_code=502, detail=f"shortlink_error: {j}")
    return short

# ===================== Geo por IP (robusto) =====================
def _is_private_ip(ip: str) -> bool:
    try:
        return ipaddress.ip_address(ip).is_private or ip in ("127.0.0.1", "::1")
    except Exception:
        return True

IPAPI_FIELDS = "status,country,region,regionName,city,zip,lat,lon,isp,org,as,query"
def geo_lookup(ip: str) -> Dict[str, Any]:
    if not ip or _is_private_ip(ip):
        return {
            "geo_status": "skip",
            "geo_country": None, "geo_state": None, "geo_region": None, "geo_city": None,
            "geo_zip": None, "geo_lat": None, "geo_lon": None, "geo_isp": None, "geo_org": None, "geo_asn": None,
        }
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
        if resp.status_code != 200:
            norm = {"geo_status": f"http_{resp.status_code}"}
            try: r.setex(cache_key, 300, json.dumps(norm, ensure_ascii=False))
            except Exception: pass
            print(f"[geo_lookup] http={resp.status_code} ip={ip}")
            return norm
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "json" not in ctype:
            norm = {"geo_status": "non_json"}
            try: r.setex(cache_key, 300, json.dumps(norm, ensure_ascii=False))
            except Exception: pass
            print(f"[geo_lookup] non-json content-type='{ctype}' ip={ip}")
            return norm
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
        ttl = GEO_CACHE_TTL_SECONDS if norm["geo_status"] == "success" else 300
        try: r.setex(cache_key, ttl, json.dumps(norm, ensure_ascii=False))
        except Exception: pass
        return norm
    except Exception as e:
        norm = {"geo_status": "error"}
        try: r.setex(cache_key, 300, json.dumps(norm, ensure_ascii=False))
        except Exception: pass
        print(f"[geo_lookup] exceção: {e.__class__.__name__}")
        return norm

# ===================== (Predição – mantida simplificada) =====================
# Para manter compatibilidade com o seu /health e logs, deixamos stubs seguros
_cb_model = None; _rf_model = None; _xgb_model = None; _stack_model = None
_FEATURE_NAMES_FROM_META: List[str] = []; _CAT_IDX_FROM_META: List[int] = []
_TE_STATS = None; _TE_PRIORS = None
_thresholds_stack = {"qvc_mid": 0.5, "atc_high": 0.5}
_thresholds_cb    = {"qvc_mid": 0.5, "atc_high": 0.5}
_thresholds_rf    = None; _thresholds_xgb = None
_current_mode     = SERVING_MODE_ENV
_POS_IDX = 1

def _load_models_if_available():
    # Mantém logs e leitura opcional; se não existirem, segue em frente.
    global _cb_model, _rf_model, _xgb_model, _stack_model
    global _FEATURE_NAMES_FROM_META, _CAT_IDX_FROM_META, _thresholds_stack, _thresholds_cb
    try:
        if os.path.exists(MODEL_META_PATH):
            meta = json.load(open(MODEL_META_PATH, "r", encoding="utf-8"))
            _FEATURE_NAMES_FROM_META = list(meta.get("feature_names", []))
            _CAT_IDX_FROM_META = list(meta.get("cat_idx", []))
            t_stack = meta.get("thresholds") or {}
            _thresholds_stack = {"qvc_mid": float(t_stack.get("qvc_mid", 0.5)),
                                 "atc_high": float(t_stack.get("atc_high", 0.5))}
            t_cb = meta.get("thresholds_cb") or {}
            _thresholds_cb = {"qvc_mid": float(t_cb.get("qvc_mid", _thresholds_stack["qvc_mid"])),
                              "atc_high": float(t_cb.get("atc_high", _thresholds_stack["atc_high"]))}
    except Exception as e:
        print("[model-meta] erro ao ler:", e)

    try:
        if os.path.exists(MODEL_CB_PATH):
            from catboost import CatBoostClassifier
            m = CatBoostClassifier()
            m.load_model(MODEL_CB_PATH)
            _cb_model = m
    except Exception as e:
        print("[CatBoost] erro:", e)

    try:
        if os.path.exists(MODEL_RF_PATH):
            import joblib
            _rf_model = joblib.load(MODEL_RF_PATH)
    except Exception as e:
        print("[RF] erro:", e)

    try:
        if os.path.exists(MODEL_XGB_PATH):
            import xgboost as xgb
            try:
                clf = xgb.XGBClassifier()
                clf.load_model(MODEL_XGB_PATH)
                _xgb_model = clf
            except Exception:
                bst = xgb.Booster()
                bst.load_model(MODEL_XGB_PATH)
                _xgb_model = bst
    except Exception as e:
        print("[XGB] erro:", e)

    try:
        if os.path.exists(MODEL_STACK_PATH):
            import joblib
            _stack_model = joblib.load(MODEL_STACK_PATH)
    except Exception as e:
        print("[STACK] erro:", e)

    print(f"[models] loaded: cb={bool(_cb_model)} rf={bool(_rf_model)} xgb={bool(_xgb_model)} stack={bool(_stack_model)}")
    print(f"[meta] n_features={len(_FEATURE_NAMES_FROM_META)} cat_idx={_CAT_IDX_FROM_META}")
    print(f"[thr] stack={_thresholds_stack} cb={_thresholds_cb} rf={_thresholds_rf} xgb={_thresholds_xgb}")

_load_models_if_available()

def _features_full_dict(os_family, device_name, os_version, referrer, utm_original,
                        iso_time=None, geo: Optional[Dict[str,Any]]=None) -> Dict[str, Any]:
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
    hour = _extract_hour_from_iso_for_log(iso_time)
    dow  = _extract_dow_from_iso_for_log(iso_time)
    is_android = 1 if "android" in (os_family or "").lower() else 0
    is_ios     = 1 if re.search(r"ios|iphone|ipad", (os_family or ""), re.I) else 0
    os_ver_num = 0.0
    try:
        m = re.search(r"(\d+(\.\d+)*)", str(os_version or ""))
        if m: os_ver_num = float(m.group(1).split(".")[0])
    except: pass
    ref_domain = _get_first_domain_for_log(referrer or "")
    part_of_day = "unknown"
    try:
        part_of_day = ("dawn" if 0<=hour<6 else "morning" if 6<=hour<12 else "afternoon" if 12<=hour<18 else "evening")
    except: pass
    geo = geo or {}
    return {
        "hour": float(hour), "dow": float(dow),
        "is_android": float(is_android), "is_ios": float(is_ios),
        "os_version_num": float(os_ver_num),
        "ref_domain": ref_domain, "utm_original": utm_original or "",
        "part_of_day": part_of_day, "os_family": os_family or "", "device_bucket": device_name or "",
        "geo_city": geo.get("geo_city",""), "geo_region": geo.get("geo_region",""), "geo_state": geo.get("geo_state","")
    }

def _compute_all_probs(feats_dict: Dict[str, Any]) -> Tuple[Dict[str,float], Dict[str, Optional[float]]]:
    # Stubs: devolve 0.0 se modelos não estiverem carregados
    p_cb_map = {"comprou": 0.0, "quase": 0.0, "desinteressado": 1.0}
    p_avail = {"cb": None, "rf": None, "xgb": None, "stack": None}
    return p_cb_map, p_avail

def _thresholds_for_mode(mode: str) -> Tuple[float,float]:
    if mode == "stack": return float(_thresholds_stack["qvc_mid"]), float(_thresholds_stack["atc_high"])
    if mode == "cb":    return float(_thresholds_cb["qvc_mid"]), float(_thresholds_cb["atc_high"])
    if mode == "rf" and _thresholds_rf:  return float(_thresholds_rf["qvc_mid"]), float(_thresholds_rf["atc_high"])
    if mode == "xgb" and _thresholds_xgb: return float(_thresholds_xgb["qvc_mid"]), float(_thresholds_xgb["atc_high"])
    return 0.5, 0.5

def _choose_active_mode(forced_mode: Optional[str], p_avail: Dict[str, Optional[float]]) -> str:
    for mode in (forced_mode or "", _current_mode, "cb", "xgb", "rf", "stack"):
        if mode in ("cb","xgb","rf","stack"):
            if p_avail.get(mode) is not None:
                return mode
    return "none"

# ======== UTM numerado ========
def _window_add_and_trim(utm_value: str):
    pipe = r.pipeline()
    pipe.lpush(UTM_RECENT_LIST_KEY, utm_value)
    pipe.ltrim(UTM_RECENT_LIST_KEY, 0, UTM_RECENT_MAX - 1)
    pipe.execute()
    try:
        llen = r.llen(UTM_RECENT_LIST_KEY)
        if llen and llen > UTM_RECENT_MAX:
            popped = r.rpop(UTM_RECENT_LIST_KEY)
            if popped:
                try:
                    popped_str = popped.decode() if isinstance(popped, bytes) else popped
                    r.srem(UTM_RECENT_SET_KEY, popped_str)
                except Exception:
                    pass
    except Exception:
        pass

def next_unique_numbered(seed_clean_for_numbered: str) -> str:
    attempts = 0
    while attempts < 12:
        attempts += 1
        n = incr_counter_monotonic()
        utm = f"{seed_clean_for_numbered}N{n}"
        try:
            added = r.sadd(UTM_RECENT_SET_KEY, utm)
        except Exception:
            added = 1
        if added == 1:
            _window_add_and_trim(utm)
            return utm
    try:
        jump = random.randint(10, 1000)
        r.incrby(COUNTER_KEY, jump)
    except Exception:
        pass
    n = incr_counter_monotonic()
    utm = f"{seed_clean_for_numbered}N{n}"
    try:
        r.sadd(UTM_RECENT_SET_KEY, utm)
    except Exception:
        pass
    _window_add_and_trim(utm)
    return utm

_realign_counter_from_recent_window()

# ===================== TikTok helper =====================
def send_tiktok_event(
    event_name: str,
    event_id: str,
    ts: int,
    page_url: str,
    ip: str,
    user_agent: str,
    ttclid: Optional[str],
    currency: str,
    value: float,
    content_ids: List[str],
    contents_payload: List[Dict[str, Any]],
) -> str:
    if not (TIKTOK_PIXEL_ID and TIKTOK_ACCESS_TOKEN):
        print(f"[tiktok.{event_name}] SKIP no creds")
        return "skipped:no_tiktok_creds"

    payload = {
        "pixel_code": TIKTOK_PIXEL_ID,
        "event": event_name,
        # TikTok v1.3 aceita string; erro anterior: "not a valid string"
        "event_id": event_id,
        "timestamp": str(ts),
        "context": {
            "ad": {"callback": ttclid} if ttclid else {},
            "page": {"url": page_url},
            "user": {"ip": ip, "user_agent": user_agent}
        },
        "properties": {
            "currency": currency,
            "value": float(value),
            "content_type": "product",
            "content_id": content_ids[0] if content_ids else None,
            "contents": contents_payload,
        }
    }
    headers = {"Content-Type": "application/json", "Access-Token": TIKTOK_ACCESS_TOKEN}

    try:
        resp = session.post(TIKTOK_API_TRACK_URL, headers=headers, json=payload, timeout=8)
        body = safe_body(resp)
        if resp.status_code < 400 and (body.get("code") in (0, "0", None)):
            print(f"[tiktok.{event_name}] OK status={resp.status_code} body={json.dumps(body, ensure_ascii=False)}")
            return "ok"
        print(f"[tiktok.{event_name}] ERRO status={resp.status_code} body={json.dumps(body, ensure_ascii=False)}")
        return f"error:{resp.status_code}"
    except Exception as e:
        print(f"[tiktok.{event_name}] exceção: {e}")
        return "error"

# ===================== Pinterest helper =====================
def send_pinterest_event(
    event_name: str,
    event_id: str,
    ts: int,
    page_url: str,
    ip: str,
    user_agent: str,
    currency: str,
    value: float,
    content_ids: List[str],
    contents_payload: List[Dict[str, Any]],
) -> str:
    """
    Envia evento server-side para a API de Conversões do Pinterest (v5).
    Requer:
      - event_time: inteiro (epoch seconds)
      - currency: string ("BRL")
      - value: número (float/int)
      - content_ids: lista de strings
      - contents: lista de objetos com quantity (int) / item_price (número)
    """
    if not (PIN_AD_ACCOUNT_ID and PIN_ACCESS_TOKEN):
        return "skipped:no_pinterest_creds"

    url = f"{PIN_API_BASE}/ad_accounts/{PIN_AD_ACCOUNT_ID}/events"
    if PIN_TEST_MODE:
        url += "?test=true"

    # Sanitizações mínimas para tipagem correta
    safe_content_ids = [str(cid) for cid in (content_ids or [])]
    safe_contents = []
    for c in (contents_payload or []):
        qty = c.get("quantity", 1)
        try:
            qty = int(qty)
        except Exception:
            qty = 1
        item_price = c.get("item_price", 0)
        try:
            item_price = float(item_price)
        except Exception:
            item_price = 0.0
        safe_contents.append({"quantity": qty, "item_price": item_price})

    payload = {
        "data": [{
            "event_name": event_name,                   # ex: "view_item", "add_to_cart"
            "action_source": "web",
            "event_time": int(ts),                      # <<<<<<<<<<<<<< INTEIRO (CORRIGIDO)
            "event_id": event_id,
            "event_source_url": page_url,
            "user_data": {
                "client_ip_address": ip,
                "client_user_agent": user_agent
            },
            "custom_data": {
                "currency": str(currency or "BRL"),     # string
                "value": float(value or 0.0),           # número
                "content_ids": safe_content_ids,        # lista[str]
                "contents": safe_contents               # lista[{quantity:int, item_price:number}]
            }
        }]}
    headers = {
        "Authorization": f"Bearer {PIN_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        resp = session.post(url, headers=headers, json=payload, timeout=8)
        body = {}
        try:
            body = resp.json()
        except Exception:
            body = {"_raw": resp.text[:500]}
        if resp.status_code < 400:
            print(f"[pinterest.{event_name}] OK status={resp.status_code} body={json.dumps(body, ensure_ascii=False)}")
            return "ok"
        else:
            print(f"[pinterest.{event_name}] ERRO status={resp.status_code} body={json.dumps(body, ensure_ascii=False)}")
            return f"error:{resp.status_code}"
    except Exception as e:
        print(f"[pinterest.{event_name}] EXC {type(e).__name__}: {e}")
        return "error"


# ===================== Meta helper (com logs consistentes) =====================
def send_meta_event(event_name: str, event_id: str, ts: int, origin_url: str, user_data_meta: Dict[str, Any],
                    content_ids: List[str], contents_payload: List[Dict[str, Any]]) -> str:
    if not (META_PIXEL_ID and META_ACCESS_TOKEN):
        print(f"[meta.{event_name}] SKIP no creds")
        return "skipped:no_meta_creds"
    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": ts,
            "event_id": event_id,
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
        }]}
    url = f"https://graph.facebook.com/{META_GRAPH_VERSION}/{META_PIXEL_ID}/events"
    params = {"access_token": META_ACCESS_TOKEN}
    if META_TEST_EVENT_CODE:
        payload["test_event_code"] = META_TEST_EVENT_CODE
    try:
        resp = session.post(url, params=params, json=payload, timeout=8)
        body = safe_body(resp)
        if resp.status_code < 400:
            print(f"[meta.{event_name}] OK status={resp.status_code} body={json.dumps(body, ensure_ascii=False)}")
            return "ok"
        print(f"[meta.{event_name}] ERRO status={resp.status_code} body={json.dumps(body, ensure_ascii=False)}")
        return f"error:{resp.status_code}"
    except Exception as e:
        print(f"[meta.{event_name}] exceção: {e}")
        return "error"

def safe_body(resp):
    try:
        return resp.json()
    except Exception:
        return {"text": (resp.text[:500] if hasattr(resp, "text") else "")}

# ===================== GCS flush =====================
def _day_key(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts))

def _gcs_object_name(ts: int, part: int) -> str:
    d = _day_key(ts)
    return f"{GCS_PREFIX}date={d}/clicks_{d}_part-{part:04d}.csv"

def _flush_buffer_to_gcs() -> int:
    global _buffer_rows
    if not _bucket:
        return 0
    with _buffer_lock:
        rows = list(_buffer_rows)
        if len(rows) == 0:
            return 0
        _buffer_rows = []
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
    try:
        counter_val = int(r.get(COUNTER_KEY) or 0)
    except Exception:
        counter_val = -1
    try:
        llen = r.llen(UTM_RECENT_LIST_KEY)
        slen = r.scard(UTM_RECENT_SET_KEY)
    except Exception:
        llen = slen = -1
    return {
        "ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET, "prefix": GCS_PREFIX,
        "video_id": VIDEO_ID,
        "skip_resolve": SKIP_RESOLVE,
        "models_loaded": {
            "cb": bool(_cb_model),
            "rf": bool(_rf_model),
            "xgb": bool(_xgb_model),
            "stack": bool(_stack_model),
        },
        "serving_mode_current": _current_mode,
        "meta": {"n_features": len(_FEATURE_NAMES_FROM_META), "cat_idx": _CAT_IDX_FROM_META},
        "thr_stack": _thresholds_stack,
        "thr_cb": _thresholds_cb,
        "thr_rf": _thresholds_rf,
        "thr_xgb": _thresholds_xgb,
        "counter_ttl": ttl,
        "counter_val": counter_val,
        "recent_len_list": llen,
        "recent_len_set": slen,
        "recent_cap": UTM_RECENT_MAX,
        "te_loaded": bool(_TE_STATS)
    }

@app.get("/robots.txt")
def robots():
    return PlainTextResponse("User-agent: *\nDisallow:\n", status_code=200)

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def home():
    try:
        index_path = os.path.join(BASE_DIR, "index.html")
        with open(index_path, "r", encoding="utf-8") as f:
            html = f.read()
        headers = {"Cache-Control": "public, max-age=3600"}
        return HTMLResponse(content=html, status_code=200, headers=headers)
    except FileNotFoundError:
        return HTMLResponse(
            content="""
            <!doctype html><meta charset="utf-8">
            <title>Rasant</title>
            <body style="font-family:system-ui;padding:24px;background:#0b0d12;color:#f8fafc">
              <h1>Rasant</h1>
              <p>Página inicial temporária. Coloque <code>index.html</code> na raiz do projeto.</p>
              <p><a href="/privacy" style="color:#2F6FD7">Política de Privacidade</a></p>
            </body>
            """,
            status_code=200
        )

@app.get("/privacy", response_class=HTMLResponse)
async def privacy():
    try:
        privacy_path = os.path.join(BASE_DIR, "privacy.html")
        with open(privacy_path, "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(content=html, status_code=200, headers={"Cache-Control": "public, max-age=3600"})
    except FileNotFoundError:
        return PlainTextResponse("privacy.html não encontrado", status_code=404)

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

# ===== Handler principal =====
@app.get("/{full_path:path}")
def track_number_and_redirect(request: Request, full_path: str = Path(...), cat: Optional[str] = Query(None), mode: Optional[str] = Query(None)):
    try:
        incoming_url, in_mode = _build_incoming_from_request(request, full_path)
    except HTTPException as he:
        return JSONResponse({"ok": False, "error": he.detail}, status_code=he.status_code)

    ts = int(time.time())
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts))
    headers = request.headers
    cookie_header = headers.get("cookie") or headers.get("Cookie")
    referrer = headers.get("referer") or "-"
    user_agent = headers.get("user-agent", "-")
    fbclid = request.query_params.get("fbclid")
    ttclid = request.query_params.get("ttclid")
    ip_addr = (request.client.host if request.client else "0.0.0.0")
    device_name, os_family, os_version = parse_device_info(user_agent)

    parts_in = urlsplit(incoming_url)
    resolved_url = _fix_scheme(incoming_url) if SKIP_RESOLVE else (_resolve_short_follow(incoming_url) if _is_short_domain(parts_in.netloc) else _fix_scheme(incoming_url))
    parts_res = urlsplit(resolved_url)
    if not parts_res.scheme or not parts_res.netloc:
        return JSONResponse({"ok": False, "error": f"URL inválida: {resolved_url}"}, status_code=400)
    if not _is_allowed_long(parts_res.netloc) and not _is_short_domain(parts_res.netloc):
        return JSONResponse({"ok": False, "error": f"Domínio não permitido: {parts_res.netloc}"}, status_code=400)

    # ===== UTM: usa SOMENTE o parâmetro 'uc' =====
    outer_uc = request.query_params.get("uc")
    if not outer_uc:
        return JSONResponse({"ok": False, "error": "utm_missing_uc"}, status_code=400)
    utm_original, utm_original_01 = _normalize_utm_content(outer_uc)
    seed_for_numbering = (utm_original_01 or utm_original)
    seed_clean_for_numbered = re.sub(r'[^A-Za-z0-9\-]', '', seed_for_numbering) or "n"
    utm_numbered = next_unique_numbered(seed_clean_for_numbered)

    origin_url = _set_utm_content_preserving_order(resolved_url, utm_numbered)

    # ===== content_ids / contents =====
    content_ids, contents_payload = _build_content_identifiers(origin_url)

    # ===== Short oficial (sempre) — subId3 = UTMNumered
    sub_id_api = sanitize_subid_for_shopee(utm_numbered) or "na"
    dest = generate_short_link(origin_url, sub_id_api)

    # ===== Cookies / IDs persistentes =====
    fbp_cookie = get_cookie_value(cookie_header, "_fbp") or _gen_fbp(ts)
    fbc_cookie = get_cookie_value(cookie_header, "_fbc") or _gen_fbc_from_fbp(fbp_cookie, ts)
    if fbclid:
        fbc_cookie = build_fbc_from_fbclid(fbclid, creation_ts=ts) or fbc_cookie
    eid_cookie = get_cookie_value(cookie_header, "_eid") or _sha256_lower(fbp_cookie)

    r.setex(f"{USERDATA_KEY_PREFIX}{utm_numbered}", USERDATA_TTL_SECONDS, json.dumps({
        "ip": ip_addr, "ua": user_agent, "referrer": referrer,
        "event_source_url": origin_url, "short_link": dest,
        "vc_time": ts,
        "utm_original": utm_original,
        "utm_original_01": utm_original_01,
        "subid_base": utm_original,
        "subid_full": (utm_original_01 or utm_original),
        "utm_numbered": utm_numbered,
        "fbclid": fbclid, "fbp": fbp_cookie, "fbc": fbc_cookie, "eid": eid_cookie, "mode": in_mode
    }, ensure_ascii=False))

    # ===== external_id / user_data_meta (Meta) =====
    external_id = eid_cookie or _sha256_lower(f"{user_agent}|{ip_addr}")
    user_data_meta = {
        "client_ip_address": ip_addr,
        "client_user_agent": user_agent,
        "external_id": external_id,
        **({"fbp": fbp_cookie} if fbp_cookie else {}),
        **({"fbc": fbc_cookie} if fbc_cookie else {})
    }

    # ===== Geo + Features =====
    geo = geo_lookup(ip_addr)
    feats_dict = _features_full_dict(os_family, device_name, os_version, referrer, utm_original, iso_time, geo)

    # ===== Predições (stubs seguros) =====
    p_cb_map, p_avail = _compute_all_probs(feats_dict)
    p_cb  = p_avail.get("cb"); p_rf  = p_avail.get("rf"); p_xgb = p_avail.get("xgb"); p_stack = p_avail.get("stack")
    forced_mode = (mode or "").strip().lower() if mode else None
    active_mode = _choose_active_mode(forced_mode, p_avail)

    if active_mode == "stack" and p_stack is not None:
        p_c = float(p_stack);  thr_q, thr_a = _thresholds_for_mode("stack")
        p_map = {"comprou": p_c, "quase": 0.0, "desinteressado": 1.0 - p_c}
    elif active_mode == "cb" and p_cb is not None:
        p_c = float(p_cb);     thr_q, thr_a = _thresholds_for_mode("cb")
        p_map = {"comprou": p_c, "quase": float(p_cb_map.get("quase", 0.0)), "desinteressado": float(p_cb_map.get("desinteressado", 1.0 - p_c))}
    elif active_mode == "xgb" and p_xgb is not None:
        p_c = float(p_xgb);    thr_q, thr_a = _thresholds_for_mode("xgb")
        p_map = {"comprou": p_c, "quase": 0.0, "desinteressado": 1.0 - p_c}
    elif active_mode == "rf" and p_rf is not None:
        p_c = float(p_rf);     thr_q, thr_a = _thresholds_for_mode("rf")
        p_map = {"comprou": p_c, "quase": 0.0, "desinteressado": 1.0 - p_c}
    else:
        p_c, thr_q, thr_a = 0.0, 0.5, 0.5
        p_map = {"comprou": 0.0, "quase": 0.0, "desinteressado": 1.0}
        active_mode = "none"

    is_qvc_flag = 1 if p_c >= thr_q else 0
    is_atc_flag = 1 if p_c >= thr_a else 0
    pred_label = f"{'comprou' if is_atc_flag else ('comprou_qvc' if is_qvc_flag else 'desinteressado')}"

    # ===== Meta: ViewContent =====
    meta_view = send_meta_event("ViewContent", utm_numbered, ts, origin_url, user_data_meta, content_ids, contents_payload)

    # ===== TikTok: ViewContent =====
    tiktok_view = send_tiktok_event(
        event_name="ViewContent",
        event_id=utm_numbered,
        ts=ts,
        page_url=origin_url,
        ip=ip_addr,
        user_agent=user_agent,
        ttclid=ttclid,
        currency="BRL",
        value=0.0,
        content_ids=content_ids,
        contents_payload=contents_payload,
    )

    # ===== Pinterest: view_item =====
    pinterest_view = send_pinterest_event(
        event_name="view_item",
        event_id=utm_numbered,
        ts=ts,
        page_url=origin_url,
        ip=ip_addr,
        user_agent=user_agent,
        currency="BRL",
        value=0.0,
        content_ids=content_ids,
        contents_payload=contents_payload
    )

    # ===== Meta: AddToCart (quando ATC flag) =====
    meta_sent = ""
    if is_atc_flag == 1:
        meta_sent = send_meta_event("AddToCart", utm_numbered, ts, origin_url, user_data_meta, content_ids, contents_payload)

    # ===== TikTok: AddToCart =====
    tiktok_sent = ""
    if is_atc_flag == 1:
        tiktok_sent = send_tiktok_event(
            event_name="AddToCart",
            event_id=utm_numbered,
            ts=ts,
            page_url=origin_url,
            ip=ip_addr,
            user_agent=user_agent,
            ttclid=ttclid,
            currency="BRL",
            value=0.0,
            content_ids=content_ids,
            contents_payload=contents_payload,
        )

    # ===== Pinterest: add_to_cart =====
    pinterest_sent = ""
    if is_atc_flag == 1:
        pinterest_sent = send_pinterest_event(
            event_name="add_to_cart",
            event_id=utm_numbered,
            ts=ts,
            page_url=origin_url,
            ip=ip_addr,
            user_agent=user_agent,
            currency="BRL",
            value=0.0,
            content_ids=content_ids,
            contents_payload=contents_payload
        )

    # ===== Log CSV =====
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

    hour_log = _extract_hour_from_iso_for_log(iso_time)
    dow_log = _extract_dow_from_iso_for_log(iso_time)
    ref_domain_log = _get_first_domain_for_log(referrer)
    os_version_num_log = 0.0
    try:
        m = re.search(r"(\d+(\.\d+)*)", str(os_version or ""))
        if m: os_version_num_log = float(m.group(1).split(".")[0])
    except: pass
    is_android_log = 1 if "android" in (os_family or "").lower() else 0
    is_ios_log     = 1 if re.search(r"ios|iphone|ipad", (os_family or ""), re.I) else 0
    part_of_day_log = _part_of_day_from_hour(hour_log)

    pcomprou_pct = p_c * 100.0
    thr_qvc_pct  = thr_q * 100.0
    thr_atc_pct  = thr_a * 100.0
    gap_qvc      = p_c - thr_q
    gap_atc      = p_c - thr_a
    pct_of_atc   = (p_c / thr_a * 100.0) if thr_a > 0 else 0.0

    csv_row = [
        ts, iso_time, ip_addr, user_agent, referrer,
        incoming_url, resolved_url, origin_url, (cat or ""),
        utm_original, utm_original_01, utm_original, (utm_original_01 or utm_original), utm_numbered, fbclid or "", fbp_cookie or "", fbc_cookie or "",
        device_name, os_family, os_version,
        geo.get("geo_status",""), geo.get("geo_country",""), geo.get("geo_region",""), geo.get("geo_state",""),
        geo.get("geo_city",""), geo.get("geo_zip",""), geo.get("geo_lat",""), geo.get("geo_lon",""),
        geo.get("geo_isp",""), geo.get("geo_org",""), geo.get("geo_asn",""),
        part_of_day_log, hour_log, dow_log, ref_domain_log, os_version_num_log, is_android_log, is_ios_log,
        pred_label, float(p_map.get("comprou",0.0)), float(p_map.get("quase",0.0)), float(p_map.get("desinteressado",0.0)),
        float(thr_q), float(thr_a), int(is_qvc_flag), int(is_atc_flag),
        pcomprou_pct, thr_qvc_pct, thr_atc_pct, gap_qvc, gap_atc, pct_of_atc,
        meta_sent, meta_view,
        tiktok_sent, tiktok_view,
        pinterest_sent, pinterest_view
    ]
    with _buffer_lock:
        _buffer_rows.append(csv_row)

    # ===== Redirect + cookies =====
    resp = RedirectResponse(dest, status_code=302)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.set_cookie("_fbp", fbp_cookie, max_age=63072000, path="/", samesite="lax")
    resp.set_cookie("_fbc", fbc_cookie, max_age=63072000, path="/", samesite="lax")
    resp.set_cookie("_eid", eid_cookie, max_age=63072000, path="/", samesite="lax")

    # Pinterest UA: devolver HTML com refresh (mantido)
    if "pinterest" in (user_agent or "").lower():
        html = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="0.2;url={dest}">
<title>Redirecionando…</title>
<script>setTimeout(function(){{ window.location.replace("{dest}"); }}, 200);</script>
</head><body>Redirecionando…</body></html>"""
        return HTMLResponse(content=html, status_code=200, headers={
            "Cache-Control": "no-store, max-age=0",
        })

    return resp

# ===================== Admin (upload opcional) =====================
@app.get("/admin", response_class=HTMLResponse)
def admin_page():
    return """
    <html>
      <head><meta charset="utf-8"><title>Upload do Modelo (Multi-Model)</title></head>
      <body style="font-family: sans-serif; max-width: 720px; margin: 40px auto;">
        <h2>Enviar modelos</h2>
        <p><b>Arquivos aceitos (opcionais):</b></p>
        <ul>
          <li><code>model_comprou.cbm</code> (CatBoost)</li>
          <li><code>model_meta_comprou.json</code> (meta, thresholds, TE)</li>
          <li><code>rf.joblib</code> (RandomForest)</li>
          <li><code>xgb.json</code> (XGBoost)</li>
          <li><code>stack_model.joblib</code> (LogReg do stack)</li>
          <li><code>stack_meta.json</code> (ordem das features do stack)</li>
          <li><code>te_stats.json</code> (fallback do Target Encoding)</li>
        </ul>
        <form method="post" action="/admin/upload_models_bundle" enctype="multipart/form-data">
          <div>Token (X-Admin-Token): <input name="token" type="password" required /></div><br/>
          <div>model_comprou.cbm: <input name="cb" type="file" /></div><br/>
          <div>model_meta_comprou.json: <input name="meta" type="file" /></div><br/>
          <div>rf.joblib: <input name="rf" type="file" /></div><br/>
          <div>xgb.json: <input name="xgb" type="file" /></div><br/>
          <div>stack_model.joblib: <input name="stack" type="file" /></div><br/>
          <div>stack_meta.json: <input name="stack_meta" type="file" /></div><br/>
          <div>te_stats.json: <input name="te" type="file" /></div><br/>
          <button type="submit">Enviar</button>
        </form>
        <p>Depois de enviar, abra <code>/health</code> para conferir modelos e thresholds carregados.</p>
      </body>
    </html>
    """

@app.post("/admin/upload_models_bundle")
async def admin_upload_models_bundle(
    token: str = Form(...),
    cb: UploadFile = File(None),
    meta: UploadFile = File(None),
    rf: UploadFile = File(None),
    xgb: UploadFile = File(None),
    stack: UploadFile = File(None),
    stack_meta: UploadFile = File(None),
    te: UploadFile = File(None),
):
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    try:
        if cb:
            data = await cb.read()
            open(MODEL_CB_PATH, "wb").write(data)
        if meta:
            data = await meta.read()
            if data and data.strip():
                json.loads(data.decode("utf-8"))
            open(MODEL_META_PATH, "wb").write(data)
        if rf:
            data = await rf.read()
            open(MODEL_RF_PATH, "wb").write(data)
        if xgb:
            data = await xgb.read()
            if data and data.strip():
                json.loads(data.decode("utf-8"))
            open(MODEL_XGB_PATH, "wb").write(data)
        if stack:
            data = await stack.read()
            open(MODEL_STACK_PATH, "wb").write(data)
        if stack_meta:
            data = await stack_meta.read()
            if data and data.strip():
                json.loads(data.decode("utf-8"))
            open(STACK_META_PATH, "wb").write(data)
        if te:
            data = await te.read()
            open(TE_STATS_PATH, "wb").write(data)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="JSON inválido em algum artefato .json")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"falha ao salvar arquivo(s): {e}")

    _load_models_if_available()
    return {
        "ok": True,
        "models_loaded": {"cb": bool(_cb_model), "rf": bool(_rf_model), "xgb": bool(_xgb_model), "stack": bool(_stack_model)},
        "thr_stack": _thresholds_stack, "thr_cb": _thresholds_cb, "thr_rf": _thresholds_rf, "thr_xgb": _thresholds_xgb,
        "te_loaded": bool(_TE_STATS)
    }

@app.get("/admin/flush")
def admin_flush(token: str):
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    sent = _flush_buffer_to_gcs()
    return {"ok": True, "sent_rows": sent}

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

