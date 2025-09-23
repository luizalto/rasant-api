# -*- coding: utf-8 -*-
"""
Servidor FastAPI – Shopee ShortLink otimizado + Redis + GCS (flush background)
Fluxo rápido (inalterado):
- Recebe link com UTM -> gera utm_numbered=<base>N<seq> -> short-link Shopee -> log básico -> redirect imediato

Background (só para UTMs que começam com '68novolink'):
- Geo por IP (API configurável + cache Redis)
- Predição (modelo local .cbm via /admin/upload_model, ou serviço externo via MODEL_API_URL)
- Se pred=comprador -> envia AddToCart via Meta Ads Conversions API (CAPI)
- Log enriquecido (geo + pred) em outro prefixo no GCS

Também expõe:
- /admin/upload_model (upload model.cbm + model_meta.json)
- /admin/model_info
- /admin/flush  (flush manual dos buffers)
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json, pathlib
from typing import Optional, Dict, Tuple, List, Any
from queue import Queue

import requests
import redis
from fastapi import FastAPI, Request, Query, Path, UploadFile, File, Header, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse
from google.cloud import storage
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode

# (opcional) CatBoost para modelo local
CB_AVAILABLE = False
try:
    from catboost import CatBoostClassifier, Pool  # type: ignore
    CB_AVAILABLE = True
except Exception:
    CB_AVAILABLE = False

# ───────────────── Config (env) ─────────────────
DEFAULT_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "12"))

# GCS
GCS_BUCKET        = os.getenv("GCS_BUCKET")                         # ex: "utm-click-logs"
GCS_PREFIX        = os.getenv("GCS_PREFIX", "logs/")                # log básico (rápido)
GCS_ENR_PREFIX    = os.getenv("GCS_ENR_PREFIX", "logs_enriched/")   # log enriquecido (geo + pred)
FLUSH_MAX_ROWS    = int(os.getenv("FLUSH_MAX_ROWS", "500"))
FLUSH_MAX_SECONDS = int(os.getenv("FLUSH_MAX_SECONDS", "30"))

# Admin
ADMIN_TOKEN       = os.getenv("ADMIN_TOKEN", "12345678")

# Redis
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COUNTER_KEY          = os.getenv("UTM_COUNTER_KEY", "utm_counter")
USERDATA_TTL_SECONDS = int(os.getenv("USERDATA_TTL_SECONDS", "604800"))
USERDATA_KEY_PREFIX  = os.getenv("USERDATA_KEY_PREFIX", "ud:")
GEO_CACHE_TTL        = int(os.getenv("GEO_CACHE_TTL", "86400"))  # 1 dia

VIDEO_ID = os.getenv("VIDEO_ID", "v15")

# Shopee Affiliate
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "18314810331")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "LO3QSEG45TYP4NYQBRXLA2YYUL3ZCUPN")
SHOPEE_ENDPOINT   = "https://open-api.affiliate.shopee.com.br/graphql"

# Geo API (ex: ip-api.com) – use {ip} no template
GEOIP_API_URL = os.getenv(
    "GEOIP_API_URL",
    "http://ip-api.com/json/{ip}?fields=status,country,region,regionName,city,zip,lat,lon,isp,org,as"
)

# Predição – opção 1 (HTTP externo) ou opção 2 (modelo local)
MODEL_API_URL    = os.getenv("MODEL_API_URL")        # ex: "https://meu-ml/predict"
MODELS_DIR       = os.getenv("MODELS_DIR", "/srv/models")
MODEL_LOCAL_PATH = os.getenv("MODEL_LOCAL_PATH")     # ex: "/srv/models/model.cbm"
os.makedirs(MODELS_DIR, exist_ok=True)

# Meta Ads (CAPI)
FB_ACCESS_TOKEN  = os.getenv("FB_ACCESS_TOKEN")      # obrigatório p/ enviar evento
FB_PIXEL_ID      = os.getenv("FB_PIXEL_ID")          # obrigatório p/ enviar evento
FB_TEST_EVENT_CODE = os.getenv("FB_TEST_EVENT_CODE") # opcional p/ modo teste (Events Manager)

# ───────────────── App / clients ─────────────────
app = FastAPI(title="Shopee UTM Numbered → ShortLink + Redis + GCS + Meta CAPI")

r = redis.from_url(REDIS_URL)

_gcs_client: Optional[storage.Client] = None
_bucket: Optional[storage.Bucket] = None
if GCS_BUCKET:
    _gcs_client = storage.Client()  # usa GOOGLE_APPLICATION_CREDENTIALS do ambiente
    _bucket = _gcs_client.bucket(GCS_BUCKET)

session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50)
session.mount("http://", _adapter)
session.mount("https://", _adapter)

# ───────────────── Buffers de CSV ─────────────────
_buffer_lock = threading.Lock()
_buffer_rows: List[List[str]] = []
_last_flush_ts = time.time()

CSV_HEADERS = [
    "timestamp","iso_time","ip","user_agent","device_name","os_family","os_version","referrer",
    "short_link_in","resolved_url","final_url","category",
    "utm_original","utm_numbered",
    "sub_id1","sub_id2","sub_id3","sub_id4","sub_id5",
    "fbclid","fbp","fbc"
]

_enr_lock = threading.Lock()
_enr_rows: List[List[str]] = []

ENR_HEADERS = CSV_HEADERS + [
    "geo_status","geo_country","geo_region","geo_city","geo_zip","geo_lat","geo_lon","geo_isp","geo_org","geo_asn",
    "pred_label","pred_score"
]

# fila para enrichment
_bg_queue: "Queue[Dict[str, Any]]" = Queue()

# ───────────────── Helpers ─────────────────
def incr_counter() -> int:
    return int(r.incr(COUNTER_KEY))

def get_cookie_value(cookie_header: Optional[str], name: str) -> Optional[str]:
    if not cookie_header: return None
    try:
        for it in [c.strip() for c in cookie_header.split(";")]:
            if it.startswith(name + "="):
                return it.split("=", 1)[1]
    except Exception:
        pass
    return None

def build_fbc_from_fbclid(fbclid: Optional[str], creation_ts: Optional[int] = None) -> Optional[str]:
    if not fbclid: return None
    if creation_ts is None: creation_ts = int(time.time())
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
    elif "Windows" in ua: os_family = "Windows"
    elif "Mac OS X" in ua or "Macintosh" in ua: os_family = "macOS"
    elif "Linux" in ua: os_family = "Linux"
    device_name = "iPhone" if "iphone" in ua_l else ("Android" if "android" in ua_l else "Desktop")
    return device_name, os_family, os_version

def parse_subids_from_query(qs: str) -> Dict[str, Optional[str]]:
    out = {"utm_content": None, "sub_id1": None, "sub_id2": None, "sub_id3": None, "sub_id4": None, "sub_id5": None}
    if not qs: return out
    q = urllib.parse.parse_qs(qs, keep_blank_values=True)
    if "utm_content" in q and q["utm_content"]:
        out["utm_content"] = q["utm_content"][0]
    for i in range(5):
        key_idx = f"subIds[{i}]"
        if key_idx in q and q[key_idx]:
            out[f"sub_id{i+1}"] = q[key_idx][0]
    if "subIds" in q:
        vals = q["subIds"]
        for i in range(min(5, len(vals))):
            out[f"sub_id{i+1}"] = out[f"sub_id{i+1}"] or vals[i]
    return out

def add_or_update_query_param(raw_url: str, key: str, value: str) -> str:
    parsed = urlsplit(raw_url)
    q = parse_qs(parsed.query, keep_blank_values=True)
    q[key] = [value]
    new_query = urlencode(q, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))

def extract_utm_content_from_url(u: str) -> str:
    try:
        qs = urlsplit(u).query
        if not qs:
            return ""
        q = parse_qs(qs, keep_blank_values=True)
        return (q.get("utm_content") or [""])[0]
    except:
        return ""

def resolve_short_link(short_url: str, max_hops: int = 1) -> Tuple[str, Dict[str, Optional[str]]]:
    current = short_url
    final_url = short_url
    try:
        resp = session.get(current, allow_redirects=False, timeout=DEFAULT_TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (resolver/1.0)"
        })
        if 300 <= resp.status_code < 400 and "Location" in resp.headers:
            location = resp.headers["Location"]
            current = urllib.parse.urljoin(current, location)
            final_url = current
    except Exception:
        final_url = short_url
    parsed = urllib.parse.urlsplit(final_url)
    subids = parse_subids_from_query(parsed.query)
    return final_url, subids

# Shopee short-link
_SUBID_MAXLEN = int(os.getenv("SHOPEE_SUBID_MAXLEN", "50"))
_SUBID_REGEX  = re.compile(r"[^A-Za-z0-9]")

def sanitize_subid_for_shopee(value: str) -> str:
    if not value: return "na"
    cleaned = _SUBID_REGEX.sub("", value)
    return cleaned[:_SUBID_MAXLEN] if cleaned else "na"

def generate_short_link(origin_url: str, utm_content_for_api: str) -> str:
    payload_obj = {
        "query": (
            "mutation{generateShortLink(input:{"
            f"originUrl:\"{origin_url}\","
            f"subIds:[\"\",\"\",\"{utm_content_for_api}\",\"\",\"\"]"
            "}){shortLink}}"
        )
    }
    payload = json.dumps(payload_obj, separators=(',', ':'), ensure_ascii=False)
    ts = str(int(time.time()))
    signature = hashlib.sha256((SHOPEE_APP_ID + ts + payload + SHOPEE_APP_SECRET).encode("utf-8")).hexdigest()
    headers = {
        "Authorization": f"SHA256 Credential={SHOPEE_APP_ID}, Timestamp={ts}, Signature={signature}",
        "Content-Type": "application/json"
    }
    try:
        resp = session.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=(2, 10))
        j = resp.json()
        short = (((j or {}).get("data") or {}).get("generateShortLink") or {}).get("shortLink")
        if not short:
            raise ValueError(f"Resposta Shopee sem shortLink: {j}")
        return short
    except Exception as e:
        print(f"[ShopeeShortLink] ERRO ao gerar shortlink: {e}. Fallback para URL numerada.")
        return origin_url

# GCS helpers
def _day_key(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts))

def _gcs_object_name(ts: int, part: int, prefix: str) -> str:
    d = _day_key(ts)
    return f"{prefix}date={d}/clicks_{d}_part-{part:04d}.csv"

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
    blob_name = _gcs_object_name(int(time.time()), part, GCS_PREFIX)
    _bucket.blob(blob_name).upload_from_string(data, content_type="text/csv")
    print(f"[FLUSH] {len(rows)} linha(s) → gs://{GCS_BUCKET}/{blob_name}")
    return len(rows)

def _flush_enriched_to_gcs() -> int:
    if not _bucket:
        return 0
    with _enr_lock:
        rows = list(_enr_rows)
        if len(rows) == 0:
            return 0
        _enr_rows.clear()
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(ENR_HEADERS)
    w.writerows(rows)
    data = output.getvalue().encode("utf-8")
    part = int(time.time() * 1000) % 10_000_000
    blob_name = _gcs_object_name(int(time.time()), part, GCS_ENR_PREFIX)
    _bucket.blob(blob_name).upload_from_string(data, content_type="text/csv")
    print(f"[ENR-FLUSH] {len(rows)} linha(s) → gs://{GCS_BUCKET}/{blob_name}")
    return len(rows)

def _background_flusher():
    while True:
        try:
            _flush_buffer_to_gcs()
            _flush_enriched_to_gcs()
        except Exception as e:
            print("[FLUSH-ERR]", e)
        time.sleep(FLUSH_MAX_SECONDS)

threading.Thread(target=_background_flusher, daemon=True).start()

# ────────────── Geo + Predição + Meta CAPI ──────────────
def _geo_cache_key(ip: str) -> str:
    return f"geo:{ip}"

def fetch_geo(ip: str) -> Dict[str, Any]:
    if not ip:
        return {"geo_status":"fail"}
    ck = _geo_cache_key(ip)
    cached = r.get(ck)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass
    url = GEOIP_API_URL.replace("{ip}", ip)
    try:
        resp = session.get(url, timeout=5)
        j = resp.json() if resp.ok else {}
    except Exception as e:
        j = {"status":"fail", "error": str(e)}
    out = {
        "geo_status": j.get("status","fail"),
        "geo_country": j.get("country"),
        "geo_region": j.get("region"),
        "geo_city": j.get("city"),
        "geo_zip": j.get("zip"),
        "geo_lat": j.get("lat"),
        "geo_lon": j.get("lon"),
        "geo_isp": j.get("isp"),
        "geo_org": j.get("org"),
        "geo_asn": j.get("as"),
    }
    try:
        r.setex(ck, GEO_CACHE_TTL, json.dumps(out, ensure_ascii=False))
    except Exception:
        pass
    return out

_MODEL_LOCAL: Optional["CatBoostClassifier"] = None
_META_FEATURES: Optional[List[str]] = None
_META_CAT_IDX: Optional[List[int]] = None

def _load_model_from_disk(model_path: str, meta_path: Optional[str] = None):
    global _MODEL_LOCAL, _META_FEATURES, _META_CAT_IDX
    if not CB_AVAILABLE:
        raise RuntimeError("CatBoost não disponível no ambiente")
    m = CatBoostClassifier()
    m.load_model(model_path)
    _MODEL_LOCAL = m
    _META_FEATURES = None
    _META_CAT_IDX = None
    if meta_path and os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        _META_FEATURES = meta.get("feature_names")
        _META_CAT_IDX  = meta.get("cat_idx")
    print("[MODEL] Modelo local carregado:", model_path)

def _try_load_local_model_once():
    if not MODEL_LOCAL_PATH or not CB_AVAILABLE:
        return
    if _MODEL_LOCAL is None and os.path.exists(MODEL_LOCAL_PATH):
        meta_path = os.path.join(os.path.dirname(MODEL_LOCAL_PATH), "model_meta.json")
        _load_model_from_disk(MODEL_LOCAL_PATH, meta_path if os.path.exists(meta_path) else None)

_try_load_local_model_once()

def _part_of_day(hour: Optional[int]) -> str:
    try:
        h = int(hour)
    except Exception:
        return "unknown"
    if 0<=h<6: return "dawn"
    if 6<=h<12: return "morning"
    if 12<=h<18: return "afternoon"
    if 18<=h<=23: return "evening"
    return "unknown"

def _version_num(s):
    m = re.search(r"(\d+(\.\d+)*)", str(s or ""))
    if not m: return 0.0
    try: return float(m.group(1).split(".")[0])
    except: return 0.0

def _ref_domain(url: str) -> str:
    try:
        from urllib.parse import urlparse
        netloc = urlparse(str(url or "")).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except: return ""

def _utm_base_prefix(utm_original: str, utm_numbered: str) -> str:
    base = (utm_original or "").strip()
    if base:
        return re.sub(r'[^A-Za-z0-9]', '', base)
    m = re.match(r'^([A-Za-z0-9]+)N\d+$', utm_numbered or "")
    if m:
        return m.group(1)
    m2 = re.match(r'^([A-Za-z0-9]+)', utm_numbered or "")
    return m2.group(1) if m2 else ""

def build_features_row(event: Dict[str, Any], geo: Dict[str, Any]) -> Dict[str, Any]:
    hour = None
    if event.get("hora_br"):
        try: hour = int(str(event["hora_br"]).split(":")[0])
        except: hour = None
    elif event.get("iso_time"):
        try:
            fmt = "%Y-%m-%dT%H:%M:%S"
            hour = int(time.strptime(event["iso_time"][:19], fmt).tm_hour)
        except: hour = None

    os_family = str(event.get("os_family") or "")
    os_version = str(event.get("os_version") or "")
    device_name = str(event.get("device_name") or "")

    row = {
        "device_name": device_name,
        "os_family": os_family,
        "part_of_day": _part_of_day(hour),
        "geo_country": str(geo.get("geo_country") or ""),
        "geo_region":  str(geo.get("geo_region") or ""),
        "geo_city":    str(geo.get("geo_city") or ""),
        "geo_zip":     str(geo.get("geo_zip") or ""),
        "geo_isp":     str(geo.get("geo_isp") or ""),
        "geo_org":     str(geo.get("geo_org") or ""),
        "ref_domain":  _ref_domain(event.get("referrer") or ""),
        "utm_prefix":  (re.match(r"^([A-Za-z0-9]+)", str(event.get("utm_numbered") or "")) or [None]).group(1) if re.match(r"^([A-Za-z0-9]+)", str(event.get("utm_numbered") or "")) else "",
        "hour": float(hour or 0),
        "os_version_num": float(_version_num(os_version)),
        "is_android": 1.0 if "android" in os_family.lower() else 0.0,
        "is_ios":     1.0 if any(x in os_family.lower() for x in ["ios","iphone","ipad"]) else 0.0,
    }
    return row

def predict_buyer(event: Dict[str, Any], geo: Dict[str, Any]) -> Tuple[Optional[int], Optional[float]]:
    # prioridade: MODEL_API_URL -> modelo local (.cbm) -> None
    if MODEL_API_URL:
        try:
            payload = {"event": event, "geo": geo}
            resp = session.post(MODEL_API_URL, json=payload, timeout=5)
            if resp.ok:
                j = resp.json()
                return j.get("pred"), j.get("score")
        except Exception as e:
            print("[PRED] Falha MODEL_API_URL:", e)

    if _MODEL_LOCAL is not None:
        try:
            row = build_features_row(event, geo)
            if _META_FEATURES:
                row = {c: row.get(c, "") for c in _META_FEATURES}
            import pandas as pd
            df = pd.DataFrame([row])
            pool = Pool(df, cat_features=_META_CAT_IDX if _META_CAT_IDX else None)
            proba = _MODEL_LOCAL.predict_proba(pool)[0]
            score = float(proba[-1]) if len(proba)==2 else float(max(proba))
            pred = 1 if score >= 0.5 else 0
            return pred, score
        except Exception as e:
            print("[PRED] Falha modelo local:", e)

    return None, None

def send_meta_add_to_cart(event: Dict[str, Any], geo: Dict[str, Any], score: Optional[float]):
    """Dispara AddToCart via Meta Ads CAPI."""
    if not (FB_ACCESS_TOKEN and FB_PIXEL_ID):
        return

    # construir user_data com IP/UA e cookies se houver
    user_data = {
        "client_ip_address": event.get("ip"),
        "client_user_agent": event.get("user_agent")
    }
    # opcional: fbp/fbc melhoram matching
    if event.get("fbp"):
        user_data["fbp"] = event.get("fbp")
    if event.get("fbc"):
        user_data["fbc"] = event.get("fbc")

    data_event = {
        "event_name": "AddToCart",
        "event_time": int(time.time()),
        "action_source": "website",
        "event_source_url": event.get("event_source_url") or event.get("final_url") or "",
        "user_data": user_data,
        "custom_data": {
            # você pode adicionar conteúdo real aqui (id/valor/moeda) se tiver
            "content_type": "product",
            "utm_numbered": event.get("utm_numbered"),
            "score": score
        }
    }

    payload = {"data": [data_event]}
    if FB_TEST_EVENT_CODE:
        payload["test_event_code"] = FB_TEST_EVENT_CODE

    url = f"https://graph.facebook.com/v19.0/{FB_PIXEL_ID}/events"
    try:
        resp = session.post(url, params={"access_token": FB_ACCESS_TOKEN}, json=payload, timeout=6)
        if not resp.ok:
            print("[META] erro:", resp.status_code, resp.text[:300])
    except Exception as e:
        print("[META] exceção:", e)

def enqueue_enrichment(row_basic: Dict[str, Any]):
    _bg_queue.put(row_basic)

def _enrichment_worker():
    """geo -> predição -> se comprador envia Meta CAPI -> grava linha enriquecida no GCS"""
    while True:
        item = _bg_queue.get()
        try:
            ip = item.get("ip") or ""
            geo = fetch_geo(ip)
            event = {
                "iso_time": item.get("iso_time"),
                "os_family": item.get("os_family"),
                "device_name": item.get("device_name"),
                "os_version": item.get("os_version"),
                "referrer": item.get("referrer"),
                "utm_numbered": item.get("utm_numbered"),
                "ip": ip,
                "user_agent": item.get("user_agent"),
                "event_source_url": item.get("final_url"),
                "fbp": item.get("fbp"), "fbc": item.get("fbc")
            }
            pred, score = predict_buyer(event, geo)

            # só envia para Meta se comprador
            if pred == 1:
                send_meta_add_to_cart(event, geo, score)

            # guarda linha enriquecida no buffer
            enr = [
                item["timestamp"], item["iso_time"], ip, item["user_agent"], item["device_name"], item["os_family"],
                item["os_version"], item["referrer"], item["short_link_in"], item["resolved_url"], item["final_url"],
                item.get("category",""), item["utm_original"], item["utm_numbered"],
                item.get("sub_id1",""), item.get("sub_id2",""), item.get("sub_id3",""), item.get("sub_id4",""), item.get("sub_id5",""),
                item.get("fbclid",""), item.get("fbp",""), item.get("fbc",""),
                geo.get("geo_status","fail"), geo.get("geo_country"), geo.get("geo_region"), geo.get("geo_city"),
                geo.get("geo_zip"), geo.get("geo_lat"), geo.get("geo_lon"), geo.get("geo_isp"), geo.get("geo_org"), geo.get("geo_asn"),
                ("comprador" if pred==1 else ("nao" if pred==0 else "")), (score if score is not None else "")
            ]
            with _enr_lock:
                _enr_rows.append(enr)
        except Exception as e:
            print("[ENR] erro:", e)
        finally:
            _bg_queue.task_done()

threading.Thread(target=_enrichment_worker, daemon=True).start()

# ───────────────── Rotas ─────────────────
@app.get("/health")
def health():
    return {
        "ok": True,
        "ts": int(time.time()),
        "bucket": GCS_BUCKET,
        "prefix": GCS_PREFIX,
        "model_local_loaded": (_MODEL_LOCAL is not None),
        "model_api": bool(MODEL_API_URL),
        "pixel": bool(FB_PIXEL_ID),
        "meta_token": bool(FB_ACCESS_TOKEN)
    }

@app.get("/")
def root():
    return PlainTextResponse("OK", status_code=200)

@app.get("/{full_path:path}")
def track_number_and_redirect(
    request: Request,
    full_path: str = Path(...),
    cat: Optional[str] = Query(None),
):
    raw_path = urllib.parse.unquote(full_path or "").strip()
    if not raw_path or raw_path == "favicon.ico":
        return JSONResponse({"ok": False, "error": "missing_url"}, status_code=400)
    if raw_path.startswith("//"):
        raw_path = "https:" + raw_path

    incoming_url = raw_path
    outer_uc = request.query_params.get("uc")  # UTM original por fora (zero rede)

    # básicos
    ts = int(time.time())
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts))
    headers = request.headers
    cookie_header = headers.get("cookie") or headers.get("Cookie")
    referrer = headers.get("referer") or "-"
    user_agent = headers.get("user-agent", "-")
    fbclid = request.query_params.get("fbclid")
    ip_addr = (request.client.host if request.client else "0.0.0.0")
    device_name, os_family, os_version = parse_device_info(user_agent)

    # UTM otimizada (inalterado)
    resolved_url = incoming_url
    utm_original = outer_uc or ""
    if not utm_original:
        utm_original = extract_utm_content_from_url(incoming_url)

    subids_in = {}
    if not utm_original:
        resolved_url, subids_in = resolve_short_link(incoming_url, max_hops=1)
        utm_original = subids_in.get("utm_content") or extract_utm_content_from_url(resolved_url)

    # gera utm_numbered sem tocar utm_content
    clean_base = re.sub(r'[^A-Za-z0-9]', '', utm_original or "") or "n"
    seq = incr_counter()
    utm_numbered = f"{clean_base}N{seq}"
    final_with_number = add_or_update_query_param(resolved_url, "utm_numbered", utm_numbered)

    # short-link Shopee
    sub_id_api = sanitize_subid_for_shopee(utm_numbered)
    dest = generate_short_link(final_with_number, sub_id_api)

    # snapshot no Redis
    fbp_cookie = get_cookie_value(cookie_header, "_fbp")
    fbc_val    = build_fbc_from_fbclid(fbclid, creation_ts=ts)
    snapshot = {
        "ip": ip_addr, "user_agent": user_agent, "referrer": referrer,
        "event_source_url": final_with_number, "short_link": dest,
        "vc_time": ts, "utm_original": utm_original, "utm_numbered": utm_numbered,
        "fbclid": fbclid, "fbp": fbp_cookie, "fbc": fbc_val,
        "os_family": os_family, "device_name": device_name, "os_version": os_version,
        "iso_time": iso_time
    }
    try:
        r.setex(f"{USERDATA_KEY_PREFIX}{utm_numbered}", USERDATA_TTL_SECONDS, json.dumps(snapshot))
    except Exception:
        pass

    # log básico (rápido)
    csv_row = [
        ts, iso_time, ip_addr, user_agent, device_name, os_family, os_version, referrer,
        incoming_url, resolved_url, final_with_number, (cat or ""),
        utm_original, utm_numbered,
        subids_in.get("sub_id1") or "", subids_in.get("sub_id2") or "",
        subids_in.get("sub_id3") or "", subids_in.get("sub_id4") or "", subids_in.get("sub_id5") or "",
        fbclid or "", fbp_cookie or "", fbc_val or ""
    ]
    with _buffer_lock:
        _buffer_rows.append(csv_row)

    # só enfileira enrichment se UTM começar com 68novolink
    base = (re.sub(r'[^A-Za-z0-9]', '', utm_original or "") or "").lower()
    if not base:
        m = re.match(r'^([A-Za-z0-9]+)', utm_numbered or "")
        base = (m.group(1).lower() if m else "")
    if base.startswith("68novolink"):
        enqueue_enrichment({
            "timestamp": ts, "iso_time": iso_time,
            "ip": ip_addr, "user_agent": user_agent, "referrer": referrer,
            "device_name": device_name, "os_family": os_family, "os_version": os_version,
            "short_link_in": incoming_url, "resolved_url": resolved_url, "final_url": final_with_number,
            "category": (cat or ""), "utm_original": utm_original, "utm_numbered": utm_numbered,
            "sub_id1": subids_in.get("sub_id1") or "", "sub_id2": subids_in.get("sub_id2") or "",
            "sub_id3": subids_in.get("sub_id3") or "", "sub_id4": subids_in.get("sub_id4") or "", "sub_id5": subids_in.get("sub_id5") or "",
            "fbclid": fbclid or "", "fbp": fbp_cookie or "", "fbc": fbc_val or ""
        })

    # redirect imediato
    return RedirectResponse(dest, status_code=302)

# ───────── Admin: upload/ativação de modelo ─────────
@app.post("/admin/upload_model")
async def admin_upload_model(
    model: UploadFile = File(...),            # model.cbm
    meta: UploadFile = File(None),            # model_meta.json (opcional)
    x_admin_token: str = Header(None)         # header: X-Admin-Token: <token>
):
    token = x_admin_token or ""
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

    model_path = os.path.join(MODELS_DIR, "model.cbm")
    meta_path  = os.path.join(MODELS_DIR, "model_meta.json")

    model_bytes = await model.read()
    pathlib.Path(model_path).write_bytes(model_bytes)

    meta_ok = False
    if meta is not None:
        meta_bytes = await meta.read()
        pathlib.Path(meta_path).write_bytes(meta_bytes)
        meta_ok = True

    try:
        _load_model_from_disk(model_path, meta_path if meta_ok else None)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha ao carregar modelo: {e}")

    return {"ok": True, "model_path": model_path, "meta_loaded": meta_ok}

@app.get("/admin/model_info")
def admin_model_info(x_admin_token: str = Header(None)):
    token = x_admin_token or ""
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    return {
        "loaded": (_MODEL_LOCAL is not None),
        "has_meta_features": bool(_META_FEATURES),
        "model_api": bool(MODEL_API_URL)
    }

@app.get("/admin/flush")
def admin_flush(token: str):
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    sent_basic = _flush_buffer_to_gcs()
    sent_enr   = _flush_enriched_to_gcs()
    return {"ok": True, "sent_rows_basic": sent_basic, "sent_rows_enriched": sent_enr}

# (opcional) upload_csv existente
@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    content = await file.read()
    text = content.decode("utf-8", errors="replace").splitlines()
    reader = csv.DictReader(text)
    processed: List[Dict[str, Any]] = []
    for row in reader:
        processed.append(row)
    return JSONResponse({"rows": processed})

@atexit.register
def _flush_on_exit():
    try:
        _flush_buffer_to_gcs()
        _flush_enriched_to_gcs()
    except Exception:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
