# -*- coding: utf-8 -*-
"""
FastAPI – Shopee ShortLink com UTM numerada (somente letras e números) + Logs em GCS + Redis + Google GA4

Melhorias:
- Endpoint assíncrono (httpx.AsyncClient global)
- Redireciona sempre para o short-link oficial da Shopee (fallback para URL longa numerada)
- GA4 e flush GCS em background
- Cache Redis para short-link Shopee
- Flush automático do GCS em thread separada
- Captura de IP sanitizada
- Allowlist de hosts Shopee
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json, asyncio
from typing import Optional, Dict, Tuple, List, Any

import httpx
import redis
from fastapi import FastAPI, Request, Query, Path, BackgroundTasks, UploadFile, File
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse
from google.cloud import storage

# ─────────── Config ───────────
DEFAULT_TIMEOUT = httpx.Timeout(5.0, connect=1.0)

# GCS
GCS_BUCKET        = os.getenv("GCS_BUCKET")
GCS_PREFIX        = os.getenv("GCS_PREFIX", "logs/")
FLUSH_MAX_ROWS    = int(os.getenv("FLUSH_MAX_ROWS", "500"))
FLUSH_MAX_SECONDS = int(os.getenv("FLUSH_MAX_SECONDS", "30"))

# Admin
ADMIN_TOKEN       = os.getenv("ADMIN_TOKEN", "12345678")

# Redis
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COUNTER_KEY          = os.getenv("UTM_COUNTER_KEY", "utm_counter")
USERDATA_TTL_SECONDS = int(os.getenv("USERDATA_TTL_SECONDS", "604800"))
USERDATA_KEY_PREFIX  = os.getenv("USERDATA_KEY_PREFIX", "ud:")

VIDEO_ID = os.getenv("VIDEO_ID", "v15")

# Shopee Affiliate
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "18314810331")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "LO3QSEG45TYP4NYQBRXLA2YYUL3ZCUPN")
SHOPEE_ENDPOINT   = "https://open-api.affiliate.shopee.com.br/graphql"

# GA4
GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID")
GA4_API_SECRET     = os.getenv("GA4_API_SECRET")
GA4_ENDPOINT = (
    f"https://www.google-analytics.com/mp/collect?measurement_id={GA4_MEASUREMENT_ID}&api_secret={GA4_API_SECRET}"
    if (GA4_MEASUREMENT_ID and GA4_API_SECRET) else None
)

# ─────────── App / Clients ───────────
app = FastAPI(title="Shopee UTM Numbered → ShortLink + GCS + Redis + GA4")
r = redis.from_url(REDIS_URL)

client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT, http2=False)

@app.on_event("shutdown")
async def _close_client():
    await client.aclose()

_gcs_client: Optional[storage.Client] = None
_bucket: Optional[storage.Bucket] = None
if GCS_BUCKET:
    _gcs_client = storage.Client()
    _bucket = _gcs_client.bucket(GCS_BUCKET)

# ─────────── Buffer CSV ───────────
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

# ─────────── Helpers ───────────
def incr_counter() -> int:
    return int(r.incr(COUNTER_KEY))

def get_client_ip(request: Request) -> str:
    xfwd = request.headers.get("x-forwarded-for", "")
    if xfwd:
        return xfwd.split(",")[0].strip()
    host = request.client.host if request.client else "0.0.0.0"
    return host.split("::ffff:")[-1]

def get_cookie_value(cookie_header: Optional[str], name: str) -> Optional[str]:
    if not cookie_header: return None
    try:
        for it in [c.strip() for c in cookie_header.split(";")]:
            if it.startswith(name + "="):
                return it.split("=", 1)[1]
    except Exception: pass
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

def replace_utm_content_only(raw_url: str, new_value: str) -> str:
    parsed = urllib.parse.urlsplit(raw_url)
    parts = parsed.query.split("&") if parsed.query else []
    found = False
    for i, part in enumerate(parts):
        if part.startswith("utm_content="):
            parts[i] = "utm_content=" + new_value
            found = True
            break
    if not found:
        parts.append("utm_content=" + new_value)
    new_query = "&".join([p for p in parts if p])
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))

# ─────────── Flush automático para GCS ───────────
def _gcs_object_name(ts: int, part: int) -> str:
    d = time.strftime("%Y-%m-%d", time.localtime(ts))
    return f"{GCS_PREFIX}date={d}/clicks_{d}_part-{part:04d}.csv"

def _flush_buffer_to_gcs(force: bool = False) -> int:
    global _buffer_rows, _last_flush_ts
    if not _bucket:
        return 0
    with _buffer_lock:
        rows = list(_buffer_rows)
        need = (force or len(rows) >= FLUSH_MAX_ROWS or (time.time() - _last_flush_ts) >= FLUSH_MAX_SECONDS)
        if not need or len(rows) == 0:
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
    return len(rows)

def _background_flush():
    while True:
        try:
            _flush_buffer_to_gcs(force=False)
        except Exception:
            pass
        time.sleep(FLUSH_MAX_SECONDS)

if _bucket:
    t = threading.Thread(target=_background_flush, daemon=True)
    t.start()

# ─────────── GA4 ───────────
def _ga4_client_id(ip: str, ua: str) -> str:
    return hashlib.sha256(f"{ip}|{ua}".encode("utf-8")).hexdigest()[:32]

async def send_ga4_event_async(name: str, ip: str, ua: str, params: Dict[str, Any], event_ts: Optional[int] = None):
    if not GA4_ENDPOINT:
        return
    payload = {
        "client_id": _ga4_client_id(ip, ua),
        "timestamp_micros": int((event_ts or int(time.time())) * 1_000_000),
        "events": [ { "name": name, "params": {**params, "engagement_time_msec": "1"} } ]
    }
    try:
        await client.post(GA4_ENDPOINT, json=payload, headers={"User-Agent": ua, "X-Forwarded-For": ip})
    except Exception:
        pass

# ─────────── Shopee: short-link ───────────
_SUBID_REGEX = re.compile(r"[^A-Za-z0-9]")
_SUBID_MAXLEN = int(os.getenv("SHOPEE_SUBID_MAXLEN", "50"))

def sanitize_subid_for_shopee(value: str) -> str:
    if not value: return "na"
    cleaned = _SUBID_REGEX.sub("", value)
    return cleaned[:_SUBID_MAXLEN] if cleaned else "na"

async def generate_short_link_async(origin_url: str, utm_content_for_api: str) -> str:
    key = f"short:{utm_content_for_api}"
    cached = r.get(key)
    if cached:
        return cached.decode()
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
        resp = await client.post(SHOPEE_ENDPOINT, headers=headers, content=payload)
        j = resp.json()
        short = (((j or {}).get("data") or {}).get("generateShortLink") or {}).get("shortLink")
        if short:
            r.setex(key, 86400, short)
            return short
    except Exception as e:
        print(f"[Shopee] erro ao gerar short: {e}")
    return origin_url

# ─────────── Rotas ───────────
@app.get("/health")
async def health():
    return {"ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET, "prefix": GCS_PREFIX, "video_id": VIDEO_ID}

@app.get("/")
async def root():
    return PlainTextResponse("OK", status_code=200)

@app.get("/{full_path:path}")
async def track_number_and_redirect(
    request: Request,
    background: BackgroundTasks,
    full_path: str = Path(...),
    cat: Optional[str] = Query(None)
):
    raw_path = urllib.parse.unquote(full_path or "").strip()
    if not raw_path or raw_path == "favicon.ico":
        return JSONResponse({"ok": False, "error": "missing_url"}, status_code=400)
    if raw_path.startswith("//"):
        raw_path = "https:" + raw_path
    if not raw_path.startswith("http"):
        return JSONResponse({"ok": False, "error": "invalid_url"}, status_code=400)

    incoming_url = raw_path
    ts = int(time.time())
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts))

    headers = request.headers
    cookie_header = headers.get("cookie") or headers.get("Cookie")
    referrer = headers.get("referer") or "-"
    user_agent = headers.get("user-agent", "-")
    fbclid = request.query_params.get("fbclid")
    ip_addr = get_client_ip(request)

    fbp_cookie = get_cookie_value(cookie_header, "_fbp")
    fbc_val = build_fbc_from_fbclid(fbclid, creation_ts=ts)

    device_name, os_family, os_version = parse_device_info(user_agent)

    utm_original = ""
    clean_base = "n"
    seq = incr_counter()
    utm_numbered = f"{clean_base}N{seq}"
    final_with_number = replace_utm_content_only(incoming_url, utm_numbered)

    sub_id_api = sanitize_subid_for_shopee(utm_numbered)
    dest = await generate_short_link_async(final_with_number, sub_id_api)

    # GA4 em background
    ga_params = {
        "link_url": dest,
        "event_source_url": final_with_number,
        "utm_original": utm_original,
        "utm_numbered": utm_numbered,
        "category": (cat or ""),
        "referrer": referrer,
        "video_id": VIDEO_ID,
        "fbclid": fbclid or "",
        "fbp": fbp_cookie or "",
        "fbc": fbc_val or "",
    }
    background.add_task(send_ga4_event_async, "view_content", ip_addr, user_agent, ga_params, ts)

    row = [
        ts, iso_time, ip_addr, user_agent, device_name, os_family, os_version, referrer,
        incoming_url, incoming_url, final_with_number, (cat or ""),
        utm_original, utm_numbered, "", "", "", "", "", fbclid or "", fbp_cookie or "", fbc_val or ""
    ]
    with _buffer_lock:
        _buffer_rows.append(row)

    return RedirectResponse(dest, status_code=302)

# Flush manual
@app.get("/admin/flush")
async def admin_flush(token: str):
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    sent = _flush_buffer_to_gcs(force=True)
    return {"ok": True, "sent_rows": sent}

# Upload CSV
@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    content = await file.read()
    text = content.decode("utf-8", errors="replace").splitlines()
    reader = csv.DictReader(text)
    processed = [row for row in reader]
    return JSONResponse({"rows": processed})

@atexit.register
def _flush_on_exit():
    try:
        _flush_buffer_to_gcs(force=True)
    except Exception:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
