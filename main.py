# -*- coding: utf-8 -*-
"""
FastAPI – Shopee ShortLink otimizado + Redis + GCS (flush background)

Melhorias:
- Sessão HTTP global (menos latência)
- Resolver shortlink com 1 hop
- Incremento UTM imediato
- Geração Shopee Shortlink com timeout otimizado
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json
from typing import Optional, Dict, Tuple, List, Any
import requests
import redis
from fastapi import FastAPI, Request, Query, Path
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse
from google.cloud import storage

# ─────────── Config ───────────
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12"))

# GCS
GCS_BUCKET        = os.getenv("GCS_BUCKET")
GCS_PREFIX        = os.getenv("GCS_PREFIX", "logs/")
FLUSH_MAX_ROWS    = int(os.getenv("FLUSH_MAX_ROWS", "500"))
FLUSH_MAX_SECONDS = int(os.getenv("FLUSH_MAX_SECONDS", "30"))

# Redis
REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COUNTER_KEY = os.getenv("UTM_COUNTER_KEY", "utm_counter")
r = redis.from_url(REDIS_URL)

USERDATA_TTL_SECONDS = int(os.getenv("USERDATA_TTL_SECONDS", "604800"))
USERDATA_KEY_PREFIX  = os.getenv("USERDATA_KEY_PREFIX", "ud:")

VIDEO_ID = os.getenv("VIDEO_ID", "v15")

# Shopee
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "18314810331")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "LO3QSEG45TYP4NYQBRXLA2YYUL3ZCUPN")
SHOPEE_ENDPOINT   = "https://open-api.affiliate.shopee.com.br/graphql"

# Sessão HTTP global
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50)
session.mount("http://", adapter)
session.mount("https://", adapter)

# ─────────── App ───────────
app = FastAPI(title="Shopee UTM Numbered Optimized")

# ─────────── Buffer CSV ───────────
_buffer_lock = threading.Lock()
_buffer_rows: List[List[str]] = []
_last_flush_ts = time.time()

CSV_HEADERS = [
    "timestamp","iso_time","ip","user_agent","device_name","os_family","os_version","referrer",
    "short_link_in","resolved_url","final_url","category",
    "utm_original","utm_numbered",
    "fbclid","fbp","fbc"
]

# ─────────── Helpers ───────────
def incr_counter() -> int:
    return int(r.incr(COUNTER_KEY))

def parse_device_info(ua: str):
    ua = ua or "-"
    ua_l = ua.lower()
    os_family, os_version = "Other", "-"
    if "iphone" in ua_l: return "iPhone","iOS","-"
    if "android" in ua_l:
        m = re.search(r"Android\s(\d+)", ua, re.I)
        return "Android","Android", (m.group(1) if m else "-")
    if "windows" in ua: return "Desktop","Windows","-"
    if "mac" in ua: return "Desktop","macOS","-"
    return "Desktop", os_family, os_version

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
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "&".join(parts), parsed.fragment))

def resolve_short_link(short_url: str, max_hops: int = 1) -> str:
    """Resolve no máximo 1 hop para não atrasar clique"""
    current = short_url
    try:
        resp = session.get(current, allow_redirects=False, timeout=DEFAULT_TIMEOUT)
        if 300 <= resp.status_code < 400 and "Location" in resp.headers:
            return urllib.parse.urljoin(current, resp.headers["Location"])
    except Exception:
        pass
    return current

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
        if short: return short
    except Exception as e:
        print("[ShopeeShortLink] ERRO:", e)
    return origin_url

# ─────────── GCS flush ───────────
_gcs_client = storage.Client() if GCS_BUCKET else None
_bucket = _gcs_client.bucket(GCS_BUCKET) if _gcs_client else None

def _flush_buffer_to_gcs() -> int:
    global _buffer_rows, _last_flush_ts
    if not _bucket: return 0
    with _buffer_lock:
        rows = list(_buffer_rows)
        if len(rows) == 0: return 0
        _buffer_rows = []
        _last_flush_ts = time.time()
    output = io.StringIO()
    w = csv.writer(output)
    w.writerow(CSV_HEADERS)
    w.writerows(rows)
    data = output.getvalue().encode("utf-8")
    part = int(time.time() * 1000) % 10_000_000
    blob_name = f"{GCS_PREFIX}date={time.strftime('%Y-%m-%d')}/clicks_part-{part:04d}.csv"
    _bucket.blob(blob_name).upload_from_string(data, content_type="text/csv")
    print(f"[FLUSH] {len(rows)} linhas → gs://{GCS_BUCKET}/{blob_name}")
    return len(rows)

def _background_flusher():
    while True:
        try: _flush_buffer_to_gcs()
        except Exception as e: print("[FLUSH-ERR]", e)
        time.sleep(FLUSH_MAX_SECONDS)

threading.Thread(target=_background_flusher, daemon=True).start()

# ─────────── Rotas ───────────
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET}

@app.get("/{full_path:path}")
def track_and_redirect(request: Request, full_path: str = Path(...), cat: Optional[str] = Query(None)):
    raw_path = urllib.parse.unquote(full_path or "").strip()
    if not raw_path or raw_path == "favicon.ico":
        return JSONResponse({"ok": False}, status_code=400)
    if raw_path.startswith("//"): raw_path = "https:" + raw_path

    # Incremento imediato
    seq = incr_counter()
    utm_original = "utm"  # você pode extrair se precisar
    utm_numbered = f"{utm_original}N{seq}"

    # Resolve e gera shortlink
    resolved = resolve_short_link(raw_path)
    final_url = replace_utm_content_only(resolved, utm_numbered)
    dest = generate_short_link(final_url, utm_numbered)

    # Dados de contexto
    ts = int(time.time())
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts))
    ip_addr = request.client.host
    ua = request.headers.get("user-agent","-")
    device_name, os_family, os_version = parse_device_info(ua)

    row = [ts, iso_time, ip_addr, ua, device_name, os_family, os_version,
           request.headers.get("referer","-"), raw_path, resolved, final_url,
           (cat or ""), utm_original, utm_numbered,
           request.query_params.get("fbclid",""), "-", "-"]
    with _buffer_lock: _buffer_rows.append(row)

    return RedirectResponse(dest, status_code=302)

@atexit.register
def _flush_on_exit():
    try: _flush_buffer_to_gcs()
    except: pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT","10000")), reload=False)
