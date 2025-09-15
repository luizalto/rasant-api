# -*- coding: utf-8 -*-
"""
FastAPI – Shopee ShortLink com UTM numerada (somente letras e números) + Logs em GCS + Redis

Processo:
- Recebe short/URL Shopee com UTM → resolve para URL longa
- Extrai utm_content ORIGINAL → remove símbolos (fica só [A-Za-z0-9]) → monta <base>N<sequência>
- Substitui SOMENTE utm_content na URL longa
- Gera short-link oficial (Shopee GraphQL) usando subIds[2] = UTM numerada (já alfanumérica)
- Salva cache/CSV e redireciona 302 para o short oficial
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json
from typing import Optional, Dict, Tuple, List, Any

import requests
import redis
from fastapi import FastAPI, Request, Query, Path, UploadFile, File
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse
from google.cloud import storage

# ─────────── Config ───────────
DEFAULT_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "12"))

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

# ─────────── App / Clients ───────────
app = FastAPI(title="Shopee UTM Numbered → ShortLink + GCS + Redis")
r = redis.from_url(REDIS_URL)

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

def resolve_short_link(short_url: str, max_hops: int = 6) -> Tuple[str, Dict[str, Optional[str]]]:
    current = short_url
    final_url = short_url
    try:
        session = requests.Session()
        for _ in range(max_hops):
            resp = session.get(current, allow_redirects=False, timeout=DEFAULT_TIMEOUT, headers={
                "User-Agent": "Mozilla/5.0 (resolver/1.0)"
            })
            if 300 <= resp.status_code < 400 and "Location" in resp.headers:
                location = resp.headers["Location"]
                current = urllib.parse.urljoin(current, location)
                final_url = current
                continue
            break
    except Exception:
        final_url = short_url
    parsed = urllib.parse.urlsplit(final_url)
    subids = parse_subids_from_query(parsed.query)
    return final_url, subids

# ─────────── GCS flush ───────────
def _day_key(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts))

def _gcs_object_name(ts: int, part: int) -> str:
    d = _day_key(ts)
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

# ─────────── Shopee short-link ───────────
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
        resp = requests.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=20)
        j = resp.json()
        short = (((j or {}).get("data") or {}).get("generateShortLink") or {}).get("shortLink")
        if not short:
            raise ValueError(f"Resposta Shopee sem shortLink: {j}")
        return short
    except Exception as e:
        print(f"[ShopeeShortLink] ERRO ao gerar shortlink: {e}. Fallback para URL numerada.")
        return origin_url

# ─────────── Rotas ───────────
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET, "prefix": GCS_PREFIX, "video_id": VIDEO_ID}

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
    ts = int(time.time())
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts))

    headers = request.headers
    cookie_header = headers.get("cookie") or headers.get("Cookie")
    referrer = headers.get("referer") or "-"
    user_agent = headers.get("user-agent", "-")
    fbclid = request.query_params.get("fbclid")

    client_host = request.client.host if request.client else None
    if client_host and client_host.startswith("::ffff:"):
        client_host = client_host.split("::ffff:")[-1]
    ip_addr = client_host or headers.get("x-forwarded-for") or "0.0.0.0"

    fbp_cookie = get_cookie_value(cookie_header, "_fbp")
    fbc_val    = build_fbc_from_fbclid(fbclid, creation_ts=ts)

    device_name, os_family, os_version = parse_device_info(user_agent)

    resolved_url, subids_in = resolve_short_link(incoming_url)
    utm_original = subids_in.get("utm_content") or ""
    clean_base = re.sub(r'[^A-Za-z0-9]', '', utm_original or "") or "n"
    seq = incr_counter()
    utm_numbered = f"{clean_base}N{seq}"
    final_with_number = replace_utm_content_only(resolved_url, utm_numbered)

    sub_id_api = sanitize_subid_for_shopee(utm_numbered)
    dest = generate_short_link(final_with_number, sub_id_api)

    r.setex(f"{USERDATA_KEY_PREFIX}{utm_numbered}", USERDATA_TTL_SECONDS, json.dumps({
        "ip": ip_addr, "ua": user_agent, "referrer": referrer,
        "event_source_url": final_with_number, "short_link": dest,
        "vc_time": ts, "utm_original": utm_original, "utm_numbered": utm_numbered,
        "fbclid": fbclid, "fbp": fbp_cookie, "fbc": fbc_val
    }))

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
    _flush_buffer_to_gcs(force=False)

    return RedirectResponse(dest, status_code=302)

@app.get("/admin/flush")
def admin_flush(token: str):
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    sent = _flush_buffer_to_gcs(force=True)
    return {"ok": True, "sent_rows": sent}

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
        _flush_buffer_to_gcs(force=True)
    except Exception:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
