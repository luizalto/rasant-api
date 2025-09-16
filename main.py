# -*- coding: utf-8 -*-
"""
Render Server – FastAPI + Redis + failover PC worker

Fluxo:
- Recebe clique
- Enfileira no Redis
- Espera resposta do PC (300ms)
- Se o PC respondeu -> redireciona para dest calculado pelo PC
- Se não respondeu -> processa localmente (igual código original)
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json, uuid
from typing import Optional, Dict, Tuple, List, Any
import requests, redis
from fastapi import FastAPI, Request, Query, Path
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse
from google.cloud import storage

# ─────────── Config ───────────
DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12"))
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY       = os.getenv("QUEUE_KEY", "q:jobs")
RES_PREFIX      = os.getenv("RES_PREFIX", "res:")
ACCEL_WAIT      = float(os.getenv("ACCEL_WAIT", "0.3"))  # segundos de espera pelo PC

# GCS
GCS_BUCKET        = os.getenv("GCS_BUCKET")
GCS_PREFIX        = os.getenv("GCS_PREFIX", "logs/")
FLUSH_MAX_ROWS    = int(os.getenv("FLUSH_MAX_ROWS", "500"))
FLUSH_MAX_SECONDS = int(os.getenv("FLUSH_MAX_SECONDS", "30"))

# Admin
ADMIN_TOKEN       = os.getenv("ADMIN_TOKEN", "12345678")

# Redis
COUNTER_KEY          = os.getenv("UTM_COUNTER_KEY", "utm_counter")
USERDATA_TTL_SECONDS = int(os.getenv("USERDATA_TTL_SECONDS", "604800"))
USERDATA_KEY_PREFIX  = os.getenv("USERDATA_KEY_PREFIX", "ud:")
VIDEO_ID             = os.getenv("VIDEO_ID", "v15")

# Shopee Affiliate
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "")
SHOPEE_ENDPOINT   = "https://open-api.affiliate.shopee.com.br/graphql"

# ─────────── App / Clients ───────────
app = FastAPI(title="Shopee ShortLink – Render")
r = redis.from_url(REDIS_URL)

_gcs_client = _bucket = None
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
            if it.startswith(name + "="): return it.split("=", 1)[1]
    except: pass
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
    if not found: parts.append("utm_content=" + new_value)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "&".join(parts), parsed.fragment))

def resolve_short_link(short_url: str, max_hops: int = 4) -> Tuple[str, Dict[str, Optional[str]]]:
    current = short_url; final_url = short_url
    try:
        session = requests.Session()
        for _ in range(max_hops):
            resp = session.get(current, allow_redirects=False, timeout=DEFAULT_TIMEOUT)
            if 300 <= resp.status_code < 400 and "Location" in resp.headers:
                current = urllib.parse.urljoin(current, resp.headers["Location"])
                final_url = current
                continue
            break
    except: final_url = short_url
    return final_url, urllib.parse.parse_qs(urllib.parse.urlsplit(final_url).query)

def sanitize_subid_for_shopee(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]","",value)[:50] or "na"

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
    signature = hashlib.sha256((SHOPEE_APP_ID+ts+payload+SHOPEE_APP_SECRET).encode()).hexdigest()
    headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID}, Timestamp={ts}, Signature={signature}","Content-Type": "application/json"}
    try:
        resp = requests.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=20)
        j = resp.json(); short = (((j or {}).get("data") or {}).get("generateShortLink") or {}).get("shortLink")
        if not short: raise ValueError(f"Resposta Shopee sem shortLink: {j}")
        return short
    except Exception as e:
        print("[ShopeeShortLink] ERRO:", e); return origin_url

# ─────────── Rotas ───────────
@app.get("/health")
def health(): return {"ok":True,"ts":int(time.time())}

@app.get("/{full_path:path}")
def track(request: Request, full_path: str = Path(...), cat: Optional[str] = Query(None)):
    if not full_path or full_path=="favicon.ico": return JSONResponse({"ok":False,"error":"missing_url"},400)
    if full_path.startswith("//"): full_path="https:"+full_path
    incoming_url = urllib.parse.unquote(full_path)

    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "url": incoming_url,
        "cat": cat,
        "headers": dict(request.headers),
        "query": dict(request.query_params),
        "ip": request.client.host if request.client else ""
    }
    # envia job para fila
    r.lpush(QUEUE_KEY, json.dumps(job))

    # espera pelo PC
    deadline = time.time()+ACCEL_WAIT
    res_key = RES_PREFIX+job_id
    while time.time()<deadline:
        res = r.get(res_key)
        if res:
            r.delete(res_key)
            return RedirectResponse(json.loads(res)["dest"],302)
        time.sleep(0.01)

    # failover local
    headers=job["headers"]; user_agent=headers.get("user-agent","-")
    device,osfam,osver = parse_device_info(user_agent)
    ts=int(time.time()); iso=time.strftime("%Y-%m-%dT%H:%M:%S%z",time.localtime(ts))
    resolved,qs = resolve_short_link(incoming_url)
    utm = (qs.get("utm_content") or [""])[0]
    seq = incr_counter()
    utm_num = f"{re.sub(r'[^A-Za-z0-9]','',utm) or 'n'}N{seq}"
    final_url = replace_utm_content_only(resolved, utm_num)
    dest = generate_short_link(final_url, sanitize_subid_for_shopee(utm_num))

    # grava redis + gcs
    r.setex(f"{USERDATA_KEY_PREFIX}{utm_num}",USERDATA_TTL_SECONDS,json.dumps({"ip":job["ip"],"ua":user_agent,"final":final_url,"short":dest}))
    with _buffer_lock: _buffer_rows.append([ts,iso,job["ip"],user_agent,device,osfam,osver,headers.get("referer","-"),incoming_url,resolved,final_url,cat or "",utm,utm_num,"","","","","","",""])
    return RedirectResponse(dest,302)
