# -*- coding: utf-8 -*-
"""
FastAPI - Logger de cliques → CSV no Google Cloud Storage (GCS)

Uso SEM codificar o short-link:
  https://SEU-APP.onrender.com/https://s.shopee.com.br/XXXX?cat=beleza

Env vars (Render):
  GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/service_account.json  (Secret File)
  ou GOOGLE_APPLICATION_CREDENTIALS_JSON=<conteúdo do JSON>         (alternativa)
  GCS_BUCKET=utm-click-logs
  (opcionais) GCS_PREFIX=logs/ | FLUSH_MAX_ROWS=500 | FLUSH_MAX_SECONDS=30
  (opcionais) HTTP_TIMEOUT=12 | ADMIN_TOKEN=troque_isto
"""

import os, re, time, csv, io, threading, urllib.parse, atexit
from typing import Optional, Dict, Tuple, List

import requests
from fastapi import FastAPI, Request, Query, Path
from fastapi.responses import RedirectResponse, JSONResponse

# ─────────────────────────── Credenciais GCP ────────────────────────────────
def _bootstrap_gcp_credentials():
    """
    1) Se GOOGLE_APPLICATION_CREDENTIALS estiver setado (ex.: /etc/secrets/service_account.json) → ok.
    2) Senão, se GOOGLE_APPLICATION_CREDENTIALS_JSON existir → grava em /tmp/gcp.json e seta a env.
    """
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    raw_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if raw_json:
        path = "/tmp/gcp.json"
        with open(path, "w", encoding="utf-8") as f:
            f.write(raw_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path

_bootstrap_gcp_credentials()
from google.cloud import storage  # importar após preparar as credenciais

# ───────────────────────────── Configuração ─────────────────────────────────
DEFAULT_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "12"))
GCS_BUCKET        = os.getenv("GCS_BUCKET")                    # obrigatório
GCS_PREFIX        = os.getenv("GCS_PREFIX", "logs/")           # opcional
FLUSH_MAX_ROWS    = int(os.getenv("FLUSH_MAX_ROWS", "500"))
FLUSH_MAX_SECONDS = int(os.getenv("FLUSH_MAX_SECONDS", "30"))
ADMIN_TOKEN       = os.getenv("ADMIN_TOKEN", "12345678")

# ───────────────────────────── App / GCS ────────────────────────────────────
app = FastAPI(title="Click Logger → GCS (CSV em lotes)")

_gcs_client: Optional[storage.Client] = None
_bucket: Optional[storage.Bucket] = None
if GCS_BUCKET:
    _gcs_client = storage.Client()
    _bucket = _gcs_client.bucket(GCS_BUCKET)

# ───────────────────────── Buffer em memória (thread-safe) ──────────────────
_buffer_lock = threading.Lock()
_buffer_rows: List[List[str]] = []
_last_flush_ts = time.time()

CSV_HEADERS = [
    "timestamp","iso_time","ip","user_agent","device_name","os_family","os_version","referrer",
    "short_link","final_url","category",
    "utm_content","sub_id1","sub_id2","sub_id3","sub_id4","sub_id5",
    "fbclid","fbp","fbc"
]

# ───────────────────────────── Helpers ──────────────────────────────────────
def parse_subids_from_query(qs: str) -> Dict[str, Optional[str]]:
    out = {"utm_content": None, "sub_id1": None, "sub_id2": None, "sub_id3": None, "sub_id4": None, "sub_id5": None}
    if not qs: return out
    q = urllib.parse.parse_qs(qs, keep_blank_values=True)

    if "utm_content" in q and q["utm_content"]:
        out["utm_content"] = q["utm_content"][0]

    # subIds[indexados]
    for i in range(5):
        key_idx = f"subIds[{i}]"
        if key_idx in q and q[key_idx]:
            out[f"sub_id{i+1}"] = q[key_idx][0]

    # múltiplos subIds sem índice
    if "subIds" in q:
        vals = q["subIds"]
        for i in range(min(5, len(vals))):
            out[f"sub_id{i+1}"] = out[f"sub_id{i+1}"] or vals[i]

    # apelidos comuns
    aliases = {
        "sub_id1":"sub_id1","subid1":"sub_id1","Sub_id1":"sub_id1",
        "sub_id2":"sub_id2","subid2":"sub_id2","Sub_id2":"sub_id2",
        "sub_id3":"sub_id3","subid3":"sub_id3","Sub_id3":"sub_id3",
        "sub_id4":"sub_id4","subid4":"sub_id4","Sub_id4":"sub_id4",
        "sub_id5":"sub_id5","subid5":"sub_id5","Sub_id5":"sub_id5",
    }
    for src, dst in aliases.items():
        if src in q and q[src] and not out[dst]:
            out[dst] = q[src][0]

    if not out["utm_content"] and out["sub_id3"]:
        out["utm_content"] = out["sub_id3"]
    return out

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

def _day_key(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts))

def _gcs_object_name(ts: int, part: int) -> str:
    d = _day_key(ts)
    return f"{GCS_PREFIX}date={d}/clicks_{d}_part-{part:04d}.csv"

def _flush_buffer_to_gcs(force: bool = False) -> int:
    """Envia o buffer ao GCS se bater limite de linhas/tempo ou se force=True."""
    global _buffer_rows, _last_flush_ts
    if not _bucket:
        print("[WARN] GCS_BUCKET não configurado; pulando flush.")
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

    print(f"[FLUSH] {len(rows)} linha(s) → gs://{GCS_BUCKET}/{blob_name}")
    return len(rows)

# ───────────────────────────── Rotas ────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET, "prefix": GCS_PREFIX}

@app.get("/{full_path:path}")
def track_and_redirect(
    request: Request,
    full_path: str = Path(..., description="Short-link direto, ex.: https://s.shopee.com.br/XXXX"),
    cat: Optional[str] = Query(None, description="Categoria opcional p/ organização"),
):
    """
    Recebe o short-link DIRETO no path (sem codificar), ex.:
      https://seu-app.onrender.com/https://s.shopee.com.br/XXXX?cat=beleza
    Resolve, extrai UTM/SubIDs, loga e redireciona.
    """
    s = full_path  # short-link direto informado

    ts = int(time.time())
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts))

    headers = request.headers
    cookie_header = headers.get("cookie") or headers.get("Cookie")
    referrer = headers.get("referer") or headers.get("referrer") or "-"
    user_agent = headers.get("user-agent", "-")
    fbclid = request.query_params.get("fbclid")

    # IP
    client_host = request.client.host if request.client else None
    if client_host and client_host.startswith("::ffff:"):
        client_host = client_host.split("::ffff:")[-1]
    ip_addr = client_host or headers.get("x-forwarded-for") or "0.0.0.0"

    # FB cookies
    fbp_cookie = get_cookie_value(cookie_header, "_fbp")
    fbc_val    = build_fbc_from_fbclid(fbclid, creation_ts=ts)

    # Device
    device_name, os_family, os_version = parse_device_info(user_agent)

    # Resolve short-link e extrai UTM/SubIDs
    final_url, subids = resolve_short_link(s)
    utm_content = subids.get("utm_content")
    sub_id1, sub_id2, sub_id3 = subids.get("sub_id1"), subids.get("sub_id2"), subids.get("sub_id3")
    sub_id4, sub_id5 = subids.get("sub_id4"), subids.get("sub_id5")

    # Nova linha (cada clique = 1 linha; sem deduplicação)
    csv_row = [
        ts, iso_time, ip_addr, user_agent, device_name, os_family, os_version, referrer,
        s, final_url, (cat or ""),
        (utm_content or ""), (sub_id1 or ""), (sub_id2 or ""), (sub_id3 or ""), (sub_id4 or ""), (sub_id5 or ""),
        (fbclid or ""), (fbp_cookie or ""), (fbc_val or "")
    ]

    # 🔎 Printar nos logs do servidor (Render → Logs)
    print("Novo clique registrado:", {
        "timestamp": ts,
        "iso_time": iso_time,
        "ip": ip_addr,
        "user_agent": user_agent,
        "device_name": device_name,
        "os_family": os_family,
        "os_version": os_version,
        "referrer": referrer,
        "short_link": s,
        "final_url": final_url,
        "category": cat or "",
        "utm_content": utm_content or "",
        "sub_id1": sub_id1 or "",
        "sub_id2": sub_id2 or "",
        "sub_id3": sub_id3 or "",
        "sub_id4": sub_id4 or "",
        "sub_id5": sub_id5 or "",
        "fbclid": fbclid or "",
        "fbp": fbp_cookie or "",
        "fbc": fbc_val or ""
    })

    with _buffer_lock:
        _buffer_rows.append(csv_row)
    _flush_buffer_to_gcs(force=False)

    return RedirectResponse(s, status_code=302)

@app.get("/admin/flush")
def admin_flush(token: str):
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    sent = _flush_buffer_to_gcs(force=True)
    return {"ok": True, "sent_rows": sent}

# Flush final ao encerrar (melhor esforço)
@atexit.register
def _flush_on_exit():
    try:
        _flush_buffer_to_gcs(force=True)
    except Exception:
        pass
