# -*- coding: utf-8 -*-
"""
FastAPI - Logger de cliques → CSV no Google Cloud Storage (GCS)
+ Workaround para abrir o APP da Shopee (Instagram/Facebook in-app browser)

Uso SEM codificar o short-link:
  https://SEU-APP.onrender.com/https://s.shopee.com.br/XXXX?cat=beleza

Env vars:
  GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/service_account.json  (ou use GOOGLE_APPLICATION_CREDENTIALS_JSON)
  GCS_BUCKET=utm-click-logs
  (opcionais) GCS_PREFIX=logs/ | FLUSH_MAX_ROWS=500 | FLUSH_MAX_SECONDS=30 | HTTP_TIMEOUT=12 | ADMIN_TOKEN=12345678

Start: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, html
from typing import Optional, Dict, Tuple, List

import requests
from fastapi import FastAPI, Request, Query, Path
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse

# ─────────────────────────── Credenciais GCP ────────────────────────────────
def _bootstrap_gcp_credentials():
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

# Pacotes possíveis do app da Shopee (Android). Ajuste se necessário.
SHOPEE_ANDROID_PACKAGES = ["com.shopee.br", "com.shopee.app"]

# Links da loja (fallback)
PLAY_STORE_URL = "https://play.google.com/store/apps/details?id=com.shopee.br"
APP_STORE_URL  = "https://apps.apple.com/app/id959841449"  # Shopee: pode variar por região

# ───────────────────────────── App / GCS ────────────────────────────────────
app = FastAPI(title="Click Logger → GCS (CSV em lotes) + App Open Workaround")

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

    for i in range(5):
        key_idx = f"subIds[{i}]"
        if key_idx in q and q[key_idx]:
            out[f"sub_id{i+1}"] = q[key_idx][0]

    if "subIds" in q:
        vals = q["subIds"]
        for i in range(min(5, len(vals))):
            out[f"sub_id{i+1}"] = out[f"sub_id{i+1}"] or vals[i]

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

def _is_in_app_browser(ua: str) -> bool:
    ua = (ua or "").lower()
    return ("instagram" in ua) or ("fbav" in ua) or ("fban" in ua) or ("facebook" in ua)

def _app_open_html(final_url: str, is_android: bool) -> str:
    # Tenta abrir o APP; se não, cai para loja; oferece botões.
    safe_final = html.escape(final_url, quote=True)
    encoded_final = urllib.parse.quote(final_url, safe="")

    # Deeplink genérico Shopee (muitos apps resolvem sem path):
    shopee_deeplink = "shopee://open"

    # Android Intent candidates
    android_intents = [
        f"intent://open#Intent;scheme=shopee;package={pkg};S.browser_fallback_url={urllib.parse.quote(PLAY_STORE_URL, safe='')};end"
        for pkg in SHOPEE_ANDROID_PACKAGES
    ]

    return f"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Abrindo no app…</title>
<style>
  body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding: 24px; }}
  .box {{ max-width: 680px; margin: 0 auto; }}
  .btn {{ display:inline-block; padding:14px 18px; border-radius:10px; text-decoration:none; margin:8px 0; border:1px solid #ccc; }}
  .primary {{ background:#ff5722; color:#fff; border-color:#ff5722; }}
  .muted {{ color:#333; }}
  .small {{ color:#666; font-size:12px; }}
</style>
</head>
<body>
<div class="box">
  <h2>Queremos abrir no app da Shopee</h2>
  <p class="muted">Se não abrir automaticamente em alguns segundos, toque em “Abrir no app”.</p>

  <div style="margin:18px 0;">
    <a class="btn primary" id="openAppBtn" href="{shopee_deeplink}">Abrir no app</a><br/>
    <a class="btn" id="openExternalBtn" target="_blank" rel="noopener" href="{safe_final}">Abrir no navegador externo</a>
  </div>

  <p class="small">Se nada acontecer, instale/atualize o app:</p>
  <div>
    <a class="btn" target="_blank" rel="noopener" href="{PLAY_STORE_URL}">Google Play</a>
    <a class="btn" target="_blank" rel="noopener" href="{APP_STORE_URL}">App Store</a>
  </div>

  <p class="small">Link do produto: <br/><code>{safe_final}</code></p>
</div>

<script>
(function() {{
  var isAndroid = {str(is_android).lower()};
  var finalUrl = "{safe_final}";
  var deepLink = "{shopee_deeplink}";
  var androidIntents = {android_intents};

  function tryOpen() {{
    try {{
      if (isAndroid && androidIntents.length) {{
        // Tenta Intent com pacote específico (Instagram geralmente respeita)
        window.location.href = androidIntents[0];
        setTimeout(function() {{
          if (androidIntents[1]) window.location.href = androidIntents[1];
        }}, 800);
        // Fallback para deeplink genérico
        setTimeout(function() {{ window.location.href = deepLink; }}, 1600);
      }} else {{
        // iOS: deeplink; se falhar, Safari abrirá o link e o usuário pode tocar "Abrir no app"
        window.location.href = deepLink;
      }}
      // Último fallback: abrir o link final
      setTimeout(function() {{ window.location.href = finalUrl; }}, 2400);
    }} catch (e) {{
      // Fallback final
      window.location.href = finalUrl;
    }}
  }}

  // Dispara automaticamente após pequeno delay (evita alguns bloqueios)
  setTimeout(tryOpen, 350);

  // Botões
  document.getElementById('openAppBtn').addEventListener('click', function(e) {{
    e.preventDefault(); tryOpen();
  }});
}})();
</script>
</body>
</html>"""
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
    Recebe o short-link DIRETO no path. Loga clique → GCS.
    Se detectar Instagram/Facebook webview, serve HTML que tenta abrir o app via intent/deeplink.
    Caso contrário, faz 302 normal para o short-link.
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

    # Device / OS
    device_name, os_family, os_version = parse_device_info(user_agent)
    is_android = (os_family.lower() == "android")
    is_in_app  = _is_in_app_browser(user_agent)

    # Resolve short-link → extrai UTM/SubIDs p/ log
    final_url, subids = resolve_short_link(s)
    utm_content = subids.get("utm_content")
    sub_id1, sub_id2, sub_id3 = subids.get("sub_id1"), subids.get("sub_id2"), subids.get("sub_id3")
    sub_id4, sub_id5 = subids.get("sub_id4"), subids.get("sub_id5")

    # Log (cada clique = 1 linha)
    csv_row = [
        ts, iso_time, ip_addr, user_agent, device_name, os_family, os_version, referrer,
        s, final_url, (cat or ""),
        (utm_content or ""), (sub_id1 or ""), (sub_id2 or ""), (sub_id3 or ""), (sub_id4 or ""), (sub_id5 or ""),
        (fbclid or ""), (fbp_cookie or ""), (fbc_val or "")
    ]
    with _buffer_lock:
        _buffer_rows.append(csv_row)
    _flush_buffer_to_gcs(force=False)

    # 1) Se for webview do Instagram/Facebook → devolve HTML que tenta abrir o APP
    if is_in_app:
        html_doc = _app_open_html(final_url, is_android=is_android)
        return HTMLResponse(content=html_doc, status_code=200)

    # 2) Fora do Instagram/FB → 302 normal
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
