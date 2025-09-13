# main.py
# -*- coding: utf-8 -*-

import os
import re
import csv
import io
import json
import time
import uuid
import hashlib
import urllib.parse
import threading
import sqlite3
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List
from contextlib import suppress

import requests
import redis
from fastapi import FastAPI, Request, Path, Query
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse, Response

# =============================================================================
#                              CONFIG / ENVs
# =============================================================================
# 1) Redis (aceita vários formatos de vars)
REDIS_URL       = os.getenv("REDIS_URL", "").strip() or os.getenv("UPSTASH_REDIS_URL", "").strip()
REDIS_HOST      = os.getenv("REDIS_HOST", "").strip()
REDIS_PORT      = os.getenv("REDIS_PORT", "6379").strip()
REDIS_PASSWORD  = os.getenv("REDIS_PASSWORD", "").strip()
REDIS_TLS       = os.getenv("REDIS_TLS", "true").lower() in ("1", "true", "yes", "on")

UTM_COUNTER_KEY = os.getenv("UTM_COUNTER_KEY", "utm_counter_global")
REDIS_CONNECT_TIMEOUT = float(os.getenv("REDIS_CONNECT_TIMEOUT", "2.0"))
REDIS_SOCKET_TIMEOUT  = float(os.getenv("REDIS_SOCKET_TIMEOUT", "2.0"))
REDIS_RETRIES         = int(os.getenv("REDIS_RETRIES", "2"))

# 2) Shopee Affiliate (shortlink opcional)
SHOPEE_APP_ID          = os.getenv("SHOPEE_APP_ID", "").strip()
SHOPEE_APP_SECRET      = os.getenv("SHOPEE_APP_SECRET", "").strip()
SHOPEE_ENDPOINT        = os.getenv("SHOPEE_ENDPOINT", "https://open-api.affiliate.shopee.com.br/graphql").strip()
SHOPEE_SHORTLINK_ENABLED = os.getenv("SHOPEE_SHORTLINK_ENABLED", "true").lower() in ("1", "true", "yes", "on")

# 3) GCS (logs CSV) — aceita ambos nomes
GCS_BUCKET_NAME        = (os.getenv("GCS_BUCKET_NAME", "").strip()
                          or os.getenv("GCS_BUCKET", "").strip())
GCS_PREFIX             = os.getenv("GCS_PREFIX", "logs").strip()
# Caminho do JSON da Service Account (opcional). Se não setar, usa ADC padrão do ambiente.
GCS_SA_KEY_FILE        = os.getenv("GCS_SA_KEY_FILE", "").strip() or os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

# 4) Flush (aceita ambos nomes)
FLUSH_EVERY_N          = int(os.getenv("FLUSH_EVERY_N", os.getenv("FLUSH_MAX_ROWS", "50")))
FLUSH_EVERY_SECONDS    = int(os.getenv("FLUSH_EVERY_SECONDS", "60"))

# 5) Expansão do shortlink
EXPAND_TIMEOUT_SEC     = float(os.getenv("EXPAND_TIMEOUT_SEC", "6.0"))

# =============================================================================
#                              ESTADO GLOBAL
# =============================================================================
# Redis (lazy + fallback) — NÃO use 'r =' global
_redis_client: Optional[redis.Redis] = None
_redis_lock = threading.Lock()

# Fallback em memória para contador
_mem_lock = threading.Lock()
_mem_counter = 0

# Fallback SQLite (persiste enquanto o container viver)
COUNTER_DB_PATH = os.getenv("COUNTER_DB_PATH", "/tmp/utm_counter.db")

def _sqlite_init():
    os.makedirs(os.path.dirname(COUNTER_DB_PATH), exist_ok=True)
    with sqlite3.connect(COUNTER_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS counters(
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        conn.execute("INSERT OR IGNORE INTO counters(name, value) VALUES(?, 0)", (UTM_COUNTER_KEY,))
        conn.commit()

_sqlite_init()

def _sqlite_incr(name: str) -> int:
    with sqlite3.connect(COUNTER_DB_PATH) as conn:
        cur = conn.execute("UPDATE counters SET value = value + 1 WHERE name = ?", (name,))
        if cur.rowcount == 0:
            conn.execute("INSERT INTO counters(name, value) VALUES(?, 1)", (name,))
            val = 1
        else:
            val = conn.execute("SELECT value FROM counters WHERE name = ?", (name,)).fetchone()[0]
        conn.commit()
        return int(val)

def _build_redis_url() -> str:
    if REDIS_URL:
        return REDIS_URL
    if REDIS_HOST:
        scheme = "rediss" if REDIS_TLS else "redis"
        auth = f"default:{REDIS_PASSWORD}@" if REDIS_PASSWORD else ""
        return f"{scheme}://{auth}{REDIS_HOST}:{REDIS_PORT}"
    return ""

def _get_redis() -> Optional[redis.Redis]:
    """Cria cliente Redis (suporta rediss://) com retries. Retorna None se indisponível."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    with _redis_lock:
        if _redis_client is not None:
            return _redis_client

        url = _build_redis_url()
        if not url:
            return None  # sem config → fallback

        try:
            use_ssl = url.startswith("rediss://")
            _redis_client = redis.Redis.from_url(
                url,
                decode_responses=True,
                ssl=use_ssl,
                socket_connect_timeout=REDIS_CONNECT_TIMEOUT,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                health_check_interval=30,
            )
            ok = False
            for _ in range(REDIS_RETRIES + 1):
                try:
                    _redis_client.ping()
                    ok = True
                    break
                except Exception:
                    time.sleep(0.25)
            if not ok:
                _redis_client = None
        except Exception:
            _redis_client = None

        return _redis_client

def _incr_counter() -> int:
    """Tenta Redis; se falhar, usa SQLite; por fim, memória."""
    rcli = _get_redis()
    if rcli is not None:
        with suppress(Exception):
            return int(rcli.incr(UTM_COUNTER_KEY))

    with suppress(Exception):
        return _sqlite_incr(UTM_COUNTER_KEY)

    global _mem_counter
    with _mem_lock:
        _mem_counter += 1
        return _mem_counter

# Buffer na RAM para CSV (uma lista de dicts)
_click_buffer: List[Dict[str, Any]] = []
_last_flush_ts = time.time()

# Cliente GCS lazy (evita falha no import quando não configurado)
_gcs_client = None
_gcs_bucket = None

def _get_gcs_bucket():
    global _gcs_client, _gcs_bucket
    if not GCS_BUCKET_NAME:
        return None
    if _gcs_bucket is None:
        try:
            from google.cloud import storage
        except Exception:
            return None
        if GCS_SA_KEY_FILE:
            _gcs_client = storage.Client.from_service_account_json(GCS_SA_KEY_FILE)
        else:
            _gcs_client = storage.Client()
        _gcs_bucket = _gcs_client.bucket(GCS_BUCKET_NAME)
    return _gcs_bucket

# =============================================================================
#                              UTILITÁRIOS
# =============================================================================
def _now_ts() -> int:
    return int(time.time())

def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")

def _date_str(ts: Optional[int] = None) -> str:
    dt = datetime.utcfromtimestamp(ts or _now_ts())
    return dt.strftime("%Y-%m-%d")

def _safe_get_ip(req: Request) -> str:
    xfwd = req.headers.get("x-forwarded-for")
    if xfwd:
        ip = xfwd.split(",")[0].strip()
    else:
        ip = req.client.host if req.client else "0.0.0.0"
    if ip.startswith("::ffff:"):
        ip = ip.split("::ffff:")[-1]
    return ip

def _parse_device_os(ua: str) -> Tuple[str, str, str]:
    if not ua:
        return ("Desktop", "Other", "-")
    dev = "Desktop"
    if "Mobile" in ua or "Android" in ua or "iPhone" in ua:
        dev = "Mobile"
    if "iPad" in ua or "Tablet" in ua:
        dev = "Tablet"
    if "Android" in ua:
        m = re.search(r"Android\s+([0-9]+(?:\.[0-9]+)*)", ua)
        return (dev, "Android", m.group(1) if m else "-")
    if "iPhone" in ua or "iPad" in ua or "CPU iPhone OS" in ua:
        m = re.search(r"OS\s+([0-9_]+)", ua)
        ver = (m.group(1).replace("_", ".") if m else "-")
        return (dev, "iOS", ver)
    if "Windows" in ua:
        m = re.search(r"Windows NT\s+([0-9.]+)", ua)
        return (dev, "Windows", m.group(1) if m else "-")
    if "Mac OS X" in ua:
        m = re.search(r"OS X\s+([0-9_]+)", ua)
        return (dev, "macOS", m.group(1).replace("_", ".") if m else "-")
    if "Linux" in ua:
        return (dev, "Linux", "-")
    return (dev, "Other", "-")

def _expand_shortlink(url: str) -> str:
    """Segue redirects do shortlink para obter a URL final de produto."""
    try:
        resp = requests.get(
            url,
            allow_redirects=True,
            timeout=EXPAND_TIMEOUT_SEC,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        return resp.url or url
    except Exception:
        return url

def _replace_or_append_utm_content_preserving_order(original_url: str, utm_value: str) -> str:
    """
    Modifica APENAS o valor de utm_content preservando a ordem dos demais parâmetros.
    Se não existir utm_content, adiciona no FINAL da query string.
    """
    parsed = urllib.parse.urlsplit(original_url)
    query = parsed.query

    if not query:
        new_query = f"utm_content={utm_value}"
    else:
        pattern = re.compile(r'(^|&)(utm_content=)([^&]*)')
        if pattern.search(query):
            new_query = pattern.sub(lambda m: f"{m.group(1)}{m.group(2)}{utm_value}", query, count=1)
        else:
            sep = '&' if query and not query.endswith('&') else ''
            new_query = f"{query}{sep}utm_content={utm_value}"

    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))

def _generate_shopee_shortlink(origin_url: str, utm_content: str) -> str:
    """
    Gera shortlink via GraphQL, deixando subId3 = utm_content.
    Retorna o shortlink. Lança exceção se não houver shortLink.
    """
    payload_obj = {
        "query": (
            "mutation{generateShortLink(input:{"
            f"originUrl:\"{origin_url}\","
            f"subIds:[\"\",\"\",\"{utm_content}\",\"\",\"\"]"
            "}){shortLink}}"
        )
    }
    payload = json.dumps(payload_obj, separators=(',', ':'), ensure_ascii=False)
    ts = str(int(time.time()))
    signature = hashlib.sha256((SHOPEE_APP_ID + ts + payload + SHOPEE_APP_SECRET).encode("utf-8")).hexdigest()
    headers = {
        "Authorization": f"SHA256 Credential={SHOPEE_APP_ID}, Timestamp={ts}, Signature={signature}",
        "Content-Type": "application/json",
    }
    resp = requests.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    d = data.get("data") or {}
    gl = d.get("generateShortLink") or {}
    short = gl.get("shortLink")
    if not short:
        raise KeyError("Campo 'data.generateShortLink.shortLink' ausente")
    return short

def _flush_if_needed(force: bool = False) -> Optional[str]:
    """Sobe o buffer como CSV para o GCS quando atingir N linhas ou X segundos."""
    global _click_buffer, _last_flush_ts
    if not GCS_BUCKET_NAME:
        return None  # logging desativado

    bucket = _get_gcs_bucket()
    if bucket is None:
        return None

    do_flush = force or (len(_click_buffer) >= FLUSH_EVERY_N) or (time.time() - _last_flush_ts >= FLUSH_EVERY_SECONDS)
    if not do_flush or not _click_buffer:
        return None

    date_str = _date_str()
    part_id = (str(uuid.uuid4()).replace("-", "")[:7] + str(int(time.time()))[-4:])
    object_name = f"{GCS_PREFIX}/date={date_str}/clicks_{date_str}_part-{part_id}.csv"

    buf = io.StringIO()
    fieldnames = list(_click_buffer[0].keys())
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in _click_buffer:
        writer.writerow(row)
    data_csv = buf.getvalue()

    blob = bucket.blob(object_name)
    blob.upload_from_string(data_csv, content_type="text/csv")

    count = len(_click_buffer)
    _click_buffer = []
    _last_flush_ts = time.time()
    print(f"[FLUSH] {count} linha(s) → gs://{GCS_BUCKET_NAME}/{object_name}")
    return object_name

def _add_log_row(row: Dict[str, Any]):
    _click_buffer.append(row)
    _flush_if_needed(force=False)

# =============================================================================
#                              FASTAPI
# =============================================================================
app = FastAPI(title="Redirector Shopee · UTM dinâmica · Logs CSV (GCS)")

@app.get("/", response_class=PlainTextResponse)
def root():
    return "OK - Redirector ativo. Use /https://s.shopee.com.br/XXXX"

# Evita 500 para favicon
@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)

@app.get("/health")
def health():
    status = {"time": _iso_now(), "gcs_bucket": GCS_BUCKET_NAME or None}
    # Redis
    rcli = _get_redis()
    if rcli is None:
        status["redis"] = "not_configured_or_down"
    else:
        try:
            rcli.ping()
            status["redis"] = "ok"
        except Exception:
            status["redis"] = "down"
    # SQLite
    with suppress(Exception):
        _ = _sqlite_incr("__health__")
        status["sqlite"] = "ok"
    status.setdefault("sqlite", "down")
    status["shortlink_enabled"] = SHOPEE_SHORTLINK_ENABLED
    return status

@app.get("/{full_url:path}")
def redirect_with_dynamic_utm(
    request: Request,
    full_url: str = Path(..., description="URL começando por https:// (ex.: shortlink da Shopee)"),
    cat: Optional[str] = Query(None, description="Categoria opcional para log (livre)")
):
    """
    Fluxo:
      1) Recebe path tipo: /https://s.shopee.com.br/abc123?utm_x=...
      2) Expande shortlink → URL final do produto (mantém query original)
      3) Gera sufixo n{contador} e substitui APENAS utm_content (se não existir, adiciona no fim)
      4) (Opcional) Gera shortlink novo via GraphQL usando subId3 = utm_content
      5) Loga clique e redireciona 302
    """
    # Reconstrói URL original (quando vem percent-encoded no path)
    if not (full_url.startswith("http://") or full_url.startswith("https://")):
        full_url = urllib.parse.unquote(full_url)

    origin_short = full_url

    # 1) Expandir shortlink para final_url
    expanded = _expand_shortlink(origin_short)

    # 2) Gerar contador e utm_content
    n = _incr_counter()
    utm_value = f"n{n}"

    # 3) Substituir só utm_content (preservando ordem/estrutura). NÃO reordena outros parâmetros.
    final_with_utm = _replace_or_append_utm_content_preserving_order(expanded, utm_value)

    # Extrai utm_content final
    parsed_q = urllib.parse.urlsplit(final_with_utm).query
    m = re.search(r'(?:^|&)utm_content=([^&]*)', parsed_q)
    utm_content_final = m.group(1) if m else utm_value

    # 4) (Opcional) Gerar shortlink via GraphQL
    new_location = final_with_utm
    if SHOPEE_SHORTLINK_ENABLED and SHOPEE_APP_ID and SHOPEE_APP_SECRET:
        try:
            new_location = _generate_shopee_shortlink(final_with_utm, utm_content_final)
        except Exception as e:
            print(f"[ShopeeShortLink] ERRO ao gerar shortlink: {e}. Fallback para final_with_utm.")

    # 5) Log
    try:
        ua = request.headers.get("user-agent", "-")
        dev_name, os_family, os_version = _parse_device_os(ua)
        ip = _safe_get_ip(request)
        ref = request.headers.get("referer") or request.headers.get("Referer") or "-"

        q_all = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(final_with_utm).query, keep_blank_values=True))

        row = {
            "timestamp": _now_ts(),
            "iso_time": _iso_now(),
            "ip": ip,
            "user_agent": ua,
            "device_name": dev_name,
            "os_family": os_family,
            "os_version": os_version,
            "referrer": ref or "-",
            "short_link": origin_short,
            "final_url": final_with_utm,
            "redirect_to": new_location,
            "categoria": cat or "",
            # UTMs
            "utm_source": q_all.get("utm_source", ""),
            "utm_medium": q_all.get("utm_medium", ""),
            "utm_campaign": q_all.get("utm_campaign", ""),
            "utm_term": q_all.get("utm_term", ""),
            "utm_content": q_all.get("utm_content", utm_content_final),
            # SubIDs Shopee (se existirem)
            "sub_id1": q_all.get("sub_id1", ""),
            "sub_id2": q_all.get("sub_id2", ""),
            "sub_id3": q_all.get("sub_id3", ""),
            "sub_id4": q_all.get("sub_id4", ""),
            "sub_id5": q_all.get("sub_id5", ""),
            # Facebook param (se vier)
            "fbclid": q_all.get("fbclid", ""),
        }
        _add_log_row(row)
    except Exception as e:
        print("[LOG] erro ao montar linha de clique:", e)

    _flush_if_needed(force=False)

    # 6) Redireciona
    return RedirectResponse(new_location, status_code=302)

@app.get("/admin/flush")
def admin_flush():
    """Força o flush do buffer para o GCS (debug)."""
    path = _flush_if_needed(force=True)
    return {
        "ok": True,
        "flushed_to": f"gs://{GCS_BUCKET_NAME}/{path}" if path else None,
        "buffer_len": len(_click_buffer),
    }

# =============================================================================
#                              ENTRYPOINT
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
