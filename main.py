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
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

import requests
import redis
from fastapi import FastAPI, Request, Path, Query
from fastapi.responses import RedirectResponse, JSONResponse

# ─────────────────────── Configuração ───────────────────────
# Redis (contador UTM)
REDIS_URL        = os.getenv("REDIS_URL", "redis://localhost:6379/0")
UTM_COUNTER_KEY  = os.getenv("UTM_COUNTER_KEY", "utm_counter_global")

# Shopee Affiliate (shortlink)
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "")
SHOPEE_ENDPOINT   = "https://open-api.affiliate.shopee.com.br/graphql"

# Google Cloud Storage (logs CSV)
GCS_BUCKET_NAME   = os.getenv("GCS_BUCKET_NAME", "")  # ex.: "utm-click-logs"
GCS_PREFIX        = os.getenv("GCS_PREFIX", "logs")   # ex.: "logs"
# Caminho do JSON da Service Account (opcional). Se não setar, usa ADC padrão do ambiente.
GCS_SA_KEY_FILE   = os.getenv("GCS_SA_KEY_FILE", "")

# Lote/flush dos logs
FLUSH_EVERY_N        = int(os.getenv("FLUSH_EVERY_N", "50"))
FLUSH_EVERY_SECONDS  = int(os.getenv("FLUSH_EVERY_SECONDS", "60"))

# Comportamento de expansão do shortlink recebido (seguir redirecionamentos)
EXPAND_TIMEOUT_SEC   = float(os.getenv("EXPAND_TIMEOUT_SEC", "6.0"))

# ─────────────────────── Estado global ───────────────────────
r = redis.from_url(REDIS_URL)

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
        from google.cloud import storage
        if GCS_SA_KEY_FILE.strip():
            _gcs_client = storage.Client.from_service_account_json(GCS_SA_KEY_FILE.strip())
        else:
            _gcs_client = storage.Client()
        _gcs_bucket = _gcs_client.bucket(GCS_BUCKET_NAME)
    return _gcs_bucket

# ─────────────────────── Funções auxiliares ───────────────────────
def _now_ts() -> int:
    return int(time.time())

def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")

def _date_str(ts: Optional[int] = None) -> str:
    dt = datetime.utcfromtimestamp(ts or _now_ts())
    return dt.strftime("%Y-%m-%d")

def _safe_get_ip(req: Request) -> str:
    # Render/Proxies
    xfwd = req.headers.get("x-forwarded-for")
    if xfwd:
        # pega o primeiro da lista
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
    # OS
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

def _incr_counter() -> int:
    return int(r.incr(UTM_COUNTER_KEY))

def _append_n_to_all_utms(url: str, suffix: str) -> str:
    """
    Acrescenta 'suffix' (ex.: 'n123') ao final de TODAS as UTMs existentes.
    Se nenhuma UTM existir, cria utm_content=suffix.
    """
    parsed = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    utm_keys = [k for k in q.keys() if k.lower().startswith("utm_")]
    if utm_keys:
        for k in utm_keys:
            vals = q.get(k, [])
            new_vals = []
            for v in vals:
                v = v or ""
                # Evita duplicar se já tiver exatamente o mesmo sufixo
                if not v.endswith(suffix):
                    new_vals.append(v + suffix)
                else:
                    new_vals.append(v)
            q[k] = new_vals
    else:
        q["utm_content"] = [suffix]

    new_query = urllib.parse.urlencode(q, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))

def _expand_shortlink(url: str) -> str:
    """
    Tenta seguir redirecionamentos do shortlink para obter a URL final de produto.
    Em caso de erro, devolve a URL original.
    """
    try:
        # GET com redirects (HEAD às vezes bloqueia em alguns domínios)
        resp = requests.get(url, allow_redirects=True, timeout=EXPAND_TIMEOUT_SEC, headers={"User-Agent": "Mozilla/5.0"})
        return resp.url or url
    except Exception:
        return url

def _generate_shopee_shortlink(origin_url: str, utm_content: str) -> str:
    """
    Gera shortlink novo na Shopee, usando utm_content no SubID3.
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
    rqs = requests.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=15)
    rqs.raise_for_status()
    data = rqs.json()
    return data["data"]["generateShortLink"]["shortLink"]

def _flush_if_needed(force: bool = False) -> Optional[str]:
    """
    Sobe o buffer como CSV para o GCS quando atingir N linhas ou X segundos.
    Retorna o path do GCS se fez flush.
    """
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
    # Nome "part" aleatório pra evitar colisão
    part_id = str(uuid.uuid4()).replace("-", "")[:7] + str(int(time.time()))[-4:]
    object_name = f"{GCS_PREFIX}/date={date_str}/clicks_{date_str}_part-{part_id}.csv"

    # CSV em memória
    buf = io.StringIO()
    fieldnames = list(_click_buffer[0].keys())
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in _click_buffer:
        writer.writerow(row)
    data = buf.getvalue()

    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type="text/csv")

    count = len(_click_buffer)
    _click_buffer = []
    _last_flush_ts = time.time()
    print(f"[FLUSH] {count} linha(s) → gs://{GCS_BUCKET_NAME}/{object_name}")
    return object_name

def _add_log_row(row: Dict[str, Any]):
    _click_buffer.append(row)
    _flush_if_needed(force=False)

# ─────────────────────── FastAPI ───────────────────────
app = FastAPI(title="Redirector Shopee + UTM dinâmica + CSV no GCS")

@app.get("/health")
def health():
    return {"ok": True, "time": _iso_now(), "redis": True, "gcs_bucket": GCS_BUCKET_NAME or None}

@app.get("/{full_url:path}")
def redirect_with_dynamic_utm(
    request: Request,
    full_url: str = Path(..., description="Shortlink Shopee completo começando por https://"),
    cat: Optional[str] = Query(None, description="Categoria opcional para log (livre)")
):
    """
    Exemplo de chamada:
      GET /https://s.shopee.com.br/9KYd3xqbXC
    Passos:
      1) Expande shortlink -> URL final (produto) com UTMs existentes
      2) Gera sufixo n{contador} e acrescenta em TODAS as UTMs (se não houver, cria utm_content)
      3) Gera shortlink novo (Shopee GraphQL) com subId3 = utm_content atualizado
      4) Loga clique em CSV (GCS) + retorna 302 para o novo shortlink
    """

    # Reconstroi a URL original (pois o FastAPI te entrega sem a barra inicial em full_url)
    if not (full_url.startswith("http://") or full_url.startswith("https://")):
        # Se o path veio duplo (p.ex. "/https%3A//s..."), tente decodificar
        full_url = urllib.parse.unquote(full_url)
    origin_short = full_url

    # 1) Expandir shortlink para final_url (melhor para tocar nas UTMs originais)
    expanded = _expand_shortlink(origin_short)

    # 2) Gerar sufixo UTM n{contador}
    n = _incr_counter()
    suffix = f"n{n}"

    # 2.1) Acrescentar sufixo a TODAS as UTMs (ou criar utm_content=suffix)
    final_with_n = _append_n_to_all_utms(expanded, suffix)

    # Captura o valor final de utm_content (caso exista)
    parsed = urllib.parse.urlsplit(final_with_n)
    q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    utm_content = (q.get("utm_content", [suffix]) or [suffix])[0]

    # 3) Gerar shortlink novo na Shopee
    try:
        new_short = _generate_shopee_shortlink(final_with_n, utm_content)
    except Exception as e:
        print(f"[ShopeeShortLink] ERRO ao gerar shortlink: {e}. Fallback para final_with_n.")
        new_short = final_with_n  # fallback

    # 4) Logar clique em memória (CSV → GCS por flush)
    try:
        ua = request.headers.get("user-agent", "-")
        dev_name, os_family, os_version = _parse_device_os(ua)
        ip = _safe_get_ip(request)
        ref = request.headers.get("referer") or request.headers.get("referer".title()) or "-"
        fbclid = q.get("fbclid", [""])[0] if "fbclid" in q else (request.query_params.get("fbclid") or "")
        fbp = ""   # cookies só seriam acessíveis se o domínio fosse seu; aqui deixamos vazio
        fbc = ""   # idem (ou constrói se você usa fbclid para isso)

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
            "final_url": final_with_n,
            "categoria": cat or "",
            # UTM base (todas as que existirem)
            "utm_source": (q.get("utm_source", [""])[0]),
            "utm_medium": (q.get("utm_medium", [""])[0]),
            "utm_campaign": (q.get("utm_campaign", [""])[0]),
            "utm_term": (q.get("utm_term", [""])[0]),
            "utm_content": utm_content,
            # SubIDs padrão Shopee (se estiverem presentes)
            "sub_id1": (q.get("sub_id1", [""])[0]),
            "sub_id2": (q.get("sub_id2", [""])[0]),
            "sub_id3": (q.get("sub_id3", [""])[0]),
            "sub_id4": (q.get("sub_id4", [""])[0]),
            "sub_id5": (q.get("sub_id5", [""])[0]),
            # Facebook params (se vier pelo app/IG)
            "fbclid": fbclid,
            "fbp": fbp,
            "fbc": fbc,
        }
        _add_log_row(row)
    except Exception as e:
        print("[LOG] erro ao montar linha de clique:", e)

    # Força flush se ficou muito tempo sem enviar
    _flush_if_needed(force=False)

    # 5) Redireciona
    return RedirectResponse(new_short, status_code=302)

@app.get("/admin/flush")
def admin_flush():
    """Força o flush do buffer para o GCS (debug)."""
    path = _flush_if_needed(force=True)
    return {"ok": True, "flushed_to": f"gs://{GCS_BUCKET_NAME}/{path}" if path else None, "buffer_len": len(_click_buffer)}

# ─────────────────────── Entrypoint ───────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
