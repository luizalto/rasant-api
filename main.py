# -*- coding: utf-8 -*-
"""
FastAPI – Logger de cliques em CSV (GCS) + Redis (contador infinito) + Shopee ShortLink + Meta CAPI

Como usar (short-link direto SEM codificar no path):
  https://SEU-APP.onrender.com/https://s.shopee.com.br/XXXX?cat=beleza

ENV (Render):
  # GCP (um dos dois)
  GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/service_account.json
  ou GOOGLE_APPLICATION_CREDENTIALS_JSON=<conteudo do JSON>

  # GCS
  GCS_BUCKET=utm-click-logs
  (opcionais) GCS_PREFIX=logs/ | FLUSH_MAX_ROWS=500 | FLUSH_MAX_SECONDS=30
  (opcionais) HTTP_TIMEOUT=12 | ADMIN_TOKEN=troque_isto

  # Redis
  REDIS_URL=redis://default:senha@host:6379/0
  UTM_COUNTER_KEY=utm_counter

  # Meta CAPI
  FB_PIXEL_ID=XXXXXXXXXXXXXXX
  FB_ACCESS_TOKEN=EAAB... (ou META_* equivalentes)

  # Shopee Affiliate
  SHOPEE_APP_ID=18314810331
  SHOPEE_APP_SECRET=LO3QSEG45TYP4NYQBRXLA2YYUL3ZCUPN

  # Scoring (opcional)
  ATC_BASELINE_RATE=0.05
  ATC_THRESHOLD=0.15
  ATC_MAX_RATE=0.25
  RULES_KEY=rules:hour_category
  MODEL_KEY=model:logreg
  MODEL_META_KEY=model:meta

  # Vários
  USERDATA_TTL_SECONDS=604800
  MAX_DELAY_SECONDS=604800
  CLICK_WINDOW_SECONDS=3600
  MAX_CLICKS_PER_FP=2
  FINGERPRINT_PREFIX=fp:
  EMIT_INTERNAL_BLOCK_LOG=true
  VIDEO_ID=v15  # apenas para logs; a UTM numerada usa o contador
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json
from typing import Optional, Dict, Tuple, List, Any

import requests
import redis
from fastapi import FastAPI, Request, Query, Path, UploadFile, File
from fastapi.responses import RedirectResponse, JSONResponse

# ─────────────────────────── Credenciais GCP ────────────────────────────────
def _bootstrap_gcp_credentials():
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    raw_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if raw_json:
        path = "/tmp/gcp.json"
        with open(path, "w", encoding="utf-8") as f:
            f.write(raw_json)
        # corrige e garante a env certa
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path

_bootstrap_gcp_credentials()
from google.cloud import storage  # importar após preparar as credenciais

# ───────────────────────────── Configuração ─────────────────────────────────
DEFAULT_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "12"))
GCS_BUCKET        = os.getenv("GCS_BUCKET")
GCS_PREFIX        = os.getenv("GCS_PREFIX", "logs/")
FLUSH_MAX_ROWS    = int(os.getenv("FLUSH_MAX_ROWS", "500"))
FLUSH_MAX_SECONDS = int(os.getenv("FLUSH_MAX_SECONDS", "30"))
ADMIN_TOKEN       = os.getenv("ADMIN_TOKEN", "12345678")

# Redis / chaves
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COUNTER_KEY          = os.getenv("UTM_COUNTER_KEY", "utm_counter")
USERDATA_TTL_SECONDS = int(os.getenv("USERDATA_TTL_SECONDS", "604800"))
USERDATA_KEY_PREFIX  = os.getenv("USERDATA_KEY_PREFIX", "ud:")
MAX_DELAY_SECONDS    = int(os.getenv("MAX_DELAY_SECONDS", str(7 * 24 * 60 * 60)))

CLICK_WINDOW_SECONDS = int(os.getenv("CLICK_WINDOW_SECONDS", "3600"))
MAX_CLICKS_PER_FP    = int(os.getenv("MAX_CLICKS_PER_FP", "2"))
FINGERPRINT_PREFIX   = os.getenv("FINGERPRINT_PREFIX", "fp:")
EMIT_INTERNAL_BLOCK_LOG = str(os.getenv("EMIT_INTERNAL_BLOCK_LOG", "true")).lower() == "true"
VIDEO_ID             = os.getenv("VIDEO_ID", "v15")  # apenas info de log

# Meta (Conversions API)
FB_PIXEL_ID     = os.getenv("FB_PIXEL_ID") or os.getenv("META_PIXEL_ID") or "COLOQUE_SEU_PIXEL_ID_AQUI"
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN") or os.getenv("META_ACCESS_TOKEN") or "COLOQUE_SEU_ACCESS_TOKEN_AQUI"
FB_ENDPOINT     = f"https://graph.facebook.com/v14.0/{FB_PIXEL_ID}/events?access_token={FB_ACCESS_TOKEN}"

# Shopee Affiliate
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "18314810331")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "LO3QSEG45TYP4NYQBRXLA2YYUL3ZCUPN")
SHOPEE_ENDPOINT   = "https://open-api.affiliate.shopee.com.br/graphql"

# ───────────────────────────── App / Clients ────────────────────────────────
app = FastAPI(title="Click Logger → GCS + Redis + Shopee ShortLink + Meta CAPI")

# Redis
r = redis.from_url(REDIS_URL)

# GCS
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
    "short_link","final_url","original_utm","category",
    "utm_content","sub_id1","sub_id2","sub_id3","sub_id4","sub_id5",
    "fbclid","fbp","fbc"
]

# ───────────────────────────── Helpers gerais ───────────────────────────────
def incr_counter() -> int:
    return int(r.incr(COUNTER_KEY))

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

def _day_key(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts))

def _gcs_object_name(ts: int, part: int) -> str:
    d = _day_key(ts)
    return f"{GCS_PREFIX}date={d}/clicks_{d}_part-{part:04d}.csv"

def _flush_buffer_to_gcs(force: bool = False) -> int:
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

# ─────────────────────── Meta CAPI / Scoring / Redis data ───────────────────
def send_fb_event(event_name: str, event_id: str, event_source_url: str,
                  user_data: Dict[str, Any], custom_data: Dict[str, Any], event_time: int) -> Dict[str, Any]:
    payload = {
        "data": [{
            "event_name": event_name,
            "event_time": int(event_time),
            "event_id": event_id,
            "action_source": "website",
            "event_source_url": event_source_url,
            "user_data": user_data,
            "custom_data": custom_data
        }]}
    rqs = requests.post(FB_ENDPOINT, json=payload, timeout=20)
    try: return rqs.json()
    except Exception: return {"status_code": rqs.status_code, "text": rqs.text}

def parse_device_os(ua: str) -> Tuple[str, str]:
    if not ua: return ("-", "-")
    m = re.search(r"iPhone OS (\d+)_?", ua) or re.search(r"CPU iPhone OS (\d+)", ua)
    if m: return ("iOS", m.group(1))
    m = re.search(r"Android (\d+)", ua)
    if m: return ("Android", m.group(1))
    if "iPhone" in ua or "iPad" in ua: return ("iOS", "-")
    if "Android" in ua: return ("Android", "-")
    return ("Other", "-")

def make_fingerprint(ip: str, ua: str) -> str:
    osfam, osmaj = parse_device_os(ua)
    return hashlib.sha1(f"{ip}|{osfam}|{osmaj}".encode("utf-8")).hexdigest()

def fp_counter_key(fp: str) -> str:
    return f"{FINGERPRINT_PREFIX}{fp}"

def allow_viewcontent(ip: str, ua: str) -> Tuple[bool, str, int]:
    fp = make_fingerprint(ip, ua)
    key = fp_counter_key(fp)
    cnt = r.incr(key)
    if cnt == 1:
        r.expire(key, CLICK_WINDOW_SECONDS)
    if cnt > MAX_CLICKS_PER_FP:
        return False, "rate_limited", int(cnt)
    return True, "ok", int(cnt)

# Regras/Modelo (consumo)
ATC_BASELINE_RATE = float(os.getenv("ATC_BASELINE_RATE", "0.05"))
ATC_THRESHOLD     = float(os.getenv("ATC_THRESHOLD", "0.15"))
ATC_MAX_RATE      = float(os.getenv("ATC_MAX_RATE", "0.25"))
RULES_KEY         = os.getenv("RULES_KEY", "rules:hour_category")
MODEL_KEY         = os.getenv("MODEL_KEY", "model:logreg")
MODEL_META_KEY    = os.getenv("MODEL_META_KEY", "model:meta")

def save_rules(rules: Dict[str, Any]) -> None:
    r.set(RULES_KEY, json.dumps(rules, ensure_ascii=False))

def load_rules() -> Dict[str, Any]:
    raw = r.get(RULES_KEY)
    if not raw:
        return {"by_hour": {}, "by_category": {}, "trained_at": None, "global_rate": ATC_BASELINE_RATE}
    try: return json.loads(raw)
    except Exception: return {"by_hour": {}, "by_category": {}, "trained_at": None, "global_rate": ATC_BASELINE_RATE}

def save_model_pickle_bytes(b: bytes) -> None:
    r.set(MODEL_KEY, b)
    r.set(MODEL_META_KEY, json.dumps({"saved_at": int(time.time())}))

def load_model_pickle_bytes() -> Optional[bytes]:
    raw = r.get(MODEL_KEY)
    return bytes(raw) if raw else None

def _norm_category(cat: Optional[str]) -> str:
    return (cat or "").strip().lower()

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def score_click_prob_rules(hour: Optional[int], category: Optional[str]) -> float:
    rules = load_rules()
    base = rules.get("global_rate", ATC_BASELINE_RATE) or ATC_BASELINE_RATE
    hfac = float(rules.get("by_hour", {}).get(str(int(hour)) if hour is not None else "", 1.0)) if hour is not None else 1.0
    cfac = float(rules.get("by_category", {}).get(_norm_category(category), 1.0)) if category else 1.0
    return _clamp(base * hfac * cfac, 0.0, ATC_MAX_RATE)

def model_predict_proba(features: Dict[str, Any]) -> Optional[float]:
    raw = load_model_pickle_bytes()
    if not raw: return None
    try:
        import pickle, numpy as np  # noqa: F401
        model = pickle.loads(raw)
        hour = int(features.get("hour")) if features.get("hour") is not None else -1
        cat  = _norm_category(features.get("category"))
        gr   = float(features.get("global_rate", ATC_BASELINE_RATE))
        x = [gr] + [1.0 if h==hour else 0.0 for h in range(24)]
        hv=0
        for ch in cat: hv=(hv*131+ord(ch))%(10**9+7)
        bucket=hv%5
        x += [1.0 if b==bucket else 0.0 for b in range(5)]
        return float(model.predict_proba([x])[0][1])
    except Exception as e:
        print("[MODEL] predict error:", e)
        return None

def score_click_prob(hour: Optional[int], category: Optional[str]) -> float:
    rules = load_rules()
    p_model = model_predict_proba({"hour": hour, "category": category, "global_rate": rules.get("global_rate", ATC_BASELINE_RATE)})
    return _clamp(p_model, 0.0, ATC_MAX_RATE) if p_model is not None else score_click_prob_rules(hour, category)

def maybe_send_add_to_cart(event_id: str, event_source_url: str, user_data: Dict[str, Any],
                           subid1: Optional[str], category: Optional[str], score_p: float) -> Optional[Dict[str, Any]]:
    if score_p < ATC_THRESHOLD: return None
    try:
        resp = send_fb_event(
            "AddToCart", event_id, event_source_url, user_data,
            {
                "content_category": category or "",
                "content_ids": [subid1 or "na"],
                "contents": [{"id": subid1 or "na", "quantity": 1}],
                "currency": "BRL",
                "value": 0
            },
            int(time.time())
        )
        print("[ATC] sent", {"event_id": event_id, "p": score_p, "resp": resp})
        return resp
    except Exception as e:
        print("[ATC] error", {"event_id": event_id, "p": score_p, "error": str(e)})
        return {"error": str(e)}

def save_user_data(utm: str, data: Dict[str, Any]) -> None:
    r.setex(f"{USERDATA_KEY_PREFIX}{utm}", USERDATA_TTL_SECONDS, json.dumps(data))

def load_user_data(utm: str) -> Optional[Dict[str, Any]]:
    raw = r.get(f"{USERDATA_KEY_PREFIX}{utm}")
    if not raw: return None
    try: return json.loads(raw)
    except Exception: return None

def fbc_creation_ts(fbc: Optional[str]) -> Optional[int]:
    if not fbc: return None
    try: return int(fbc.split(".")[2])
    except Exception: return None

# ───────────────────────── Shopee: gerar short link ─────────────────────────
def generate_short_link(origin_url: str, utm_content: str) -> str:
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
    headers = {"Authorization": f"SHA256 Credential={SHOPEE_APP_ID}, Timestamp={ts}, Signature={signature}",
               "Content-Type": "application/json"}
    resp = requests.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()["data"]["generateShortLink"]["shortLink"]

# ───────────────────────────── Rotas ────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET, "prefix": GCS_PREFIX, "pixel": FB_PIXEL_ID, "video_id": VIDEO_ID}

@app.get("/{full_path:path}")
def track_number_and_redirect(
    request: Request,
    full_path: str = Path(..., description="Short-link direto, ex.: https://s.shopee.com.br/XXXX"),
    cat: Optional[str] = Query(None, description="Categoria opcional p/ scoring (e logging)"),
):
    """
    1) Recebe short-link direto no path, resolve até a URL final (Shopee).
    2) Lê o utm_content existente dessa URL (original) e ACRESCENTA numeração sequencial do Redis.
       - Apenas modifica o valor de utm_content; mantém TODOS os demais parâmetros e a ordem.
    3) Gera um short-link OFICIAL da Shopee com subIds[2] = utm_content numerada (fallback para URL longa).
    4) Dispara ViewContent (e AddToCart condicional por regras/modelo).
    5) Salva user_data no Redis (chaveada pela UTM numerada).
    6) Loga o clique em CSV no GCS (inclui original_utm e utm_content numerada) e redireciona (302) para o destino gerado.
    """
    s = full_path  # short-link informado
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

    # Resolve short-link → URL final e subIDs/UTM originais
    resolved_url, subids_in = resolve_short_link(s)
    base_utm = subids_in.get("utm_content") or ""
    if base_utm is None: base_utm = ""

    # Numeração infinita: acrescenta o número ao lado da UTM original
    seq = incr_counter()
    utm_numbered = f"{base_utm}{seq}"

    # Monta URL final substituindo somente utm_content
    final_with_number = replace_utm_content_only(resolved_url, utm_numbered)

    # Shopee short link com subIds[2] = utm_numbered
    try:
        dest = generate_short_link(final_with_number, utm_numbered)
    except Exception as e:
        print(f"[ShopeeShortLink] ERRO ao gerar shortlink: {e}. Fallback para final_with_number.")
        dest = final_with_number

    # Anti-bot e CAPI
    allowed, reason, cnt = allow_viewcontent(ip_addr, user_agent)
    vc_time = ts
    user_data_vc: Dict[str, Any] = {"client_ip_address": ip_addr, "client_user_agent": user_agent}
    if fbp_cookie: user_data_vc["fbp"] = fbp_cookie
    if fbc_val:    user_data_vc["fbc"] = fbc_val

    capi_vc_resp: Dict[str, Any] = {"skipped": True, "reason": reason}
    if allowed:
        try:
            capi_vc_resp = send_fb_event("ViewContent", utm_numbered, final_with_number, user_data_vc, {"content_type": "product"}, vc_time)
        except Exception as e:
            capi_vc_resp = {"error": str(e)}

    # Scoring e ATC
    try:
        hour_now = time.localtime(vc_time).tm_hour
        atc_p = score_click_prob(hour_now, cat)
        atc_resp = maybe_send_add_to_cart(utm_numbered, final_with_number, user_data_vc, None, cat, atc_p)
    except Exception as e:
        atc_p = None
        atc_resp = {"error": str(e)}

    # Cache para futuras compras
    save_user_data(utm_numbered, {
        "user_data": user_data_vc, "event_source_url": final_with_number, "vc_time": vc_time,
        "allowed_vc": allowed, "reason": reason, "count_in_window": cnt,
        "atc_prob": atc_p, "atc_resp": atc_resp
    })

    # Log da linha (inclui ORIGINAL e NUMERADA)
    csv_row = [
        ts, iso_time, ip_addr, user_agent, device_name, os_family, os_version, referrer,
        s, final_with_number, base_utm, (cat or ""),
        utm_numbered,
        subids_in.get("sub_id1") or "", subids_in.get("sub_id2") or "",
        utm_numbered,  # sub_id3 coerente com utm numerada
        subids_in.get("sub_id4") or "", subids_in.get("sub_id5") or "",
        (fbclid or ""), (fbp_cookie or ""), (fbc_val or "")
    ]
    print("Novo clique registrado:", {
        "timestamp": ts, "iso_time": iso_time, "ip": ip_addr, "user_agent": user_agent,
        "device_name": device_name, "os_family": os_family, "os_version": os_version, "referrer": referrer,
        "short_link": s, "resolved_url": resolved_url, "final_url": final_with_number, "dest": dest,
        "category": cat or "", "original_utm": base_utm, "utm_numbered": utm_numbered, "seq": seq,
        "fbclid": fbclid or "", "fbp": fbp_cookie or "", "fbc": fbc_val or "",
        "allowed": allowed, "reason": reason, "cnt": cnt, "atc_p": atc_p,
        "vc_resp": capi_vc_resp
    }, flush=True)

    with _buffer_lock:
        _buffer_rows.append(csv_row)
    _flush_buffer_to_gcs(force=False)

    if (not allowed) and EMIT_INTERNAL_BLOCK_LOG:
        print("[BLOCKED_VC]", json.dumps({"utm": utm_numbered, "ip": ip_addr, "ua": user_agent, "reason": reason, "cnt": cnt}, ensure_ascii=False))

    return RedirectResponse(dest, status_code=302)

# Força flush
@app.get("/admin/flush")
def admin_flush(token: str):
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    sent = _flush_buffer_to_gcs(force=True)
    return {"ok": True, "sent_rows": sent}

# ─────────────── Upload CSV (dispara Purchase) – com UTM numerada ───────────
@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    content = await file.read()
    text = content.decode("utf-8", errors="replace").splitlines()
    reader = csv.DictReader(text)

    processed: List[Dict[str, Any]] = []
    now_ts = int(time.time())
    min_allowed = now_ts - MAX_DELAY_SECONDS

    def normalize_utm(u: Optional[str]) -> Optional[str]:
        # Mantém exatamente como chegou (sem remover sufixos)
        return u if u else None

    for row in reader:
        raw_utm = (
            row.get("utm_content") or row.get("utm") or
            row.get("sub_id3") or row.get("subid3") or row.get("sub_id_3") or
            row.get("Sub_id3") or row.get("SUBID3")
        )
        utm = normalize_utm(raw_utm)
        if not utm:
            processed.append({"row": row, "status": "skipped_no_utm"})
            continue

        valor_raw  = row.get("value") or row.get("valor") or row.get("price") or row.get("amount")
        vendas_raw = row.get("num_purchases") or row.get("vendas") or row.get("quantity") or row.get("qty") or row.get("purchases")

        try: valor = float(str(valor_raw).replace(",", ".")) if valor_raw not in (None, "") else 0.0
        except Exception: valor = 0.0
        try: vendas = int(float(vendas_raw)) if vendas_raw not in (None, "") else 1
        except Exception: vendas = 1

        cache = load_user_data(utm)
        if not cache or not cache.get("user_data"):
            processed.append({"utm_content": raw_utm, "utm_norm": utm, "status": "skipped_no_user_data"})
            continue

        user_data_purchase = cache["user_data"]
        event_source_url   = cache.get("event_source_url") or ""
        vc_time            = cache.get("vc_time")
        event_time = int(vc_time) if isinstance(vc_time, int) else now_ts

        if event_time > now_ts: event_time = now_ts
        if event_time < min_allowed: event_time = min_allowed
        click_ts = fbc_creation_ts(user_data_purchase.get("fbc"))
        if click_ts and event_time < click_ts: event_time = click_ts + 1

        if cache.get("allowed_vc") is False:
            processed.append({"utm_content": raw_utm, "utm_norm": utm, "status": "skipped_blocked_vc"})
            continue

        custom_data_purchase = {"currency": "BRL", "value": valor, "num_purchases": vendas}

        try:
            resp = send_fb_event("Purchase", utm, event_source_url or "https://shopee.com.br", user_data_purchase, custom_data_purchase, event_time)
            processed.append({"utm_content": raw_utm, "utm_norm": utm, "status": "sent", "capi": resp})
        except Exception as e:
            processed.append({"utm_content": raw_utm, "utm_norm": utm, "status": "error", "error": str(e)})

    return JSONResponse({"processed": processed})

# ───────────────────────────── ADMIN (sem treino) ───────────────────────────
def _require_admin(token: str):
    if token != ADMIN_TOKEN:
        raise ValueError("unauthorized")

@app.get("/admin/config")
def admin_config(token: str):
    try:
        _require_admin(token)
        return {"ok": True, "config": {
            "CLICK_WINDOW_SECONDS": CLICK_WINDOW_SECONDS,
            "MAX_CLICKS_PER_FP": MAX_CLICKS_PER_FP,
            "USERDATA_TTL_SECONDS": USERDATA_TTL_SECONDS,
            "MAX_DELAY_SECONDS": MAX_DELAY_SECONDS,
            "VIDEO_ID": VIDEO_ID,
            "FB_PIXEL_ID": FB_PIXEL_ID,
            "SHOPEE_APP_ID": SHOPEE_APP_ID,
            "ATC_BASELINE_RATE": ATC_BASELINE_RATE,
            "ATC_THRESHOLD": ATC_THRESHOLD,
            "ATC_MAX_RATE": ATC_MAX_RATE,
            "GCS_BUCKET": GCS_BUCKET,
            "GCS_PREFIX": GCS_PREFIX
        }}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=401)

@app.get("/admin/rules")
def admin_get_rules(token: str):
    try:
        _require_admin(token)
        return {"ok": True, "rules": load_rules(), "baseline": ATC_BASELINE_RATE, "threshold": ATC_THRESHOLD}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=401)

@app.get("/admin/preview_score")
def admin_preview_score(token: str, hour: Optional[int] = None, category: Optional[str] = None):
    try:
        _require_admin(token)
        pr = score_click_prob_rules(hour, category)
        pf = score_click_prob(hour, category)
        return {"ok": True, "p_rules": pr, "p_final": pf, "hour": hour, "category": category}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=401)

@app.post("/admin/upload_rules")
async def admin_upload_rules(token: str = Query(...), file: UploadFile = File(...)):
    try:
        _require_admin(token)
        content = await file.read()
        rules = json.loads(content.decode("utf-8"))
        save_rules(rules)
        return {"ok": True, "msg": "Regras carregadas", "rules_snapshot": {"by_hour": len(rules.get("by_hour",{})), "by_category": len(rules.get("by_category",{}))}}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Erro lendo regras: {e}"}, status_code=400)

@app.post("/admin/upload_model")
async def admin_upload_model(token: str = Query(...), file: UploadFile = File(...)):
    try:
        _require_admin(token)
        content = await file.read()
        save_model_pickle_bytes(content)
        return {"ok": True, "msg": "Modelo carregado"}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Erro lendo modelo: {e}"}, status_code=400)

# Flush final ao encerrar
@atexit.register
def _flush_on_exit():
    try:
        _flush_buffer_to_gcs(force=True)
    except Exception:
        pass

# ───────────────────────────── START ─────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
