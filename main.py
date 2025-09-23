# -*- coding: utf-8 -*-
"""
FastAPI – Shopee ShortLink otimizado + Redis + GCS + Geo (IP) + Predição (CatBoost) + Meta Ads
- Mantém lógica original de UTM numerada (N<seq>) e shortlink Shopee.
- Envia linha para GCS em background.
- Geolocaliza IP e PREDIZ para TODOS os cliques.
- Se predição == "comprou": envia AddToCart para Meta Ads; caso contrário: não envia.
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json
from typing import Optional, Dict, Tuple, List, Any

import requests
import redis
from fastapi import FastAPI, Request, Query, Path, UploadFile, File, Header, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse
from google.cloud import storage
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode, unquote

# ===================== Config (env c/ defaults) =====================
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
GEO_CACHE_TTL_SECONDS= int(os.getenv("GEO_CACHE_TTL_SECONDS", "259200"))  # 3 dias
GEO_CACHE_PREFIX     = os.getenv("GEO_CACHE_PREFIX", "geo:")

VIDEO_ID = os.getenv("VIDEO_ID", "v15")

# Shopee Affiliate
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "18314810331")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "LO3QSEG45TYP4NYQBRXLA2YYUL3ZCUPN")
SHOPEE_ENDPOINT   = os.getenv("SHOPEE_ENDPOINT", "https://open-api.affiliate.shopee.com.br/graphql")

# Meta Ads (opcional)
META_PIXEL_ID       = os.getenv("META_PIXEL_ID")            # ex: "123456789012345"
META_ACCESS_TOKEN   = os.getenv("META_ACCESS_TOKEN")        # token do pixel
META_GRAPH_VERSION  = os.getenv("META_GRAPH_VERSION", "v17.0")
META_TEST_EVENT_CODE= os.getenv("META_TEST_EVENT_CODE")     # opcional para modo teste

# Modelos (gravação no diretório do projeto)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODELS_DIR, exist_ok=True)
MODEL_PATH = os.path.join(MODELS_DIR, "model.cbm")
MODEL_META_PATH = os.path.join(MODELS_DIR, "model_meta.json")

# ===================== App / Clients =====================
app = FastAPI(title="Shopee UTM Numbered → ShortLink + Redis + GCS + Geo + Predict")

# Redis
r = redis.from_url(REDIS_URL)

# GCS
_gcs_client: Optional[storage.Client] = None
_bucket: Optional[storage.Bucket] = None
if GCS_BUCKET:
    _gcs_client = storage.Client()
    _bucket = _gcs_client.bucket(GCS_BUCKET)

# Sessão HTTP global
session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50)
session.mount("http://", _adapter)
session.mount("https://", _adapter)

# ===================== Buffer CSV =====================
_buffer_lock = threading.Lock()
_buffer_rows: List[List[str]] = []
_last_flush_ts = time.time()

CSV_HEADERS = [
    "timestamp","iso_time","ip","user_agent","device_name","os_family","os_version","referrer",
    "short_link_in","resolved_url","final_url","category",
    "utm_original","utm_numbered",
    "sub_id1","sub_id2","sub_id3","sub_id4","sub_id5",
    "fbclid","fbp","fbc",
    # geo enriquecido
    "geo_status","geo_country","geo_region","geo_state","geo_city","geo_zip","geo_isp","geo_org","geo_asn","geo_lat","geo_lon",
    # predição
    "pred_label","p_comprou","p_quase","p_desinteressado","meta_event_sent"
]

# ===================== Helpers gerais =====================
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
        print(f"[ShopeeShortLink] ERRO: {e}. Fallback para URL numerada.")
        return origin_url

# ===================== Geo por IP (com cache em Redis) =====================
IPAPI_FIELDS = "status,country,region,regionName,city,zip,lat,lon,isp,org,as,query"
def geo_lookup(ip: str) -> Dict[str, Any]:
    """Busca geolocalização no cache do Redis; se não houver, consulta ip-api.com/json/<ip>."""
    if not ip or ip.startswith("10.") or ip.startswith("192.168.") or ip.startswith("172.16.") or ip == "127.0.0.1":
        return {"geo_status":"fail"}
    cache_key = f"{GEO_CACHE_PREFIX}{ip}"
    try:
        cached = r.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass
    try:
        url = f"http://ip-api.com/json/{ip}?fields={IPAPI_FIELDS}&lang=pt-BR"
        resp = session.get(url, timeout=5)
        data = resp.json()
        norm = {
            "geo_status": data.get("status","fail"),
            "geo_country": data.get("country"),
            "geo_state": data.get("regionName"),
            "geo_region": data.get("region"),
            "geo_city": data.get("city"),
            "geo_zip": data.get("zip"),
            "geo_lat": data.get("lat"),
            "geo_lon": data.get("lon"),
            "geo_isp": data.get("isp"),
            "geo_org": data.get("org"),
            "geo_asn": data.get("as"),
        }
        r.setex(cache_key, GEO_CACHE_TTL_SECONDS, json.dumps(norm, ensure_ascii=False))
        return norm
    except Exception as e:
        print("[geo_lookup] erro:", e)
        return {"geo_status":"fail"}

# ===================== Predição (CatBoost) =====================
_model = None
_model_classes: List[str] = []
_thresholds: Dict[str, float] = {}

def _load_model_if_available():
    global _model, _model_classes, _thresholds
    try:
        from catboost import CatBoostClassifier
        if os.path.exists(MODEL_PATH):
            m = CatBoostClassifier()
            m.load_model(MODEL_PATH)
            _model = m
            # classes e thresholds do JSON (opcional)
            if os.path.exists(MODEL_META_PATH):
                with open(MODEL_META_PATH, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                _model_classes = meta.get("classes") or ["comprou","desinteressado","quase"]
                _thresholds = meta.get("thresholds") or {}
            else:
                _model_classes = ["comprou","desinteressado","quase"]
                _thresholds = {}
            print("[model] carregado com sucesso.")
        else:
            print("[model] arquivo não encontrado:", MODEL_PATH)
    except Exception as e:
        print("[model] erro ao carregar:", e)

def _features_for_model(os_family, device_name, os_version, referrer, utm_numbered,
                        iso_time=None, geo: Optional[Dict[str,Any]]=None):
    # Pequena engenharia de atributos consistente com treino (versão mínima)
    def version_num(s):
        m = re.search(r"(\d+(\.\d+)*)", str(s or ""))
        if not m: return 0.0
        try: return float(m.group(1).split(".")[0])
        except: return 0.0
    def get_first_domain(url):
        try:
            from urllib.parse import urlparse
            netloc = urlparse(str(url)).netloc.lower()
            return netloc[4:] if netloc.startswith("www.") else netloc
        except: return ""
    def extract_hour_from_iso(t):
        try:
            # t no formato "YYYY-mm-ddTHH:MM:SS-0300"
            hh = str(t).split("T")[1][:2]
            return int(hh)
        except:
            return 0
    def part_of_day(hour):
        h = int(hour)
        if 0<=h<6: return "dawn"
        if 6<=h<12: return "morning"
        if 12<=h<18: return "afternoon"
        if 18<=h<=23: return "evening"
        return "unknown"

    hour = extract_hour_from_iso(iso_time or "")
    is_android = 1 if str(os_family).lower().find("android")>=0 else 0
    is_ios     = 1 if re.search(r"ios|iphone|ipad", str(os_family), re.I) else 0
    os_ver_num = version_num(os_version)
    ref_domain = get_first_domain(referrer or "")
    utm_prefix = re.findall(r"^([A-Za-z0-9]+)", str(utm_numbered or ""))[:1]
    utm_prefix = utm_prefix[0] if utm_prefix else ""

    geo = geo or {}
    # Ordem: categóricas primeiro
    cat = [
        device_name or "",
        os_family or "",
        part_of_day(hour),
        geo.get("geo_country") or "",
        geo.get("geo_region") or "",
        geo.get("geo_city") or "",
        geo.get("geo_zip") or "",
        geo.get("geo_isp") or "",
        geo.get("geo_org") or "",
        ref_domain,
        utm_prefix
    ]
    num = [hour, 0, os_ver_num, is_android, is_ios]  # dow=0 (não usamos aqui)
    # CatBoost aceita uma lista com [cats..., nums...]
    return cat + num

def predict_label(proba: List[float], classes: List[str]) -> str:
    # Com thresholds hierárquicos: 1) comprou 2) quase 3) desinteressado
    try:
        idx = {c:i for i,c in enumerate(classes)}
        p = {c: proba[idx[c]] for c in classes}
        thr_c = float(_thresholds.get("comprou", 0.0))
        thr_q = float(_thresholds.get("quase", 0.0))
        if "comprou" in classes and p.get("comprou",0.0) >= thr_c:
            return "comprou"
        if "quase" in classes and p.get("quase",0.0) >= thr_q:
            return "quase"
        return "desinteressado"
    except Exception:
        # fallback argmax
        j = int(max(range(len(proba)), key=lambda i: proba[i]))
        return classes[j]

_load_model_if_available()

# ===================== Meta Ads (opcional) =====================
def send_meta_event_add_to_cart(ip: str, user_agent: str, fbp: Optional[str], fbc: Optional[str],
                                event_source_url: str, event_time: int) -> bool:
    """Envia AddToCart ao Meta Ads. Retorna True se 200 OK."""
    if not (META_PIXEL_ID and META_ACCESS_TOKEN):
        return False
    try:
        url = f"https://graph.facebook.com/{META_GRAPH_VERSION}/{META_PIXEL_ID}/events"
        payload = {
            "data": [{
                "event_name": "AddToCart",
                "event_time": event_time,
                "action_source": "website",
                "event_source_url": event_source_url,
                "user_data": {
                    "client_ip_address": ip,
                    "client_user_agent": user_agent,
                    **({"fbp": fbp} if fbp else {}),
                    **({"fbc": fbc} if fbc else {})
                },
                "custom_data": {}
            }]
        }
        data = {"data": json.dumps(payload["data"]), "access_token": META_ACCESS_TOKEN}
        if META_TEST_EVENT_CODE:
            data["test_event_code"] = META_TEST_EVENT_CODE
        resp = session.post(url, data=data, timeout=8)
        if resp.status_code >= 400:
            print("[meta] erro:", resp.status_code, resp.text[:200])
            return False
        return True
    except Exception as e:
        print("[meta] exceção:", e)
        return False

# ===================== GCS flush =====================
def _day_key(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts))

def _gcs_object_name(ts: int, part: int) -> str:
    d = _day_key(ts)
    return f"{GCS_PREFIX}date={d}/clicks_{d}_part-{part:04d}.csv"

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
    blob_name = _gcs_object_name(int(time.time()), part)
    _bucket.blob(blob_name).upload_from_string(data, content_type="text/csv")
    print(f"[FLUSH] {len(rows)} linha(s) → gs://{GCS_BUCKET}/{blob_name}")
    return len(rows)

def _background_flusher():
    while True:
        try:
            _flush_buffer_to_gcs()
        except Exception as e:
            print("[FLUSH-ERR]", e)
        time.sleep(FLUSH_MAX_SECONDS)

threading.Thread(target=_background_flusher, daemon=True).start()

# ===================== Rotas =====================
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET, "prefix": GCS_PREFIX,
            "video_id": VIDEO_ID, "model_loaded": bool(_model)}

@app.get("/robots.txt")
def robots():
    # evita que bots caiam na rota de redirect e poluam logs
    return PlainTextResponse("User-agent: *\nDisallow:\n", status_code=200)

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
    outer_uc = request.query_params.get("uc")

    ts = int(time.time())
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts))
    headers = request.headers
    cookie_header = headers.get("cookie") or headers.get("Cookie")
    referrer = headers.get("referer") or "-"
    user_agent = headers.get("user-agent", "-")
    fbclid = request.query_params.get("fbclid")
    ip_addr = (request.client.host if request.client else "0.0.0.0")
    device_name, os_family, os_version = parse_device_info(user_agent)

    # ===== UTM otimizada =====
    resolved_url = incoming_url
    utm_original = outer_uc or ""
    if not utm_original:
        utm_original = extract_utm_content_from_url(incoming_url)
    subids_in = {}
    if not utm_original:
        resolved_url, subids_in = resolve_short_link(incoming_url, max_hops=1)
        utm_original = subids_in.get("utm_content") or extract_utm_content_from_url(resolved_url)

    clean_base = re.sub(r'[^A-Za-z0-9]', '', utm_original or "") or "n"
    seq = incr_counter()
    utm_numbered = f"{clean_base}N{seq}"
    final_with_number = add_or_update_query_param(resolved_url, "utm_numbered", utm_numbered)

    # Short-link oficial Shopee com subIds[2] = utm_numbered (decodifica para evitar "invalid origin url")
    sub_id_api = sanitize_subid_for_shopee(utm_numbered)
    origin_url = unquote(final_with_number)
    dest = generate_short_link(origin_url, sub_id_api)

    # Snapshot em Redis
    fbp_cookie = get_cookie_value(cookie_header, "_fbp")
    fbc_val    = build_fbc_from_fbclid(fbclid, creation_ts=ts)
    r.setex(f"{USERDATA_KEY_PREFIX}{utm_numbered}", USERDATA_TTL_SECONDS, json.dumps({
        "ip": ip_addr, "ua": user_agent, "referrer": referrer,
        "event_source_url": final_with_number, "short_link": dest,
        "vc_time": ts, "utm_original": utm_original, "utm_numbered": utm_numbered,
        "fbclid": fbclid, "fbp": fbp_cookie, "fbc": fbc_val
    }))

    # ===== Geo + Predição (TODOS) =====
    pred_label = ""
    p_map = {"comprou": 0.0, "quase": 0.0, "desinteressado": 0.0}
    meta_sent = ""

    geo = geo_lookup(ip_addr)
    try:
        if _model is not None:
            feats = _features_for_model(os_family, device_name, os_version, referrer, utm_numbered, iso_time, geo)
            proba = list(_model.predict_proba([feats])[0])
            classes = list(getattr(_model, "classes_", ["comprou","desinteressado","quase"]))
            desired = ["comprou","desinteressado","quase"]
            order = [classes.index(c) for c in desired if c in classes]
            ordered_classes = [desired[i] for i in range(len(order))]
            proba = [proba[i] for i in order]
            for i, c in enumerate(ordered_classes):
                p_map[c] = float(proba[i])
            pred_label = predict_label(proba, ordered_classes)
            if pred_label == "comprou":
                ok = send_meta_event_add_to_cart(ip_addr, user_agent, fbp_cookie, fbc_val, final_with_number, ts)
                meta_sent = "AddToCart" if ok else "error"
    except Exception as e:
        print("[predict] erro:", e)

    # ===== Enfileira linha para GCS =====
    csv_row = [
        ts, iso_time, ip_addr, user_agent, device_name, os_family, os_version, referrer,
        incoming_url, resolved_url, final_with_number, (cat or ""),
        utm_original, utm_numbered,
        subids_in.get("sub_id1") or "", subids_in.get("sub_id2") or "",
        subids_in.get("sub_id3") or "", subids_in.get("sub_id4") or "", subids_in.get("sub_id5") or "",
        fbclid or "", fbp_cookie or "", fbc_val or "",
        geo.get("geo_status",""), geo.get("geo_country",""), geo.get("geo_region",""), geo.get("geo_state",""), geo.get("geo_city",""), geo.get("geo_zip",""),
        geo.get("geo_isp",""), geo.get("geo_org",""), geo.get("geo_asn",""), geo.get("geo_lat",""), geo.get("geo_lon",""),
        pred_label or "", p_map["comprou"], p_map["quase"], p_map["desinteressado"], meta_sent
    ]
    with _buffer_lock:
        _buffer_rows.append(csv_row)

    # Redirect imediato
    return RedirectResponse(dest, status_code=302)

# ===================== Admin =====================
@app.post("/admin/upload_model")
async def admin_upload_model(
    model_file: UploadFile = File(None),
    meta_file: UploadFile = File(None),
    # compatibilidade com script que usa nomes "model" / "meta"
    model: UploadFile = File(None),
    meta: UploadFile = File(None),
    x_admin_token: str = Header(None)
):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")

    up_model = model_file or model
    if not up_model:
        raise HTTPException(status_code=400, detail="model file ausente (campo model_file ou model)")

    try:
        with open(MODEL_PATH, "wb") as out:
            out.write(await up_model.read())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"falha ao salvar model: {e}")

    up_meta = meta_file or meta
    if up_meta:
        try:
            data = await up_meta.read()
            # valida JSON
            _ = json.loads(data.decode("utf-8"))
            with open(MODEL_META_PATH, "wb") as out:
                out.write(data)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"meta_file inválido: {e}")

    # recarrega
    _load_model_if_available()
    return {"ok": True, "saved": {"model": MODEL_PATH, "meta": (MODEL_META_PATH if up_meta else None)}, "model_loaded": bool(_model)}

@app.post("/admin/reload_model")
def admin_reload_model(x_admin_token: str = Header(None)):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    _load_model_if_available()
    return {"ok": True, "model_loaded": bool(_model)}

@app.get("/admin/flush")
def admin_flush(token: str):
    if token != ADMIN_TOKEN:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    sent = _flush_buffer_to_gcs()
    return {"ok": True, "sent_rows": sent}

@atexit.register
def _flush_on_exit():
    try:
        _flush_buffer_to_gcs()
    except Exception:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
