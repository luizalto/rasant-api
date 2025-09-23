# -*- coding: utf-8 -*-
"""
FastAPI – Shopee ShortLink otimizado + Redis + GCS + Geo (IP) + Predição (CatBoost) + Meta Ads
- Geolocaliza IP e PREDIZ para TODOS os cliques.
- Sempre envia ViewContent para o Meta.
- Se predição == "comprou": envia AddToCart para o Meta.
"""

import os, re, time, csv, io, threading, urllib.parse, atexit, hashlib, json
from typing import Optional, Dict, Tuple, List, Any

import requests
import redis
from fastapi import FastAPI, Request, Query, Path, UploadFile, File, Header, HTTPException, Form
from fastapi.responses import RedirectResponse, JSONResponse, PlainTextResponse, HTMLResponse
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
GEO_CACHE_TTL_SECONDS= int(os.getenv("GEO_CACHE_TTL_SECONDS", "259200"))
GEO_CACHE_PREFIX     = os.getenv("GEO_CACHE_PREFIX", "geo:")

VIDEO_ID = os.getenv("VIDEO_ID", "v15")

# Shopee Affiliate
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "18314810331")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "LO3QSEG45TYP4NYQBRXLA2YYUL3ZCUPN")
SHOPEE_ENDPOINT   = os.getenv("SHOPEE_ENDPOINT", "https://open-api.affiliate.shopee.com.br/graphql")

# Meta Ads (CAPI)
META_PIXEL_ID       = os.getenv("META_PIXEL_ID")
META_ACCESS_TOKEN   = os.getenv("META_ACCESS_TOKEN")
META_GRAPH_VERSION  = os.getenv("META_GRAPH_VERSION", "v17.0")
META_TEST_EVENT_CODE= os.getenv("META_TEST_EVENT_CODE")

# Modelos
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
    "geo_status","geo_country","geo_region","geo_state","geo_city","geo_zip",
    "geo_isp","geo_org","geo_asn","geo_lat","geo_lon",
    "pred_label","p_comprou","p_quase","p_desinteressado",
    "meta_event_sent","meta_view_sent"
]

# ===================== Helpers =====================
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

# ===================== Meta Ads (CAPI unificado) =====================
def send_fb_event(event_name: str, event_time: int, event_source_url: str,
                  ip: str, user_agent: str, fbp: Optional[str], fbc: Optional[str]) -> bool:
    if not (META_PIXEL_ID and META_ACCESS_TOKEN):
        return False
    try:
        url = f"https://graph.facebook.com/{META_GRAPH_VERSION}/{META_PIXEL_ID}/events"
        payload = {
            "data": [{
                "event_name": event_name,
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
        params = {"access_token": META_ACCESS_TOKEN}
        if META_TEST_EVENT_CODE:
            payload["test_event_code"] = META_TEST_EVENT_CODE
        resp = session.post(url, json=payload, params=params, timeout=8)
        if resp.status_code >= 400:
            print("[meta] erro:", resp.status_code, resp.text[:200])
            return False
        return True
    except Exception as e:
        print("[meta] exceção:", e)
        return False

# ===================== Modelo CatBoost =====================
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

def predict_label(proba: List[float], classes: List[str]) -> str:
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
        j = int(max(range(len(proba)), key=lambda i: proba[i]))
        return classes[j]

_load_model_if_available()

# ===================== Rotas =====================
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time()), "bucket": GCS_BUCKET, "prefix": GCS_PREFIX,
            "video_id": VIDEO_ID, "model_loaded": bool(_model)}

@app.get("/admin/test")
def admin_test_event():
    ts = int(time.time())
    ok = send_fb_event("ViewContent", ts, "https://teste.com", "127.0.0.1", "TestAgent/1.0", None, None)
    return {"ok": ok, "ts": ts}

# ===================== Flush on exit =====================
@atexit.register
def _flush_on_exit():
    try:
        print("[flush] encerrando servidor")
    except Exception:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
