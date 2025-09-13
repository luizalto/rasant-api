# main.py
# -*- coding: utf-8 -*-

import os
import json
import time
import hashlib
import urllib.parse
from typing import List, Tuple

import requests
import redis
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse

# ───────────────────────── CONFIG ─────────────────────────
REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379/0")
COUNTER_KEY = os.getenv("UTM_COUNTER_KEY", "utm_counter_path_style")

# Shopee Affiliate (shortlink)
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "18314810331")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "LO3QSEG45TYP4NYQBRXLA2YYUL3ZCUPN")
SHOPEE_ENDPOINT   = "https://open-api.affiliate.shopee.com.br/graphql"

DEFAULT_UA = "Mozilla/5.0 (compatible; RasantBot/1.0; +https://rasant-api.onrender.com)"

# ───────────────────────── APP / REDIS ─────────────────────────
r = redis.from_url(REDIS_URL)
app = FastAPI(title="Rasant Path Redirector with UTM n-suffix")

# ───────────────────────── HELPERS ─────────────────────────
def incr_counter_zero_based() -> int:
    return int(r.incr(COUNTER_KEY)) - 1

def expand_url(url: str, timeout: float = 15.0) -> str:
    try:
        resp = requests.get(url, allow_redirects=True, timeout=timeout, headers={"User-Agent": DEFAULT_UA})
        return resp.url
    except Exception:
        return url

def parse_query(url: str) -> Tuple[urllib.parse.SplitResult, List[Tuple[str, str]]]:
    parsed = urllib.parse.urlsplit(url)
    params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    return parsed, params

def rebuild_url(parsed: urllib.parse.SplitResult, params: List[Tuple[str, str]]) -> str:
    new_query = urllib.parse.urlencode(params, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))

def append_suffix_to_utms(raw_url: str, suffix: str) -> str:
    parsed, params = parse_query(raw_url)
    has_utm = False
    new_params: List[Tuple[str, str]] = []
    for k, v in params:
        if k.lower().startswith("utm_") and v:
            has_utm = True
            new_params.append((k, f"{v}{suffix}"))
        else:
            new_params.append((k, v))
    # Se não tem nenhuma UTM → não inventa nada
    return rebuild_url(parsed, new_params)

def extract_utm_content(raw_url: str) -> str:
    _, params = parse_query(raw_url)
    for k, v in params:
        if k.lower() == "utm_content":
            return v
    return ""

def generate_shopee_shortlink(origin_url: str, utm_content: str) -> str:
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
        "User-Agent": DEFAULT_UA,
    }
    resp = requests.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()["data"]["generateShortLink"]["shortLink"]

def clean_target_from_path(target: str) -> str:
    url = urllib.parse.unquote(target).strip()
    if url.startswith("https://rasant-api.onrender.com"):
        url = url.replace("https://rasant-api.onrender.com", "", 1).lstrip("/")
    if not (url.startswith("http://") or url.startswith("https://")):
        url = "https://" + url.lstrip("/")
    return url

# ───────────────────────── ROUTES ─────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/{target:path}")
def redirect_with_suffix(request: Request, target: str):
    incoming_url = clean_target_from_path(target)
    final_url = expand_url(incoming_url)

    # contador e sufixo
    n = incr_counter_zero_based()
    suffix = f"n{n}"

    # aplica sufixo nas UTMs
    enriched = append_suffix_to_utms(final_url, suffix)

    # extrai utm_content (se houver)
    utm_content_final = extract_utm_content(enriched)

    # gera shortlink
    try:
        short = generate_shopee_shortlink(enriched, utm_content_final or suffix)
        dest = short
    except Exception as e:
        print(f"[ShopeeShortLink][ERRO] {e}. Usando URL enriquecida.")
        dest = enriched

    # log
    print(json.dumps({
        "ip": (request.client.host if request.client else "-"),
        "ua": request.headers.get("user-agent", "-")[:160],
        "n": n,
        "incoming": incoming_url,
        "expanded": final_url,
        "enriched": enriched,
        "utm_content": utm_content_final,
        "dest": dest,
    }, ensure_ascii=False))

    return RedirectResponse(dest, status_code=302)

# ───────────────────────── START ─────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)
