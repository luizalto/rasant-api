# main.py
# -*- coding: utf-8 -*-

import os
import re
import io
import csv
import time
import json
import uuid
import hashlib
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

import requests
import redis
from fastapi import FastAPI, Request, Path, Query
from fastapi.responses import RedirectResponse, JSONResponse

# =============================================================================
# CONFIG
# =============================================================================
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379/0")
UTM_COUNTER_KEY = os.getenv("UTM_COUNTER_KEY", "utm_counter_global")

SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID", "")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET", "")
SHOPEE_ENDPOINT   = "https://open-api.affiliate.shopee.com.br/graphql"

GCS_BUCKET_NAME   = os.getenv("GCS_BUCKET_NAME", "")
GCS_PREFIX        = os.getenv("GCS_PREFIX", "logs")
GCS_SA_KEY_FILE   = os.getenv("GCS_SA_KEY_FILE", "")

FLUSH_EVERY_N       = int(os.getenv("FLUSH_EVERY_N", "50"))
FLUSH_EVERY_SECONDS = int(os.getenv("FLUSH_EVERY_SECONDS", "60"))
EXPAND_TIMEOUT_SEC  = float(os.getenv("EXPAND_TIMEOUT_SEC", "6.0"))

# =============================================================================
# Redis client
# =============================================================================
r = redis.from_url(REDIS_URL, decode_responses=True)

# =============================================================================
# GCS lazy client
# =============================================================================
_gcs_client = None
_gcs_bucket = None
_click_buffer: List[Dict[str, Any]] = []
_last_flush_ts = time.time()


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


# =============================================================================
# Helpers
# =============================================================================
def _now_ts() -> int:
    return int(time.time())


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")


def _incr_counter() -> int:
    try:
        return int(r.incr(UTM_COUNTER_KEY))
    except Exception as e:
        print("[Redis] ERRO no incr:", e)
        return int(time.time() * 1000)  # fallback: timestamp


def _append_n_to_utms(url: str, suffix: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    utm_keys = [k for k in q.keys() if k.lower().startswith("utm_")]
    if utm_keys:
        for k in utm_keys:
            vals = q.get(k, [])
            new_vals = []
            for v in vals:
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
    try:
        resp = requests.get(url, allow_redirects=True, timeout=EXPAND_TIMEOUT_SEC, headers={"User-Agent": "Mozilla/5.0"})
        return resp.url or url
    except Exception:
        return url


def _generate_shopee_shortlink(origin_url: str, utm_content: str) -> str:
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


def _flush_if_needed(force: bool = False):
    global _click_buffer, _last_flush_ts
    if not GCS_BUCKET_NAME:
        return
    bucket = _get_gcs_bucket()
    if bucket is None:
        return

    do_flush = force or (len(_click_buffer) >= FLUSH_EVERY_N) or (time.time() - _last_flush_ts >= FLUSH_EVERY_SECONDS)
    if not do_flush or not _click_buffer:
        return

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    part_id = str(uuid.uuid4())[:8]
    object_name = f"{GCS_PREFIX}/date={date_str}/clicks_{date_str}_{part_id}.csv"

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(_click_buffer[0].keys()), lineterminator="\n")
    writer.writeheader()
    for row in _click_buffer:
        writer.writerow(row)
    data = buf.getvalue()

    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type="text/csv")

    print(f"[FLUSH] {len(_click_buffer)} → gs://{GCS_BUCKET_NAME}/{object_name}")
    _click_buffer = []
    _last_flush_ts = time.time()


def _add_log_row(row: Dict[str, Any]):
    _click_buffer.append(row)
    _flush_if_needed(False)


# =============================================================================
# FastAPI
# =============================================================================
app = FastAPI(title="Shopee Redirector com UTM + Redis + ShortLink")


@app.get("/health")
def health():
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {"ok": True, "time": _iso_now(), "redis": redis_ok, "gcs_bucket": GCS_BUCKET_NAME or None}


@app.get("/{full_url:path}")
def redirect_with_dynamic_utm(request: Request, full_url: str = Path(...)):
    if not (full_url.startswith("http://") or full_url.startswith("https://")):
        full_url = urllib.parse.unquote(full_url)

    origin_short = full_url
    expanded = _expand_shortlink(origin_short)

    # contador infinito
    n = _incr_counter()
    suffix = f"n{n}"

    # acrescenta N
    final_with_n = _append_n_to_utms(expanded, suffix)

    # pega utm_content final
    parsed = urllib.parse.urlsplit(final_with_n)
    q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    utm_content = (q.get("utm_content", [suffix])[0])

    # gera shortlink Shopee
    try:
        new_short = _generate_shopee_shortlink(final_with_n, utm_content)
    except Exception as e:
        print("[ShopeeShortLink] ERRO:", e)
        new_short = final_with_n

    # log
    try:
        ua = request.headers.get("user-agent", "-")
        ip = request.client.host if request.client else "-"
        row = {
            "timestamp": _now_ts(),
            "iso_time": _iso_now(),
            "ip": ip,
            "ua": ua,
            "origin_short": origin_short,
            "expanded": expanded,
            "final_with_n": final_with_n,
            "utm_content": utm_content,
        }
        _add_log_row(row)
    except Exception as e:
        print("[LOG] erro:", e)

    _flush_if_needed(False)
    return RedirectResponse(new_short, status_code=302)


@app.get("/admin/flush")
def admin_flush():
    _flush_if_needed(True)
    return {"ok": True, "buffer": len(_click_buffer)}
