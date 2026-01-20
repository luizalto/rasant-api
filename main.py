# -*- coding: utf-8 -*-
import os, time, hashlib, random
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
import requests

# ================= CONFIG =================

META_PIXEL_ID     = os.getenv("META_PIXEL_ID")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
META_GRAPH_VER    = "v17.0"

# ================= APP =================

app = FastAPI(title="Meta CAPI Redirect Only")

session = requests.Session()

# ================= UTILS =================

def _sha256(s: str):
    return hashlib.sha256(s.encode()).hexdigest()

def gen_fbp(ts):
    return f"fb.1.{ts}.{random.randint(10**15,10**16-1)}"

def gen_fbc_from_fbp(fbp, ts):
    return f"fb.1.{ts}.{_sha256(fbp)[:16]}"

def get_cookie(cookie_header, name):
    if not cookie_header:
        return None
    for c in cookie_header.split(";"):
        c = c.strip()
        if c.startswith(name+"="):
            return c.split("=",1)[1]
    return None

def safe_json(resp):
    try:
        return resp.json()
    except:
        return {"text": resp.text[:300]}

# ================= META =================

def send_meta_view(event_id, ts, url, ip, ua, fbp, fbc, eid):

    if not META_PIXEL_ID or not META_ACCESS_TOKEN:
        print("[META] Credenciais ausentes")
        return

    payload = {
        "data":[{
            "event_name":"ViewContent",
            "event_time":ts,
            "event_id":event_id,
            "action_source":"website",
            "event_source_url":url,
            "user_data":{
                "client_ip_address":ip,
                "client_user_agent":ua,
                "external_id":eid,
                "fbp":fbp,
                "fbc":fbc
            }
        }]
    }

    url_api = f"https://graph.facebook.com/{META_GRAPH_VER}/{META_PIXEL_ID}/events"

    r = session.post(
        url_api,
        params={"access_token":META_ACCESS_TOKEN},
        json=payload,
        timeout=8
    )

    print("[META] status:", r.status_code, safe_json(r))


# ================= ROTA =================

@app.get("/{full_path:path}")
def track_and_redirect(request: Request, full_path: str):

    # link original
    link = request.query_params.get("link")
    if link:
        dest = link
    else:
        dest = "https://" + full_path if not full_path.startswith("http") else full_path

    if not dest.startswith("http"):
        raise HTTPException(400, "link inválido")

    ts = int(time.time())

    headers = request.headers
    ua = headers.get("user-agent","-")
    cookie = headers.get("cookie")

    ip = request.client.host if request.client else "0.0.0.0"

    # cookies
    fbp = get_cookie(cookie,"_fbp") or gen_fbp(ts)
    fbc = get_cookie(cookie,"_fbc") or gen_fbc_from_fbp(fbp,ts)
    eid = get_cookie(cookie,"_eid") or _sha256(fbp)

    event_id = eid  # simples

    # ENVIA META
    send_meta_view(
        event_id=event_id,
        ts=ts,
        url=dest,
        ip=ip,
        ua=ua,
        fbp=fbp,
        fbc=fbc,
        eid=eid
    )

    # REDIRECT
    resp = RedirectResponse(dest, status_code=302)
    resp.headers["Cache-Control"] = "no-store"

    resp.set_cookie("_fbp", fbp, max_age=63072000, path="/", samesite="lax")
    resp.set_cookie("_fbc", fbc, max_age=63072000, path="/", samesite="lax")
    resp.set_cookie("_eid", eid, max_age=63072000, path="/", samesite="lax")

    return resp


# ================= RUN =================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
