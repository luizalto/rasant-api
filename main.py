# -*- coding: utf-8 -*-

import os, time, hashlib, random, queue, threading
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse

# ================= CONFIG =================

META_PIXEL_ID     = os.getenv("FB_PIXEL_ID")
META_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
META_GRAPH_VER    = "v17.0"

if not META_PIXEL_ID or not META_ACCESS_TOKEN:
    print("[ERRO] Configure FB_PIXEL_ID e FB_ACCESS_TOKEN nas variáveis de ambiente")

# ================= APP =================

app = FastAPI(title="Meta CAPI Redirect")

session = requests.Session()

# ================= FILA =================

event_queue = queue.Queue()

def meta_worker():
    while True:
        payload = event_queue.get()
        try:
            url = f"https://graph.facebook.com/{META_GRAPH_VER}/{META_PIXEL_ID}/events"
            session.post(
                url,
                params={"access_token": META_ACCESS_TOKEN},
                json=payload,
                timeout=6
            )
        except Exception as e:
            print("[META] erro:", e)
        finally:
            event_queue.task_done()

threading.Thread(target=meta_worker, daemon=True).start()

# ================= UTILS =================

def sha256(s: str):
    return hashlib.sha256(s.encode()).hexdigest()

def gen_fbp(ts):
    return f"fb.1.{ts}.{random.randint(10**15,10**16-1)}"

def gen_fbc(fbp, ts):
    return f"fb.1.{ts}.{sha256(fbp)[:16]}"

def get_cookie(cookie_header, name):
    if not cookie_header:
        return None
    for c in cookie_header.split(";"):
        c = c.strip()
        if c.startswith(name + "="):
            return c.split("=",1)[1]
    return None

# ================= META =================

def enqueue_meta_view(payload):
    try:
        event_queue.put_nowait(payload)
    except Exception:
        pass

# ================= ROTA =================

@app.get("/{path:path}")
def track_and_redirect(request: Request, path: str):

    # pega link original
    dest = request.query_params.get("link")
    if not dest:
        if not path:
            raise HTTPException(400, "link ausente")
        dest = path
        if not dest.startswith("http"):
            dest = "https://" + dest

    if not dest.startswith("http"):
        raise HTTPException(400, "link inválido")

    ts = int(time.time())
    headers = request.headers

    ua = headers.get("user-agent","-")
    cookie = headers.get("cookie")
    ip = request.client.host if request.client else "0.0.0.0"

    # cookies
    fbp = get_cookie(cookie,"_fbp") or gen_fbp(ts)
    fbc = get_cookie(cookie,"_fbc") or gen_fbc(fbp,ts)
    eid = get_cookie(cookie,"_eid") or sha256(fbp)

    # payload meta
    payload = {
        "data":[{
            "event_name":"ViewContent",
            "event_time":ts,
            "event_id":eid,
            "action_source":"website",
            "event_source_url":dest,
            "user_data":{
                "client_ip_address":ip,
                "client_user_agent":ua,
                "external_id":eid,
                "fbp":fbp,
                "fbc":fbc
            }
        }]
    }

    # fila (assíncrono)
    enqueue_meta_view(payload)

    # redirect
    resp = RedirectResponse(dest, status_code=302)
    resp.headers["Cache-Control"] = "no-store"

    resp.set_cookie("_fbp", fbp, max_age=63072000, path="/", samesite="lax")
    resp.set_cookie("_fbc", fbc, max_age=63072000, path="/", samesite="lax")
    resp.set_cookie("_eid", eid, max_age=63072000, path="/", samesite="lax")

    return resp


# ================= RUN =================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT","10000")),
        reload=False
    )
