# -*- coding: utf-8 -*-

import os, time, hashlib, random, queue, threading
import requests
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse

META_PIXEL_ID     = os.getenv("FB_PIXEL_ID")
META_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
META_GRAPH_VER    = "v17.0"

WHATSAPP_TEST_NUMBER = "5548920013625"
WHATSAPP_TEST_TEXT   = "Quero o link"

app = FastAPI()
session = requests.Session()
q = queue.Queue()

# ---------------- WORKER (PRODUÇÃO) ----------------
def worker():
    while True:
        payload = q.get()
        try:
            session.post(
                f"https://graph.facebook.com/{META_GRAPH_VER}/{META_PIXEL_ID}/events",
                params={"access_token": META_ACCESS_TOKEN},
                json=payload,
                timeout=2
            )
        except:
            pass
        q.task_done()

threading.Thread(target=worker, daemon=True).start()

sha = lambda s: hashlib.sha256(s.encode()).hexdigest()

# ---------------- FILTRO BOT ----------------
BOT_UA = [
    "facebookexternalhit",
    "facebot",
    "metainspector",
    "adsbot",
    "crawler",
    "bot"
]

def is_bot(ua: str):
    if not ua:
        return False
    ua = ua.lower()
    return any(b in ua for b in BOT_UA)

# ---------------- ROTA PRINCIPAL ----------------
@app.get("/{p:path}")
def go(r: Request, p: str):

    # destino original
    d = r.query_params.get("link") or p
    if not d.startswith("http"):
        d = "https://" + d

    # 🔥 MODO TESTE (PRIORIDADE TOTAL)
    if r.query_params.get("test") == "1":
        wa_url = (
            f"https://wa.me/{WHATSAPP_TEST_NUMBER}"
            f"?text={WHATSAPP_TEST_TEXT.replace(' ', '%20')}"
        )
        return RedirectResponse(wa_url, status_code=302)

    # ---------------- PRODUÇÃO NORMAL ----------------

    h  = r.headers
    ua = h.get("user-agent","")
    ck = h.get("cookie")
    ip = r.client.host if r.client else "0.0.0.0"

    # filtro bot
    if is_bot(ua):
        return RedirectResponse(d,302)

    # só conta se clk=1
    if r.query_params.get("clk") != "1":
        return RedirectResponse(d,302)

    # cookies
    t   = int(time.time())
    fbp = (ck.split("_fbp=")[1].split(";")[0]
           if ck and "_fbp=" in ck
           else f"fb.1.{t}.{random.randint(10**15,10**16-1)}")

    fbc = (ck.split("_fbc=")[1].split(";")[0]
           if ck and "_fbc=" in ck
           else f"fb.1.{t}.{sha(fbp)[:16]}")

    eid = (ck.split("_eid=")[1].split(";")[0]
           if ck and "_eid=" in ck
           else sha(fbp))

    # fila de evento (produção)
    q.put_nowait({
        "data":[{
            "event_name":"ViewContent",
            "event_time":t,
            "event_id":eid,
            "action_source":"website",
            "event_source_url":d,
            "user_data":{
                "client_ip_address":ip,
                "client_user_agent":ua,
                "external_id":eid,
                "fbp":fbp,
                "fbc":fbc
            }
        }]
    })

    # redirect final
    resp = RedirectResponse(d,302)
    resp.set_cookie("_fbp", fbp, max_age=63072000, path="/", samesite="lax")
    resp.set_cookie("_fbc", fbc, max_age=63072000, path="/", samesite="lax")
    resp.set_cookie("_eid", eid, max_age=63072000, path="/", samesite="lax")
    return resp


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT","10000")),
        workers=4
    )
