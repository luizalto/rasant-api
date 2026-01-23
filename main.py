# -*- coding: utf-8 -*-

import os, time, hashlib, random, queue, threading
import requests
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, PlainTextResponse

META_PIXEL_ID     = os.getenv("FB_PIXEL_ID")
META_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
META_GRAPH_VER    = "v17.0"

# 🔑 TOKEN DE VERIFICAÇÃO DO WHATSAPP (ENV)
WHATSAPP_VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN")

app = FastAPI()
session = requests.Session()
q = queue.Queue()

# -------- WORKER --------
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

# -------- FILTROS --------
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

# -------- WEBHOOK WHATSAPP --------
@app.get("/webhook")
def verify_webhook(r: Request):
    mode      = r.query_params.get("hub.mode")
    token     = r.query_params.get("hub.verify_token")
    challenge = r.query_params.get("hub.challenge")

    if mode == "subscribe" and token == WHATSAPP_VERIFY_TOKEN:
        return PlainTextResponse(challenge, status_code=200)

    return PlainTextResponse("Verification failed", status_code=403)


@app.post("/webhook")
async def receive_webhook(r: Request):
    # Por enquanto não faz nada com a mensagem
    # Só recebe para não dar erro no Meta
    return {"status": "ok"}

# -------- ROTA PRINCIPAL --------
@app.get("/{p:path}")
def go(r: Request, p: str):

    # destino
    d = r.query_params.get("link") or p
    if not d.startswith("http"):
        d = "https://" + d

    # headers
    h  = r.headers
    ua = h.get("user-agent","")
    ck = h.get("cookie")
    ip = r.client.host if r.client else "0.0.0.0"

    # 🔒 FILTRO 1: BOT
    if is_bot(ua):
        return RedirectResponse(d,302)

    # 🔒 FILTRO 2: SÓ CONTA SE clk=1
    if r.query_params.get("clk") != "1":
        return RedirectResponse(d,302)

    # cookies
    t   = int(time.time())
    fbp = (ck.split("_fbp=")[1].split(";")[0] if ck and "_fbp=" in ck else f"fb.1.{t}.{random.randint(10**15,10**16-1)}")
    fbc = (ck.split("_fbc=")[1].split(";")[0] if ck and "_fbc=" in ck else f"fb.1.{t}.{sha(fbp)[:16]}")
    eid = (ck.split("_eid=")[1].split(";")[0] if ck and "_eid=" in ck else sha(fbp))

    # fila (evento real)
    q.put_nowait({
        "data":[
            {
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
            }
        ]
    })

    # redirect
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
