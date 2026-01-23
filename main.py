# -*- coding: utf-8 -*-

import os, time, json, hashlib, string, secrets, requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()
session = requests.Session()

# =====================
# CONFIG
# =====================

# WhatsApp
WHATSAPP_VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN")
WHATSAPP_TOKEN        = os.getenv("WHATSAPP_TOKEN")
PHONE_NUMBER_ID       = os.getenv("PHONE_NUMBER_ID")

# Meta
META_PIXEL_ID      = os.getenv("META_PIXEL_ID")
META_ACCESS_TOKEN  = os.getenv("META_ACCESS_TOKEN")
META_GRAPH_VERSION = os.getenv("META_GRAPH_VERSION", "v17.0")

# Shopee
SHOPEE_APP_ID     = os.getenv("SHOPEE_APP_ID")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET")
SHOPEE_ENDPOINT   = os.getenv("SHOPEE_ENDPOINT")

PRODUCTS = {
    "capa": {
        "base_code": "190teste",
        "origin_url": "https://shopee.com.br/SEU_LINK_CAPA"
    },
    "sofa": {
        "base_code": "191teste",
        "origin_url": "https://shopee.com.br/SEU_LINK_SOFA"
    }
}

# =====================
# HELPERS
# =====================

def random_7():
    chars = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(chars) for _ in range(7))

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()

def gerar_shortlink_shopee(origin_url: str, subid: str) -> str:
    payload_obj = {
        "query": (
            "mutation{generateShortLink(input:{"
            f"originUrl:\"{origin_url}\","
            f"subIds:[\"\",\"\",\"{subid}\",\"\",\"\"]"
            "}){shortLink}}"
        )
    }

    payload = json.dumps(payload_obj, separators=(",", ":"))
    ts = str(int(time.time()))
    signature = hashlib.sha256(
        (SHOPEE_APP_ID + ts + payload + SHOPEE_APP_SECRET).encode("utf-8")
    ).hexdigest()

    headers = {
        "Authorization": f"SHA256 Credential={SHOPEE_APP_ID}, Timestamp={ts}, Signature={signature}",
        "Content-Type": "application/json"
    }

    resp = session.post(SHOPEE_ENDPOINT, headers=headers, data=payload, timeout=10)
    j = resp.json()
    short = (((j or {}).get("data") or {}).get("generateShortLink") or {}).get("shortLink")

    if not short:
        raise Exception(f"Erro Shopee: {j}")

    return short

def enviar_evento_meta(phone: str, external_id: str):
    payload = {
        "data": [{
            "event_name": "AddToCart",
            "event_time": int(time.time()),
            "event_id": external_id,
            "action_source": "business_messaging",
            "user_data": {
                "ph": [sha256(phone)],
                "external_id": external_id
            }
        }]
    }

    session.post(
        f"https://graph.facebook.com/{META_GRAPH_VERSION}/{META_PIXEL_ID}/events",
        params={"access_token": META_ACCESS_TOKEN},
        json=payload,
        timeout=5
    )

def enviar_whatsapp(phone: str, shortlink: str):
    url = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": "Clique abaixo para abrir na Shopee 👇"},
            "action": {
                "buttons": [{
                    "type": "url",
                    "title": "Abrir Shopee",
                    "url": shortlink
                }]
            }
        }
    }
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    session.post(url, headers=headers, json=payload, timeout=5)

# =====================
# WEBHOOK VERIFY
# =====================

@app.get("/webhook")
def verify(req: Request):
    if (
        req.query_params.get("hub.mode") == "subscribe"
        and req.query_params.get("hub.verify_token") == WHATSAPP_VERIFY_TOKEN
    ):
        return PlainTextResponse(req.query_params.get("hub.challenge"))
    return PlainTextResponse("forbidden", status_code=403)

# =====================
# WEBHOOK WHATSAPP
# =====================

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()

    try:
        msg = data["entry"][0]["changes"][0]["value"]["messages"][0]
        phone = msg["from"]
        text = msg["text"]["body"].lower()
    except Exception:
        return PlainTextResponse("ignored", status_code=200)

    if "capa" in text:
        product = "capa"
    elif "sofa" in text or "sofá" in text:
        product = "sofa"
    else:
        product = "capa"

    base = PRODUCTS[product]
    utm_code = base["base_code"] + random_7()

    shortlink = gerar_shortlink_shopee(
        base["origin_url"],
        utm_code
    )

    # 🔥 envia evento Meta já colando telefone ↔ UTM
    enviar_evento_meta(phone, utm_code)

    # 🔥 responde WhatsApp com botão
    enviar_whatsapp(phone, shortlink)

    return PlainTextResponse("ok", status_code=200)
