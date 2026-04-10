import os
import time
import json
import hashlib
import random
import redis
import requests

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from urllib.parse import urlsplit, urlunsplit, unquote

app = FastAPI()

# =============================
# CONFIG
# =============================

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

SHOPEE_APP_ID = os.getenv("SHOPEE_APP_ID")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET")
SHOPEE_ENDPOINT = "https://open-api.affiliate.shopee.com.br/graphql"

META_PIXEL_ID = os.getenv("META_PIXEL_ID")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

# 🔥 CONTROLE DE SEQUÊNCIA
USE_SEQUENCE = os.getenv("USE_SEQUENCE", "true").lower() == "true"

r = redis.from_url(REDIS_URL)
session = requests.Session()

COUNTER_KEY = "utm_counter"

# =============================
# UTILS
# =============================

def sha256(s):
    return hashlib.sha256(s.encode()).hexdigest()

def gen_fbp(ts):
    return f"fb.1.{ts}.{random.randint(10**15,10**16-1)}"

def gen_fbc(fbp,ts):
    return f"fb.1.{ts}.{sha256(fbp)[:16]}"

def get_cookie(cookie_header,name):
    if not cookie_header:
        return None

    for c in cookie_header.split(";"):
        c=c.strip()
        if c.startswith(name+"="):
            return c.split("=")[1]

    return None

def next_number():
    return int(r.incr(COUNTER_KEY))

def set_utm(url,value):

    parts=urlsplit(url)
    query=parts.query.split("&") if parts.query else []

    replaced=False

    for i,q in enumerate(query):
        if q.startswith("utm_content="):
            query[i]="utm_content="+value
            replaced=True

    if not replaced:
        query.append("utm_content="+value)

    new_query="&".join(query)

    return urlunsplit((parts.scheme,parts.netloc,parts.path,new_query,parts.fragment))

# =============================
# SHOPEE SHORTLINK
# =============================

def generate_short_link(origin_url,subid):

    payload={
        "query":f"""
        mutation {{
            generateShortLink(
                input:{{
                    originUrl:"{origin_url}",
                    subIds:["","","{subid}","",""]
                }}
            ){{
                shortLink
            }}
        }}
        """
    }

    payload=json.dumps(payload,separators=(',',':'))

    ts=str(int(time.time()))

    signature=sha256(SHOPEE_APP_ID+ts+payload+SHOPEE_APP_SECRET)

    headers={
        "Authorization":f"SHA256 Credential={SHOPEE_APP_ID}, Timestamp={ts}, Signature={signature}",
        "Content-Type":"application/json"
    }

    resp=session.post(SHOPEE_ENDPOINT,data=payload,headers=headers)

    try:
        j=resp.json()
    except:
        print("Erro resposta Shopee:", resp.text)
        raise Exception("Erro Shopee")

    if "data" not in j:
        print("Erro Shopee:", j)
        raise Exception("Shopee API error")

    return j["data"]["generateShortLink"]["shortLink"]

# =============================
# META EVENTS
# =============================

def send_viewcontent(data):

    url=f"https://graph.facebook.com/v17.0/{META_PIXEL_ID}/events"

    payload={
        "data":[
            {
                "event_name":"ViewContent",
                "event_time":int(time.time()),
                "action_source":"website",
                "user_data":{
                    "client_ip_address":data["ip"],
                    "client_user_agent":data["ua"],
                    "fbp":data["fbp"],
                    "fbc":data["fbc"]
                },
                "custom_data":{
                    "currency":"BRL",
                    "value":0
                }
            }
        ]
    }

    params={"access_token":META_ACCESS_TOKEN}

    session.post(url,params=params,json=payload)

def send_purchase(data):

    url=f"https://graph.facebook.com/v17.0/{META_PIXEL_ID}/events"

    payload={
        "data":[
            {
                "event_name":"Purchase",
                "event_time":int(time.time()),
                "event_id":data["utm"],
                "action_source":"website",
                "user_data":{
                    "client_ip_address":data["ip"],
                    "client_user_agent":data["ua"],
                    "fbp":data["fbp"],
                    "fbc":data["fbc"]
                },
                "custom_data":{
                    "currency":"BRL",
                    "value":1
                }
            }
        ]
    }

    params={"access_token":META_ACCESS_TOKEN}

    session.post(url,params=params,json=payload)

# =============================
# PURCHASE
# =============================

@app.get("/send_purchase")
def purchase(utm:str=None):

    if not utm:
        return {"error":"missing utm"}

    data=r.get(f"click:{utm}")

    if not data:
        return {"status":"utm_not_found","utm":utm}

    data=json.loads(data)

    send_purchase(data)

    return {"status":"purchase sent"}

# =============================
# CLICK HANDLER
# =============================

@app.get("/{full_path:path}")
def click(request:Request,full_path:str):

    link=request.query_params.get("link")

    if not link and full_path.startswith("http"):
        link=full_path

    if not link:
        raise HTTPException(400,"missing link")

    link=unquote(link)

    ts=int(time.time())

    cookie=request.headers.get("cookie")
    ip=request.client.host
    ua=request.headers.get("user-agent","")

    fbclid=request.query_params.get("fbclid")

    fbp=get_cookie(cookie,"_fbp") or gen_fbp(ts)
    fbc=get_cookie(cookie,"_fbc") or gen_fbc(fbp,ts)

    if fbclid:
        fbc=f"fb.1.{ts}.{fbclid}"

    # 🔥 NOVO BLOCO
    uc = request.query_params.get("uc", "default")
    pos = request.query_params.get("pos", "Unknown").capitalize()

    if USE_SEQUENCE:
        n = next_number()
        utm = f"{uc}_{pos}_R{n}"
    else:
        utm = f"{uc}_{pos}"

    origin_url = set_utm(link, utm)

    try:
        short = generate_short_link(origin_url, utm)
    except Exception as e:
        print("Erro shortlink:", e)
        return JSONResponse({"error":"shopee_link_error"})

    data={
        "utm":utm,
        "ip":ip,
        "ua":ua,
        "fbp":fbp,
        "fbc":fbc
    }

    r.setex(f"click:{utm}",604800,json.dumps(data))

    send_viewcontent(data)

    resp=RedirectResponse(short)

    resp.set_cookie("_fbp",fbp,max_age=63072000)
    resp.set_cookie("_fbc",fbc,max_age=63072000)

    return resp

# =============================

if __name__=="__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=8000)
