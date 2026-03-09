import os
import time
import json
import hashlib
import random
import re
import redis
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from urllib.parse import urlsplit, urlunsplit

app = FastAPI()

# CONFIG
REDIS_URL = os.getenv("REDIS_URL","redis://localhost:6379/0")

SHOPEE_APP_ID = os.getenv("SHOPEE_APP_ID")
SHOPEE_APP_SECRET = os.getenv("SHOPEE_APP_SECRET")
SHOPEE_ENDPOINT = "https://open-api.affiliate.shopee.com.br/graphql"

META_PIXEL_ID = os.getenv("META_PIXEL_ID")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")

VIDEO_ID = os.getenv("VIDEO_ID","v1")

r = redis.from_url(REDIS_URL)

session = requests.Session()

COUNTER_KEY = "utm_counter"

# ============================
# UTILS
# ============================

def sha256(s):
    return hashlib.sha256(s.encode()).hexdigest()

def gen_fbp(ts):
    return f"fb.1.{ts}.{random.randint(10**15,10**16-1)}"

def gen_fbc(fbp, ts):
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

# ============================
# SHOPEE SHORTLINK
# ============================

def generate_short_link(origin_url,subid):

    payload={
        "query":f"""
        mutation {{
            generateShortLink(
                input: {{
                    originUrl: "{origin_url}",
                    subIds:["","","{subid}","",""]
                }}
            ) {{
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

    j=resp.json()

    short=j["data"]["generateShortLink"]["shortLink"]

    return short

# ============================
# META PURCHASE EVENT
# ============================

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

# ============================
# CLICK HANDLER
# ============================

@app.get("/{path:path}")
def click(request:Request,path:str):

    link=request.query_params.get("link")

    if not link:
        raise HTTPException(400,"missing link")

    ts=int(time.time())

    cookie=request.headers.get("cookie")

    ip=request.client.host

    ua=request.headers.get("user-agent","")

    fbclid=request.query_params.get("fbclid")

    fbp=get_cookie(cookie,"_fbp") or gen_fbp(ts)

    fbc=get_cookie(cookie,"_fbc") or gen_fbc(fbp,ts)

    if fbclid:
        fbc=f"fb.1.{ts}.{fbclid}"

    # gerar UTM

    n=next_number()

    utm=f"{VIDEO_ID}n{n}"

    # inserir utm no link

    origin_url=set_utm(link,utm)

    # gerar shortlink

    short=generate_short_link(origin_url,utm)

    # salvar dados para purchase futuro

    r.setex(

        f"click:{utm}",

        604800,

        json.dumps({

            "utm":utm,
            "ip":ip,
            "ua":ua,
            "fbp":fbp,
            "fbc":fbc

        })

    )

    # redirect

    resp=RedirectResponse(short)

    resp.set_cookie("_fbp",fbp,max_age=63072000)

    resp.set_cookie("_fbc",fbc,max_age=63072000)

    return resp

# ============================
# PURCHASE ENDPOINT
# ============================

@app.get("/send_purchase")
def purchase(utm:str):

    data=r.get(f"click:{utm}")

    if not data:
        return {"error":"utm not found"}

    data=json.loads(data)

    send_purchase(data)

    return {"status":"sent"}

# ============================

if __name__=="__main__":

    import uvicorn

    uvicorn.run(app,host="0.0.0.0",port=8000)
