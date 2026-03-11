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

# PIXELS DISPONÍVEIS (1 a 10)

PIXELS = {
    "1": {"id": os.getenv("META_PIXEL_ID_PX1"), "token": os.getenv("META_ACCESS_TOKEN_PX1")},
    "2": {"id": os.getenv("META_PIXEL_ID_PX2"), "token": os.getenv("META_ACCESS_TOKEN_PX2")},
    "3": {"id": os.getenv("META_PIXEL_ID_PX3"), "token": os.getenv("META_ACCESS_TOKEN_PX3")},
    "4": {"id": os.getenv("META_PIXEL_ID_PX4"), "token": os.getenv("META_ACCESS_TOKEN_PX4")},
    "5": {"id": os.getenv("META_PIXEL_ID_PX5"), "token": os.getenv("META_ACCESS_TOKEN_PX5")},
    "6": {"id": os.getenv("META_PIXEL_ID_PX6"), "token": os.getenv("META_ACCESS_TOKEN_PX6")},
    "7": {"id": os.getenv("META_PIXEL_ID_PX7"), "token": os.getenv("META_ACCESS_TOKEN_PX7")},
    "8": {"id": os.getenv("META_PIXEL_ID_PX8"), "token": os.getenv("META_ACCESS_TOKEN_PX8")},
    "9": {"id": os.getenv("META_PIXEL_ID_PX9"), "token": os.getenv("META_ACCESS_TOKEN_PX9")},
    "10": {"id": os.getenv("META_ACCESS_TOKEN_PX10"), "token": os.getenv("META_ACCESS_TOKEN_PX10")}
}

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

    j=resp.json()

    if "data" not in j:
        raise Exception("Shopee API error")

    return j["data"]["generateShortLink"]["shortLink"]

# =============================
# META EVENTS
# =============================

def send_viewcontent(data,pixel):

    if not pixel or not pixel["id"]:
        return

    url=f"https://graph.facebook.com/v17.0/{pixel['id']}/events"

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

    params={"access_token":pixel["token"]}

    session.post(url,params=params,json=payload)

def send_purchase(data,pixel):

    if not pixel or not pixel["id"]:
        return

    url=f"https://graph.facebook.com/v17.0/{pixel['id']}/events"

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

    params={"access_token":pixel["token"]}

    session.post(url,params=params,json=payload)

# =============================
# PURCHASE ENDPOINT
# =============================

@app.get("/send_purchase")
def purchase(utm:str=None,px:str=None):

    if not utm:
        return {"error":"missing utm"}

    pixel=None

    if px:
        pixel = PIXELS.get(px)

    data=r.get(f"click:{utm}")

    if not data:
        return {"status":"utm_not_found"}

    data=json.loads(data)

    send_purchase(data,pixel)

    return {"status":"purchase_sent"}

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

    uc=request.query_params.get("uc")

    if not uc:
        uc="default"

    px=request.query_params.get("px")

    pixel=None

    if px:
        pixel=PIXELS.get(px)

    n=next_number()

    utm=f"{uc}R{n}"

    origin_url=set_utm(link,utm)

    try:

        short=generate_short_link(origin_url,utm)

    except Exception:

        return JSONResponse({"error":"shopee_link_error"})

    data={
        "utm":utm,
        "ip":ip,
        "ua":ua,
        "fbp":fbp,
        "fbc":fbc
    }

    r.setex(f"click:{utm}",604800,json.dumps(data))

    if pixel:
        send_viewcontent(data,pixel)

    resp=RedirectResponse(short)

    resp.set_cookie("_fbp",fbp,max_age=63072000)
    resp.set_cookie("_fbc",fbc,max_age=63072000)

    return resp


if __name__=="__main__":

    import uvicorn

    uvicorn.run(app,host="0.0.0.0",port=8000)
