import os
import time
import re
import csv
import requests

from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from dotenv import load_dotenv

load_dotenv()  # carrega FB_PIXEL_ID, FB_ACCESS_TOKEN e VIDEO_ID do .env

PIXEL_ID     = os.getenv("FB_PIXEL_ID")
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
VIDEO_ID     = int(os.getenv("VIDEO_ID", "15"))

if not PIXEL_ID or not ACCESS_TOKEN:
    raise RuntimeError("Defina FB_PIXEL_ID e FB_ACCESS_TOKEN no .env")

# Regex pra capturar utm_content sem mexer no resto da query
pattern = re.compile(r"(?:^|&)(utm_content)=([^&]*)")

# Contador na memória (reinicia ao reiniciar o servidor)
contador = 1

app = FastAPI()

def send_fb_event(event_name: str, event_id: str, url: str, ip: str, ua: str, currency: str = None, value: float = None):
    """
    Monta o payload exatamente no formato do Helper e envia ao Conversions API.
    """
    endpoint = f"https://graph.facebook.com/v15.0/{PIXEL_ID}/events?access_token={ACCESS_TOKEN}"
    data = {
        "data": [
            {
                "event_name":     event_name,
                "event_time":     int(time.time()),
                "action_source":  "website",
                "event_id":       event_id,
                "event_source_url": url,
                "user_data": {
                    "client_ip_address": ip,
                    "client_user_agent": ua
                }
            }
        ]
    }

    # Se for Purchase, adicione custom_data
    if event_name.lower() == "purchase" and currency and value is not None:
        data["data"][0]["custom_data"] = {
            "currency": currency,
            "value":    value
        }

    resp = requests.post(endpoint, json=data)
    if resp.status_code != 200:
        # Log simples em STDOUT, mas em prod use logger
        print("Erro Conversions API:", resp.status_code, resp.text)

@app.get("/{full_path:path}")
async def track_click(full_path: str, request: Request):
    """
    Intercepta cliques, substitui utm_content por v{VIDEO_ID}n{contador}----,
    dispara ViewContent ao Meta e redireciona para a Shopee.
    """
    global contador

    # 1) pega a query string crua (preserva ordem e & sem encoding)
    raw_qs = request.scope["query_string"].decode("utf-8")
    if not raw_qs:
        raise HTTPException(400, "Parâmetros faltando na URL")

    # 2) detecta utm_content
    m = pattern.search(raw_qs)
    if not m:
        raise HTTPException(400, "utm_content não encontrado")

    # 3) gera novo valor e incrementa
    novo_valor = f"v{VIDEO_ID}n{contador}----"
    contador += 1

    # 4) substitui somente o valor de utm_content
    new_qs = pattern.sub(lambda x: f"{x.group(1)}={novo_valor}", raw_qs)

    # 5) monta URL de destino
    destino = f"https://shopee.com.br/{full_path}?{new_qs}"

    # 6) dispara ViewContent server-side
    client_ip = request.client.host
    client_ua = request.headers.get("user-agent", "")
    send_fb_event(
        event_name="ViewContent",
        event_id=novo_valor,
        url=destino,
        ip=client_ip,
        ua=client_ua
    )

    # 7) redireciona o usuário
    return RedirectResponse(destino, status_code=307)

@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    """
    Recebe CSV com colunas utm_content,vendas[,valor]
    Envia Purchase pro Meta para cada utm_content com vendas>0.
    """
    content = await file.read()
    text    = content.decode("utf-8").splitlines()
    reader  = csv.DictReader(text)

    total = 0
    sent  = 0
    for row in reader:
        total += 1
        utm    = row.get("utm_content") or row.get("utm")
        vendas = int(row.get("vendas", "0"))
        valor  = row.get("valor") or row.get("value")
        if not utm:
            continue

        if vendas > 0:
            # dispara Purchase
            client_ip = "0.0.0.0"  # ou você pode gravar IPs no clique e resgatar aqui
            client_ua = ""         # idem para user-agent
            send_fb_event(
                event_name="Purchase",
                event_id=utm,
                url="",            # opcional: você pode repetir o event_source_url original
                ip=client_ip,
                ua=client_ua,
                currency="BRL",
                value=float(valor) if valor else 0.0
            )
            sent += 1

    return JSONResponse({
        "processed_rows": total,
        "purchases_sent": sent
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
