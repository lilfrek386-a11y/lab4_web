import ssl
import uvicorn
import httpx
import jwt
import asyncio
import json
import websockets
from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager
from typing import Dict, List

import schema_pb2

from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.primitives import serialization
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from fastapi.responses import RedirectResponse, HTMLResponse

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[WebSocket, List[str]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[websocket] = []

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            del self.active_connections[websocket]

    def subscribe(self, websocket: WebSocket, symbols: List[str]):
        self.active_connections[websocket] = symbols

    async def broadcast(self, symbol: str, price: str, event_time: int):
        proto_msg = schema_pb2.TickerData(symbol=symbol, price=price, event_time=event_time)
        binary_data = proto_msg.SerializeToString()

        for ws, subscriptions in list(self.active_connections.items()):
            if symbol in subscriptions:
                try:
                    await ws.send_bytes(binary_data)
                except Exception:
                    pass

manager = ConnectionManager()

async def binance_listener():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@ticker/ethusdt@ticker/bnbusdt@ticker/solusdt@ticker"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                while True:
                    data = await ws.recv()
                    msg = json.loads(data)
                    symbol = msg.get('s')
                    price = msg.get('c')
                    time = msg.get('E')
                    if symbol and price:
                        await manager.broadcast(symbol, price, time)
        except Exception as e:
            print(f"Зв'язок з Binance розірвано. Перепідключення... {e}")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(binance_listener())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)


CASDOOR_URL = "http://localhost:9000"
CLIENT_ID = "d02c3b75740b01aca48b"
CLIENT_SECRET = "437e86c5fd121253f888ae9f53212dc369a878bf"
REDIRECT_URI = "https://localhost:8000/callback"

CASDOOR_PUBLIC_CERT="""-----BEGIN CERTIFICATE-----
MIIE3TCCAsWgAwIBAgIDAeJAMA0GCSqGSIb3DQEBCwUAMCgxDjAMBgNVBAoTBWFk
bWluMRYwFAYDVQQDEw1jZXJ0LWJ1aWx0LWluMB4XDTI2MDQyNjE1MTAxN1oXDTQ2
MDQyNjE1MTAxN1owKDEOMAwGA1UEChMFYWRtaW4xFjAUBgNVBAMTDWNlcnQtYnVp
bHQtaW4wggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQCrZ6RIaZxbQbfR
vimPYerRjf7cpf3JT49WBWVUTWrfKjpfn/gGqDTDNW2sCZkaPJOANFmF41ZYQq26
gNxRb+vUr+rdwr4uZ7AkdpehdqneaWb8BG8RDbnLV5DCaJVSKhRKINexqt4iPNOe
Fft3hW6N3Ec+kMit9qYJz8yFaa/iD5PgpSQ6/aYuNI5aGt8EAqCYF7q7BoSYo18+
t4B0oybu1GwdX28ZxjzmyxHqWMPWZhDMcfOh881d6WXx7JX9zYnAERFq+RRhe3eb
QoPpTPsJPP6C7W7Yys783MG+pr+HxPvZ15pTSgnoKlphcbQFDhnyWJOSG4NICamZ
agLvAX4jddx7Is/xaZ1hTyXfQT+yc7sv5177hMDrK90rMVTAA8RJgwetYKBcvYmK
6dMRKycwQ7S9juSJac6QONvFQY4q8j9uOT7X2VPuDUMdRSKpn3Zud5DDtRNonPrG
/d1Nwumobq8hlsBmGLIAmjtq4Q57lrzbI9L9HN9TGueVdMSehs/mVdJVn3NHzAGa
TeGthVc6qUVwD+Bojr1DooTou/mfI6VUqBzc9lFwxoEao6YV9qDIfjV0NpjbVwIu
W6BC9iVtfPw3VX7X7c3M4ZTO6FE1EdnAqdDPoaxIzYHJsqfJVTOzI2i+HSBOrZae
a0tFSEnnUylnRq/W98iXLeK7bkG+AQIDAQABoxAwDjAMBgNVHRMBAf8EAjAAMA0G
CSqGSIb3DQEBCwUAA4ICAQBu1YP+bue81ZQ9OrKUi1DKmdTtf46o3WXUBOBuiPo+
NhusAMlRfTKv3y8cjnDcdPGbr0WcEyweHBUyf4c1Rb0qus2fIDtHLD2DZD0owwPs
TQwe/pJ8Wrr1R50aWnLdleta5vOC653DzDv0pRW5vVCEzGwYFGDhSv6j0iOyju+D
7GoVFv5kNMdNvYwizhOuBI1FM7kUTsXpEBy2hEt9hFeA8OiCgterJ5Frly0i2Uaf
J6l5qYw+18JVmE1BubKZbXWPmECPNwfomQQ2D6RSLTrdxddjJ6sxc90iZ4zSaH1x
zfHbsV6Echj5FO5CoXQaZxHDgd5j0fy6wbAiD4JywNAhHF46VhsOyKC1Lw3C5GTq
uKTxpky1YN2NDIP+5v643f5gy2AHOUczbilFnoT2Pzgjfpc0TG9fazDtFnbohXzS
M1liX5aiHGegd4beD3eZQkEhogkovQmD76hF/jgOYLojKg/hIVlY5Fa68GP4FuYA
h/0NaWkS5ke3h8iE+IWHcCsu9W1oMesJYqzxh/umLTlf2mKrtoCAmCPaTU/cNdxb
xPdBsP68QikHegD3jrKhI+c6PbM/BT1/gqLqQEjPA+E8lQU9tG4vuCqV1RUiHhpm
/V4kyuL8HtBMl/b78sSCHpzsfr1DFUjrgwr2ixAyu9bwWH/lsXjTcukXVN5N5GC3
Ug==
-----END CERTIFICATE-----
"""

cert_obj = x509.load_pem_x509_certificate(CASDOOR_PUBLIC_CERT.encode('utf-8'), default_backend())
PUBLIC_KEY = cert_obj.public_key()

@app.get("/")
def serve_frontend():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Помилка!</h1><p>Файл index.html не знайдено.")

@app.get("/login")
def login(request: Request):
    token = request.cookies.get("auth_token")
    if token:
        return RedirectResponse("/")

    auth_url = f"{CASDOOR_URL}/login/oauth/authorize?client_id={CLIENT_ID}&response_type=code&redirect_uri={REDIRECT_URI}&scope=openid profile"
    return RedirectResponse(auth_url)

@app.get("/callback")
async def callback(code: str):
    async with httpx.AsyncClient() as client:
        data = {
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": code,
        }
        resp = await client.post(f"{CASDOOR_URL}/api/login/oauth/access_token", data=data)
        token_data = resp.json()

    access_token = token_data.get("access_token")

    if not access_token:
        raise HTTPException(status_code=400, detail="Невдалося отримати токен")
    response = RedirectResponse("/")
    response.set_cookie(key="auth_token", value=access_token, httponly=True)
    return response

@app.get("/user-info")
async def user_info(request: Request):
    token = request.cookies.get("auth_token")

    if not token:
        raise HTTPException(status_code=401, detail="Unauthorized: Ви не авторизовані")

    try:
        payload = jwt.decode(token, PUBLIC_KEY, algorithms=["RS256"], options={"verify_aud": False})
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Unauthorized: Термін дії токена закінчився")
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Unauthorized: Токен невалідний ({str(e)})")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка валідації токена: {str(e)}")

@app.get("/hello")
def hello():
    return {"message": "Hello from Istoshyn Pavlo KP-32"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    token = websocket.cookies.get("auth_token")
    if not token:
        await websocket.close(code=1008, reason="Missing Token")
        return

    try:
        jwt.decode(token, PUBLIC_KEY, algorithms=["RS256"], options={"verify_aud": False})
    except Exception:
        await websocket.close(code=1008, reason="Invalid Token")
        return

    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)

            if payload.get("action") == "subscribe":
                symbols = payload.get("symbols", [])
                manager.subscribe(websocket, symbols)
                print(f"Клієнт підписався на: {symbols}")

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Клієнт відключився")

def run_secure_server():
    p12_filepath = "localhost+1.p12"
    password = b"changeit"

    try:
        with open(p12_filepath, "rb") as f:
            p12_data = f.read()
    except FileNotFoundError:
        print(f"Помилка: Файл сертификата {p12_filepath} не знайдено")
        return

    private_key, certificate, additional_certs = pkcs12.load_key_and_certificates(p12_data, password)

    temp_cert_path = "temp_cert.pem"
    temp_key_path = "temp_key.pem"

    with open(temp_cert_path, "wb") as f:
        f.write(certificate.public_bytes(serialization.Encoding.PEM))

    with open(temp_key_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))

    my_ciphers = "AES256-SHA256:AES256-SHA:AES128-SHA256"

    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        ssl_keyfile=temp_key_path,
        ssl_certfile=temp_cert_path,
        ssl_version=ssl.PROTOCOL_TLSv1_2,
        ssl_ciphers=my_ciphers
    )

if __name__ == "__main__":
    run_secure_server()