import ssl
import uvicorn
from fastapi import FastAPI
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.primitives import serialization
app = FastAPI()

@app.get("/hello")
def hello():
    return {"“Hello from Istoshyn Pavlo KP-32”"}

def run_secure_server():
    p12_filepath = "localhost+1.p12"
    password = b"changeit"

    try:
        with open(p12_filepath, "rb") as f:
            p12_data = f.read()
    except FileNotFoundError:
        return

    private_key, certificate, additional_certs = pkcs12.load_key_and_certificates(
        p12_data,
        password
    )

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