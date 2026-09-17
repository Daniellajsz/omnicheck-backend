import os
import io
import asyncio
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List
from pypdf import PdfWriter

app = FastAPI(title="Omnicheck Backend API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class PDFRequest(BaseModel):
    urls: List[str]

# Obter Token JWT/OAuth direto da Adobe
async def obter_access_token(client: httpx.AsyncClient) -> str:
    client_id = os.getenv("ADOBE_CLIENT_ID") or os.getenv("PDF_SERVICES_CLIENT_ID") or "aee1a4561ffb4c28b98a70ee1153eeee"
    client_secret = os.getenv("ADOBE_CLIENT_SECRET") or os.getenv("PDF_SERVICES_CLIENT_SECRET") or "p8e-bqN7olG7P9izc8hzBf9Uh7N9gJVGLSVW"

    url = "https://pdf-services.adobe.io/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    resp = await client.post(url, data=data, headers=headers)
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Falha ao autenticar na Adobe: {resp.text}")
    return resp.json()["access_token"]

# Processar conversão HTML -> PDF direto pela API REST da Adobe
async def converter_url_adobe_direct(url: str, access_token: str, client_id: str, client: httpx.AsyncClient) -> bytes:
    # 1. Download do HTML do Mailchimp
    resp_mailchimp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10.0)
    resp_mailchimp.raise_for_status()

    headers_adobe = {
        "Authorization": f"Bearer {access_token}",
        "x-api-key": client_id,
        "Content-Type": "application/json"
    }

    # 2. Solicitar upload do Asset para a Adobe
    upload_req = await client.post(
        "https://pdf-services.adobe.io/assets",
        json={"mediaType": "text/html"},
        headers=headers_adobe
    )
    upload_req.raise_for_status()
    upload_data = upload_req.json()

    upload_url = upload_data["uploadUri"]
    asset_id = upload_data["assetID"]

    # 3. Enviar o HTML para o Storage da Adobe
    await client.put(upload_url, content=resp_mailchimp.content, headers={"Content-Type": "text/html"})

    # 4. Iniciar Job de conversão HTML to PDF
    job_payload = {
        "assetID": asset_id,
        "json": "{}",
        "pageLayout": {"pageWidth": 8.5, "pageHeight": 11}
    }
    job_req = await client.post(
        "https://pdf-services.adobe.io/operation/htmltopdf",
        json=job_payload,
        headers=headers_adobe
    )
    job_req.raise_for_status()
    status_url = job_req.headers["location"]

    # 5. Poll do resultado (máximo 15 segundos)
    for _ in range(15):
        await asyncio.sleep(1.0)
        status_req = await client.get(status_url, headers=headers_adobe)
        status_data = status_req.json()

        if status_data.get("status") == "done":
            download_url = status_data["asset"]["downloadUri"]
            download_req = await client.get(download_url)
            return download_req.content
        elif status_data.get("status") == "failed":
            raise Exception("A conversão falhou no servidor da Adobe.")

    raise Exception("Timeout ao aguardar resposta da Adobe.")

@app.get("/")
def home():
    return {"status": "ok", "message": "Backend Omnicheck Adobe Direct ativo"}

@app.post("/gerar-pdf-adobe")
@app.post("/gerar-pdf-adobe/")
@app.post("/api/gerar-pdf-adobe")
@app.post("/api/gerar-pdf-adobe/")
async def gerar_pdf_adobe(payload: PDFRequest):
    if not payload.urls:
        raise HTTPException(status_code=400, detail="A lista de URLs não pode estar vazia.")

    client_id = os.getenv("ADOBE_CLIENT_ID") or os.getenv("PDF_SERVICES_CLIENT_ID") or "aee1a4561ffb4c28b98a70ee1153eeee"

    async with httpx.AsyncClient(timeout=30.0) as client:
        access_token = await obter_access_token(client)
        semaphore = asyncio.Semaphore(5)

        async def processar_com_semaforo(url: str):
            async with semaphore:
                try:
                    return await converter_url_adobe_direct(url, access_token, client_id, client)
                except Exception as e:
                    print(f"[Erro Adobe] {url}: {str(e)}")
                    return None

        tasks = [processar_com_semaforo(url) for url in payload.urls]
        resultados = await asyncio.gather(*tasks)

    pdf_buffers = [res for res in resultados if res is not None]

    if not pdf_buffers:
        raise HTTPException(status_code=500, detail="Não foi possível converter as URLs via Adobe.")

    writer = PdfWriter()
    for pdf_bytes in pdf_buffers:
        writer.append(io.BytesIO(pdf_bytes))

    output_stream = io.BytesIO()
    writer.write(output_stream)
    writer.close()
    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=Boletins_Mailchimp_Adobe.pdf"}
    )
