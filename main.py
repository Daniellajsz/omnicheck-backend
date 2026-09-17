import os
import io
import asyncio
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List

# SDK Oficial da Adobe
from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.pdfjobs.jobs.html_to_pdf_job import HTMLtoPDFJob
from adobe.pdfservices.operation.pdfjobs.params.html_to_pdf.html_to_pdf_params import HTMLtoPDFParams
from adobe.pdfservices.operation.pdfjobs.result.html_to_pdf_result import HTMLtoPDFResult

from pypdf import PdfWriter

app = FastAPI(title="Omnicheck Backend API")

# Habilita suporte a requisições CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class PDFRequest(BaseModel):
    urls: List[str]

def obter_pdf_services():
    client_id = (
        os.getenv("ADOBE_CLIENT_ID") 
        or os.getenv("PDF_SERVICES_CLIENT_ID") 
        or "aee1a4561ffb4c28b98a70ee1153eeee"
    )
    client_secret = (
        os.getenv("ADOBE_CLIENT_SECRET") 
        or os.getenv("PDF_SERVICES_CLIENT_SECRET") 
        or "p8e-bqN7olG7P9izc8hzBf9Uh7N9gJVGLSVW"
    )
    
    if not client_id or not client_secret:
        raise ValueError("Credenciais da Adobe não configuradas.")
        
    credentials = ServicePrincipalCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    return PDFServices(credentials=credentials)

def executar_conversao_adobe_sync(html_bytes: bytes, pdf_services: PDFServices) -> bytes:
    input_asset = pdf_services.upload(
        input_stream=io.BytesIO(html_bytes),
        mime_type=PDFServicesMediaType.HTML
    )

    html_to_pdf_params = HTMLtoPDFParams()
    html_to_pdf_job = HTMLtoPDFJob(input_asset=input_asset, html_to_pdf_params=html_to_pdf_params)

    location = pdf_services.submit(html_to_pdf_job)
    pdf_services_response = pdf_services.get_job_result(location, HTMLtoPDFResult)

    result_asset = pdf_services_response.get_result().get_asset()
    stream_asset = pdf_services.get_content(result_asset)
    return stream_asset.get_input_stream().read()

async def converter_url_adobe(url: str, pdf_services: PDFServices, client: httpx.AsyncClient) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp = await client.get(url, headers=headers, timeout=12.0)
    resp.raise_for_status()
    
    return await asyncio.to_thread(executar_conversao_adobe_sync, resp.content, pdf_services)

@app.get("/")
def home():
    return {"status": "ok", "message": "Backend Omnicheck está ativo"}

@app.post("/gerar-pdf-adobe")
@app.post("/gerar-pdf-adobe/")
@app.post("/api/gerar-pdf-adobe")
@app.post("/api/gerar-pdf-adobe/")
async def gerar_pdf_adobe(payload: PDFRequest):
    if not payload.urls:
        raise HTTPException(status_code=400, detail="A lista de URLs não pode estar vazia.")

    try:
        pdf_services = obter_pdf_services()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro de autenticação Adobe: {str(e)}")

    # Semáforo limitado a 2 conversões paralelas para evitar timeout no Render
    semaphore = asyncio.Semaphore(2)

    async def processar_com_semaforo(url: str, client: httpx.AsyncClient):
        async with semaphore:
            try:
                return await converter_url_adobe(url, pdf_services, client)
            except Exception as e:
                print(f"[Aviso] Falha ao converter URL ({url}): {str(e)}")
                return None

    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0) as client:
        tasks = [processar_com_semaforo(url, client) for url in payload.urls]
        resultados = await asyncio.gather(*tasks)

    pdf_buffers = [res for res in resultados if res is not None]

    if not pdf_buffers:
        raise HTTPException(
            status_code=500, 
            detail="Não foi possível converter as URLs em PDF através da API da Adobe."
        )

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
