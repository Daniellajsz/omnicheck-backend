import os
import io
import json
import asyncio
import tempfile
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List

# Adobe PDF Services SDK (Imports modernos v3)
from adobe.pdfservices.operation.auth.credentials import Credentials
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.pdfjobs.jobs.html_to_pdf_job import HTMLToPDFJob
from adobe.pdfservices.operation.pdfjobs.result.html_to_pdf_result import HTMLToPDFResult

from pypdf import PdfWriter

app = FastAPI(title="Omnicheck Backend API")

# Permite acesso irrestrito do seu frontend do Cloudflare
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
    client_id = os.getenv("PDF_SERVICES_CLIENT_ID")
    client_secret = os.getenv("PDF_SERVICES_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise ValueError("Credenciais da Adobe não configuradas nas variáveis de ambiente.")
        
    credentials = Credentials.service_principal_credentials_builder() \
        .with_client_id(client_id) \
        .with_client_secret(client_secret) \
        .build()
        
    return PDFServices(credentials=credentials)

def converter_uma_url(url: str, pdf_services: PDFServices) -> bytes:
    # 1. Baixa o HTML da página do Mailchimp
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    html_content = resp.content

    # 2. Upload para a Adobe
    input_asset = pdf_services.upload(
        input_stream=io.BytesIO(html_content),
        mime_type=PDFServicesMediaType.HTML
    )

    # 3. Executa a conversão
    job = HTMLToPDFJob(input_asset=input_asset)
    location = pdf_services.submit(job)
    pdf_services_response = pdf_services.get_job_result(location, HTMLToPDFResult)

    result_asset = pdf_services_response.get_result().get_asset()
    stream_asset = pdf_services.get_job_output(result_asset)
    
    return stream_asset.get_input_stream().read()

@app.get("/")
def home():
    return {"status": "ok", "message": "Omnicheck Backend está ativo"}

@app.post("/gerar-pdf-adobe")
async def gerar_pdf_adobe(payload: PDFRequest):
    if not payload.urls:
        raise HTTPException(status_code=400, detail="A lista de URLs não pode estar vazia.")
    
    try:
        pdf_services = obter_pdf_services()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao inicializar Adobe: {str(e)}")

    pdf_buffers = []

    for index, url in enumerate(payload.urls):
        # Pausa a cada 10 links para respeitar o limite por minuto da Adobe
        if index > 0 and index % 10 == 0:
            await asyncio.sleep(2.5)

        try:
            # Executa a chamada em thread isolada para evitar congelar o servidor
            pdf_bytes = await asyncio.to_thread(converter_uma_url, url, pdf_services)
            pdf_buffers.append(io.BytesIO(pdf_bytes))
        except Exception as e:
            print(f"[Aviso] Falha na URL ({url}): {str(e)}")
            continue

    if not pdf_buffers:
        raise HTTPException(
            status_code=500, 
            detail="Falha ao converter os links solicitados."
        )

    # Unifica os PDFs gerados com pypdf
    writer = PdfWriter()
    for buf in pdf_buffers:
        buf.seek(0)
        writer.append(buf)

    output_stream = io.BytesIO()
    writer.write(output_stream)
    writer.close()
    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=Boletins_Mailchimp_Adobe.pdf"}
    )
