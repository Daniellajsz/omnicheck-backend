import os
import io
import asyncio
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List

# Importações oficiais da SDK v4.x da Adobe (Nomes exatos das classes)
from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.pdfjobs.jobs.html_to_pdf_job import HTMLtoPDFJob
from adobe.pdfservices.operation.pdfjobs.params.html_to_pdf.html_to_pdf_params import HTMLtoPDFParams
from adobe.pdfservices.operation.pdfjobs.result.html_to_pdf_result import HTMLtoPDFResult

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

def obter_pdf_services():
    client_id = os.getenv("PDF_SERVICES_CLIENT_ID")
    client_secret = os.getenv("PDF_SERVICES_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise ValueError("Credenciais da Adobe não configuradas nas variáveis de ambiente.")
        
    credentials = ServicePrincipalCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    return PDFServices(credentials=credentials)

def converter_uma_url(url: str, pdf_services: PDFServices) -> bytes:
    # 1. Faz o download do HTML da URL do Mailchimp
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    # 2. Upload do conteúdo HTML para a Adobe
    input_asset = pdf_services.upload(
        input_stream=io.BytesIO(resp.content),
        mime_type=PDFServicesMediaType.HTML
    )

    # 3. Cria parâmetros e Job com o padrão oficial da biblioteca (HTMLtoPDFParams / HTMLtoPDFJob)
    html_to_pdf_params = HTMLtoPDFParams()
    html_to_pdf_job = HTMLtoPDFJob(input_asset=input_asset, html_to_pdf_params=html_to_pdf_params)

    # 4. Envia o job e aguarda o retorno
    location = pdf_services.submit(html_to_pdf_job)
    pdf_services_response = pdf_services.get_job_result(location, HTMLtoPDFResult)

    result_asset = pdf_services_response.get_result().get_asset()
    stream_asset = pdf_services.get_content(result_asset)
    
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
        raise HTTPException(status_code=500, detail=f"Erro ao conectar com a Adobe: {str(e)}")

    pdf_buffers = []

    # Processa os links com tratamento em lote
    for index, url in enumerate(payload.urls):
        # Pausa a cada 10 links para respeitar o limite de taxa de requisições por minuto da Adobe
        if index > 0 and index % 10 == 0:
            await asyncio.sleep(2.5)

        try:
            pdf_bytes = await asyncio.to_thread(converter_uma_url, url, pdf_services)
            pdf_buffers.append(pdf_bytes)
        except Exception as e:
            print(f"[Aviso] Falha ao converter URL ({url}): {str(e)}")
            continue

    if not pdf_buffers:
        raise HTTPException(
            status_code=500, 
            detail="Não foi possível converter nenhuma das URLs fornecidas em PDF."
        )

    # Unifica todos os PDFs baixados usando a biblioteca pypdf
    writer = PdfWriter()
    for pdf_bytes in pdf_buffers:
        buf = io.BytesIO(pdf_bytes)
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
