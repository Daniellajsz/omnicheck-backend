import os
import io
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List

# Adobe PDF Services SDK (v3.x+)
from adobe.pdfservices.operation.auth.credentials import Credentials
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
from adobe.pdfservices.operation.io.stream_asset import StreamAsset
from adobe.pdfservices.operation.pdfjobs.params.html_to_pdf.html_to_pdf_params import HTMLToPDFParams
from adobe.pdfservices.operation.pdfjobs.jobs.html_to_pdf_job import HTMLToPDFJob
from adobe.pdfservices.operation.pdfjobs.result.html_to_pdf_result import HTMLToPDFResult

# PyPDF2 para unificar os PDFs gerados
from PyPDF2 import PdfMerger

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

def obter_credenciais_adobe():
    client_id = os.getenv("PDF_SERVICES_CLIENT_ID")
    client_secret = os.getenv("PDF_SERVICES_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        raise ValueError("Credenciais da Adobe não configuradas nas variáveis de ambiente.")
        
    return Credentials.service_principal_credentials_builder() \
        .with_client_id(client_id) \
        .with_client_secret(client_secret) \
        .build()

async def converter_url_para_pdf_bytes(url: str, credentials: Credentials) -> bytes:
    def _converter():
        # Inicializa o serviço v3 da Adobe
        pdf_services = PDFServices(credentials=credentials)
        
        # Como o HTML é via URL, fazemos o download simples dos bytes do HTML
        import requests
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        html_bytes = resp.content
        
        # Upload do HTML para os ativos da Adobe
        input_asset = pdf_services.upload(
            input_stream=io.BytesIO(html_bytes),
            mime_type=PDFServicesMediaType.HTML
        )
        
        # Criação e submissão do Job de HTML para PDF
        params = HTMLToPDFParams.builder().build()
        job = HTMLToPDFJob(input_asset=input_asset, html_to_pdf_params=params)
        
        location = pdf_services.submit(job)
        pdf_services_response = pdf_services.get_job_result(location, HTMLToPDFResult)
        
        result_asset = pdf_services_response.get_result().get_asset()
        stream_asset: StreamAsset = pdf_services.get_job_output(result_asset)
        
        return stream_asset.get_input_stream().read()

    return await asyncio.to_thread(_converter)

@app.get("/")
def home():
    return {"status": "ok", "message": "Omnicheck Backend está ativo"}

@app.post("/gerar-pdf-adobe")
async def gerar_pdf_adobe(payload: PDFRequest):
    if not payload.urls:
        raise HTTPException(status_code=400, detail="A lista de URLs não pode estar vazia.")
    
    try:
        credentials = obter_credenciais_adobe()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro de autenticação Adobe: {str(e)}")

    pdf_buffers = []
    
    for index, url in enumerate(payload.urls):
        # Pausa de 2.5s a cada 10 links para garantir respeito ao rate-limit
        if index > 0 and index % 10 == 0:
            await asyncio.sleep(2.5)

        try:
            pdf_bytes = await converter_url_para_pdf_bytes(url, credentials)
            pdf_buffers.append(io.BytesIO(pdf_bytes))
        except Exception as e:
            print(f"[Aviso] Falha ao converter URL ({url}): {str(e)}")
            continue

    if not pdf_buffers:
        raise HTTPException(
            status_code=500, 
            detail="Não foi possível converter nenhuma das URLs fornecidas em PDF."
        )

    merger = PdfMerger()
    for buf in pdf_buffers:
        buf.seek(0)
        merger.append(buf)

    output_stream = io.BytesIO()
    merger.write(output_stream)
    merger.close()
    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=Boletins_Mailchimp_Adobe.pdf"}
    )
