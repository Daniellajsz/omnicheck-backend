import os
import io
import asyncio
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List

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
    client_id = os.getenv("ADOBE_CLIENT_ID") or os.getenv("PDF_SERVICES_CLIENT_ID") or "aee1a4561ffb4c28b98a70ee1153eeee"
    client_secret = os.getenv("ADOBE_CLIENT_SECRET") or os.getenv("PDF_SERVICES_CLIENT_SECRET") or "p8e-bqN7olG7P9izc8hzBf9Uh7N9gJVGLSVW"
    
    credentials = ServicePrincipalCredentials(client_id=client_id, client_secret=client_secret)
    return PDFServices(credentials=credentials)

def processar_adobe_sync(html_bytes: bytes, pdf_services: PDFServices) -> bytes:
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

@app.get("/")
def home():
    return {"status": "ok", "message": "Omnicheck Backend Ativo"}

@app.post("/gerar-pdf-adobe")
@app.post("/gerar-pdf-adobe/")
@app.post("/api/gerar-pdf-adobe")
@app.post("/api/gerar-pdf-adobe/")
async def gerar_pdf_adobe(payload: PDFRequest):
    if not payload.urls:
        raise HTTPException(status_code=400, detail="Nenhuma URL informada.")

    url = payload.urls[0]
    pdf_services = obter_pdf_services()

    async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
        resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10.0)
        resp.raise_for_status()
        pdf_bytes = await asyncio.to_thread(processar_adobe_sync, resp.content, pdf_services)

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=boletim.pdf"}
    )
