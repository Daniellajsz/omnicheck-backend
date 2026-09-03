import os
import io
import re
import asyncio
from typing import List
from urllib.parse import quote, urlparse
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx

from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
from adobe.pdfservices.operation.pdf_services import PDFServices
from adobe.pdfservices.operation.pdfjobs.params.html_to_pdf.html_to_pdf_params import HTMLtoPDFParams
from adobe.pdfservices.operation.pdfjobs.params.combine_pdf.combine_pdf_params import CombinePDFParams
from adobe.pdfservices.operation.pdfjobs.jobs.html_to_pdf_job import HTMLtoPDFJob
from adobe.pdfservices.operation.pdfjobs.jobs.combine_pdf_job import CombinePDFJob
from adobe.pdfservices.operation.pdfjobs.result.html_to_pdf_result import HTMLtoPDFResult
from adobe.pdfservices.operation.pdfjobs.result.combine_pdf_result import CombinePDFResult

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ADOBE_CLIENT_ID = os.getenv("ADOBE_CLIENT_ID")
ADOBE_CLIENT_SECRET = os.getenv("ADOBE_CLIENT_SECRET")

SEMAFORO_CONVERSAO = asyncio.Semaphore(3)

class AdobePDFRequest(BaseModel):
    urls: List[str]

async def baixar_e_limpar_html(client: httpx.AsyncClient, url: str) -> str:
    try:
        url_limpa_str = url.strip()
        parsed = urlparse(url_limpa_str)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("URL malformada")

        path_escapado = quote(parsed.path)
        url_sanitizada = parsed._replace(path=path_escapado).geturl()

        response = await client.get(url_sanitizada)
        response.raise_for_status()
        html = response.text

        if not html or not html.strip():
            raise ValueError("HTML vazio")

        html = re.sub(r'<script[^>]*src="[^"]*mailchimp[^"]*"[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<script[^>]*src="[^"]*translate[^"]*"[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)

        css_limpeza = """
        <style>
            #awesomewrap, #archivetopbar, .mcnPreviewPane, 
            .goog-te-banner-frame, .skiptranslate, 
            header, nav, .sticky, .fixed, .navbar,
            [style*="position: fixed"], [style*="position: sticky"] { 
                display: none !important; visibility: hidden !important; height: 0px !important;
            }
            html, body { top: 0px !important; margin-top: 0px !important; position: static !important; }
        </style>
        """
        return html.replace("<head>", f"<head>{css_limpeza}") if "<head>" in html else f"{css_limpeza}{html}"

    except Exception as err:
        return f"<!DOCTYPE html><html><head><meta charset='utf-8'></head><body><h3>Link com falha de acesso:</h3><p>{url}</p></body></html>"

def processar_html_adobe(pdf_services: PDFServices, html_content: str):
    stream_bytes = io.BytesIO(html_content.encode("utf-8"))
    input_asset = pdf_services.upload(input_stream=stream_bytes, mime_type="text/html")
    html_to_pdf_job = HTMLtoPDFJob(
        input_asset=input_asset, 
        html_to_pdf_params=HTMLtoPDFParams(include_header_footer=False)
    )            
    job_control_location = pdf_services.submit(html_to_pdf_job)
    pdf_services_response = pdf_services.get_job_result(job_control_location, HTMLtoPDFResult)
    return pdf_services_response.get_result().get_asset()

async def converter_com_semaforo_seguro(pdf_services: PDFServices, html: str, url: str):
    async with SEMAFORO_CONVERSAO:
        await asyncio.sleep(0.5)
        try:
            return await asyncio.to_thread(processar_html_adobe, pdf_services, html)
        except Exception:
            html_seguro = f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head><body style="font-family: Arial, sans-serif; padding: 30px;"><div style="border: 2px solid #e74c3c; padding: 20px; border-radius: 8px; background: #fff5f5;"><h3 style="color: #c0392b; margin-top:0;">Matéria Indisponível para Conversão Direta</h3><p>O conteúdo do link abaixo não pôde ser renderizado automaticamente pela API da Adobe:</p><p><a href="{url}" target="_blank">{url}</a></p></div></body></html>"""
            return await asyncio.sleep(0.5) or await asyncio.to_thread(processar_html_adobe, pdf_services, html_seguro)

@app.post("/gerar-pdf-adobe")
async def gerar_pdf_adobe(payload: AdobePDFRequest):
    if not payload.urls:
        raise HTTPException(status_code=400, detail="Nenhuma URL informada.")

    try:
        credentials = ServicePrincipalCredentials(client_id=ADOBE_CLIENT_ID, client_secret=ADOBE_CLIENT_SECRET)
        pdf_services = PDFServices(credentials=credentials)

        limits = httpx.Limits(max_keepalive_connections=20, max_connections=35)
        async with httpx.AsyncClient(follow_redirects=True, timeout=10.0, limits=limits, headers={"User-Agent": "Mozilla/5.0"}) as client:
            html_tasks = [baixar_e_limpar_html(client, url) for url in payload.urls]
            html_contents = await asyncio.gather(*html_tasks)

        pdf_tasks = [converter_com_semaforo_seguro(pdf_services, html, url) for html, url in zip(html_contents, payload.urls)]
        pdf_assets = await asyncio.gather(*pdf_tasks)

        combine_params = CombinePDFParams()
        for asset in pdf_assets:
            combine_params.add_asset(asset)
        
        combine_job = CombinePDFJob(combine_pdf_params=combine_params)
        job_control_location = pdf_services.submit(combine_job)
        pdf_services_response = pdf_services.get_job_result(job_control_location, CombinePDFResult)
        
        final_asset = pdf_services_response.get_result().get_asset()
        pdf_bytes = pdf_services.get_content(final_asset).get_input_stream()

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=Boletim_Parcial.pdf"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar PDF: {str(e)}")

@app.post("/unificar-pdfs-temp")
async def unificar_pdfs_temp(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado.")

    try:
        credentials = ServicePrincipalCredentials(client_id=ADOBE_CLIENT_ID, client_secret=ADOBE_CLIENT_SECRET)
        pdf_services = PDFServices(credentials=credentials)

        combine_params = CombinePDFParams()
        for file in files:
            file_bytes = await file.read()
            stream_bytes = io.BytesIO(file_bytes)
            asset = pdf_services.upload(input_stream=stream_bytes, mime_type="application/pdf")
            combine_params.add_asset(asset)

        combine_job = CombinePDFJob(combine_pdf_params=combine_params)
        job_control_location = pdf_services.submit(combine_job)
        pdf_services_response = pdf_services.get_job_result(job_control_location, CombinePDFResult)

        final_asset = pdf_services_response.get_result().get_asset()
        pdf_bytes = pdf_services.get_content(final_asset).get_input_stream()

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=Boletim_Unificado.pdf"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na unificação: {str(e)}")
