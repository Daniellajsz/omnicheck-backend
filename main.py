import os
import io
import asyncio
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List

# Adobe PDF Services SDK - Importação Híbrida (Compatível com v2 e v3)
from adobe.pdfservices.operation.auth.credentials import Credentials

# Tenta carregar a v2 ou v3 dinamicamente sem quebrar a inicialização
IS_V3 = False
try:
    from adobe.pdfservices.operation.pdf_services import PDFServices
    IS_V3 = True
except ImportError:
    from adobe.pdfservices.operation.execution_context import ExecutionContext
    from adobe.pdfservices.operation.pdfops.html_to_pdf_operation import HTMLToPDFOperation
    from adobe.pdfservices.operation.pdfops.options.htmltopdf.html_to_pdf_options import HTMLToPDFOptions
    from adobe.pdfservices.operation.io.file_ref import FileRef

# PyPDF2 para unificação
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
        if IS_V3:
            # Lógica para SDK v3
            from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
            pdf_services = PDFServices(credentials=credentials)
            
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            
            input_asset = pdf_services.upload(
                input_stream=io.BytesIO(resp.content),
                mime_type=PDFServicesMediaType.HTML
            )
            
            # Import dinâmico da Job v3
            from adobe.pdfservices.operation.pdfjobs.jobs.html_to_pdf_job import HTMLToPDFJob
            job = HTMLToPDFJob(input_asset=input_asset)
            
            location = pdf_services.submit(job)
            from adobe.pdfservices.operation.pdfjobs.result.html_to_pdf_result import HTMLToPDFResult
            pdf_services_response = pdf_services.get_job_result(location, HTMLToPDFResult)
            
            result_asset = pdf_services_response.get_result().get_asset()
            stream_asset = pdf_services.get_job_output(result_asset)
            return stream_asset.get_input_stream().read()
        else:
            # Lógica para SDK v2 (Legada e estável)
            execution_context = ExecutionContext.create(credentials)
            html_to_pdf_operation = HTMLToPDFOperation.create()
            html_options = HTMLToPDFOptions.builder().build()
            html_to_pdf_operation.set_options(html_options)
            
            input_file_ref = FileRef.create_from_url(url)
            html_to_pdf_operation.set_input(input_file_ref)
            
            result_file_ref = html_to_pdf_operation.execute(execution_context)
            
            temp_path = f"/tmp/temp_{os.urandom(8).hex()}.pdf"
            result_file_ref.save_as(temp_path)
            
            with open(temp_path, "rb") as f:
                pdf_bytes = f.read()
                
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
            return pdf_bytes

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
        # Pausa de 2.5s a cada 10 links para evitar bloqueio por frequência
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
