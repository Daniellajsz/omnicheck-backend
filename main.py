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
from adobe.pdfservices.operation.exception.exceptions import ServiceApiException

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
    # ATENÇÃO: as credenciais NÃO têm mais fallback hardcoded.
    # Configure ADOBE_CLIENT_ID e ADOBE_CLIENT_SECRET nas variáveis
    # de ambiente do Render (Settings -> Environment).
    # Se você usava as credenciais antigas (aee1a4561ffb4c28b98a70ee1153eeee...),
    # REVOGUE-AS no Adobe Developer Console e gere novas, já que estavam
    # expostas no código-fonte.
    client_id = os.getenv("ADOBE_CLIENT_ID") or os.getenv("PDF_SERVICES_CLIENT_ID")
    client_secret = os.getenv("ADOBE_CLIENT_SECRET") or os.getenv("PDF_SERVICES_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise ValueError(
            "Credenciais da Adobe não configuradas. Defina ADOBE_CLIENT_ID e "
            "ADOBE_CLIENT_SECRET nas variáveis de ambiente do Render."
        )

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

async def converter_url_adobe_com_retry(
    url: str,
    pdf_services: PDFServices,
    client: httpx.AsyncClient,
    max_tentativas: int = 3,
) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp = await client.get(url, headers=headers, timeout=12.0)
    resp.raise_for_status()
    html_bytes = resp.content

    esperas = [3, 8, 15]  # segundos entre tentativas (backoff crescente)

    ultimo_erro = None
    for tentativa in range(max_tentativas):
        try:
            return await asyncio.to_thread(executar_conversao_adobe_sync, html_bytes, pdf_services)
        except ServiceApiException as e:
            mensagem = str(e)
            ultimo_erro = e
            # Só vale a pena tentar de novo em erros transitórios (503/5xx).
            # Erro de autenticação, payload inválido etc. não adianta repetir.
            if "statusCode=503" in mensagem or "ServiceUnavaibale" in mensagem or "statusCode=5" in mensagem:
                if tentativa < max_tentativas - 1:
                    espera = esperas[min(tentativa, len(esperas) - 1)]
                    print(f"[Retry] {url} -> 503 da Adobe, tentativa {tentativa + 1}/{max_tentativas}, aguardando {espera}s")
                    await asyncio.sleep(espera)
                    continue
            raise
        except Exception as e:
            ultimo_erro = e
            raise

    raise ultimo_erro

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

    # IMPORTANTE: reduzido para 1 (sequencial) porque o plano trial/free da
    # Adobe PDF Services parece não suportar conversões em paralelo,
    # respondendo com 503 (ServiceUnavaibale) quando há concorrência.
    # Se confirmar que era isso, e quiser paralelismo de novo no futuro,
    # isso só é seguro com um plano pago da Adobe que suporte concorrência.
    semaphore = asyncio.Semaphore(1)

    erros_detalhados = []

    async def processar_com_semaforo(url: str, client: httpx.AsyncClient):
        async with semaphore:
            try:
                return await converter_url_adobe_com_retry(url, pdf_services, client)
            except Exception as e:
                erro_str = str(e)
                print(f"[Aviso] Falha ao converter URL ({url}): {erro_str}")
                erros_detalhados.append(f"{url}: {erro_str}")
                return None

    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0) as client:
        tasks = [processar_com_semaforo(url, client) for url in payload.urls]
        resultados = await asyncio.gather(*tasks)

    pdf_buffers = [res for res in resultados if res is not None]

    if not pdf_buffers:
        detalhe = "Não foi possível converter as URLs em PDF através da API da Adobe."
        if erros_detalhados:
            # Inclui o motivo real da primeira falha na resposta,
            # em vez de só a mensagem genérica.
            detalhe += f" Detalhe: {erros_detalhados[0]}"
        raise HTTPException(status_code=500, detail=detalhe)

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
