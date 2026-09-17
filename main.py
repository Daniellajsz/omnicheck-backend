import io
import asyncio
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List
from xhtml2pdf import pisa
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

def html_para_pdf_bytes(html_content: str) -> bytes:
    pdf_buffer = io.BytesIO()
    pisa_status = pisa.CreatePDF(html_content, dest=pdf_buffer)
    if pisa_status.err:
        raise Exception("Erro ao converter HTML para PDF localmente.")
    return pdf_buffer.getvalue()

async def converter_url_local(url: str, client: httpx.AsyncClient) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp = await client.get(url, headers=headers, timeout=10.0)
    resp.raise_for_status()
    
    return await asyncio.to_thread(html_para_pdf_bytes, resp.text)

@app.get("/")
def home():
    return {"status": "ok", "message": "Omnicheck Backend Local está ativo"}

@app.post("/gerar-pdf-adobe")
@app.post("/gerar-pdf-adobe/")
@app.post("/api/gerar-pdf-adobe")
@app.post("/api/gerar-pdf-adobe/")
async def gerar_pdf_adobe(payload: PDFRequest):
    if not payload.urls:
        raise HTTPException(status_code=400, detail="A lista de URLs não pode estar vazia.")

    semaphore = asyncio.Semaphore(10)

    async def processar_com_semaforo(url: str, client: httpx.AsyncClient):
        async with semaphore:
            try:
                return await converter_url_local(url, client)
            except Exception as e:
                print(f"[Aviso] Falha ao converter {url}: {str(e)}")
                return None

    async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
        tasks = [processar_com_semaforo(url, client) for url in payload.urls]
        resultados = await asyncio.gather(*tasks)

    pdf_buffers = [res for res in resultados if res is not None]

    if not pdf_buffers:
        raise HTTPException(
            status_code=500, 
            detail="Não foi possível converter nenhuma das URLs em PDF."
        )

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
        headers={"Content-Disposition": "attachment; filename=Boletins_Mailchimp.pdf"}
    )
