import io
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List
from playwright.async_api import async_playwright
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

async def converter_url_com_playwright(url: str, browser) -> bytes:
    page = await browser.new_page()
    try:
        # Carrega a página do Mailchimp até renderizar todo o CSS/Imagens
        await page.goto(url, wait_until="networkidle", timeout=15000)
        pdf_bytes = await page.pdf(
            format="A4",
            print_background=True,
            margin={"top": "10mm", "bottom": "10mm", "left": "10mm", "right": "10mm"}
        )
        return pdf_bytes
    finally:
        await page.close()

@app.get("/")
def home():
    return {"status": "ok", "message": "Backend Omnicheck Playwright ativo"}

@app.post("/gerar-pdf-adobe")
@app.post("/gerar-pdf-adobe/")
@app.post("/api/gerar-pdf-adobe")
@app.post("/api/gerar-pdf-adobe/")
async def gerar_pdf_adobe(payload: PDFRequest):
    if not payload.urls:
        raise HTTPException(status_code=400, detail="Nenhuma URL informada.")

    pdf_buffers = []

    async with async_playwright() as p:
        # Inicia o naveador Chromium em modo headless
        browser = await p.chromium.launch(headless=True)
        
        for url in payload.urls:
            try:
                pdf_bytes = await converter_url_com_playwright(url, browser)
                pdf_buffers.append(pdf_bytes)
            except Exception as e:
                print(f"[Erro ao converter] {url}: {str(e)}")
                continue
                
        await browser.close()

    if not pdf_buffers:
        raise HTTPException(status_code=500, detail="Não foi possível converter os links em PDF.")

    # Unifica todos os PDFs em um só
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
        headers={"Content-Disposition": "attachment; filename=Boletins_Mailchimp.pdf"}
    )
