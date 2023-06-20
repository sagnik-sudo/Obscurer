from fastapi import FastAPI, UploadFile, File
from pdf2image import convert_from_bytes
from PIL import Image
import textract
import docx2txt
import os
import mimetypes
import re
import pandas as pd
from io import BytesIO
from pptx import Presentation
import tempfile
from PyPDF2 import PdfReader

app = FastAPI()


def convert_pdf_to_text(file_data):
    pdf = PdfReader(file_data)
    text = ""
    for page in pdf.pages:
        text += page.extract_text()
    return text


def process_text(text):
    # Perform any additional text processing as needed
    # For example, removing special characters or formatting

    # Replace line breaks and tabs with actual line breaks and tabs
    processed_text = text.replace("\n", "\\n").replace("\t", "\\t")
    return processed_text


def convert_pptx_to_text(file_data):
    prs = Presentation(file_data)
    text = ""
    for slide in prs.slides:
        for shape in slide.shapes:
            if hasattr(shape, "text"):
                text += shape.text + "\n"
    if not text:  # If no text found in the presentation
        text = "No text found in the presentation"
    return text


@app.post("/convert")
async def convert_to_text(file: UploadFile = File(...)):
    file_extension = os.path.splitext(file.filename)[1].lower()
    mime_type = mimetypes.guess_type(file.filename)[0]

    if mime_type == "application/pdf":
        file_data = await file.read()
        text = convert_pdf_to_text(BytesIO(file_data))
    elif mime_type.startswith("image/"):
        file_data = await file.read()
        with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as temp_file:
            temp_file.write(file_data)
            temp_file_path = temp_file.name
        text = textract.process(temp_file_path).decode("utf-8")
        os.remove(temp_file_path)
    elif mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        file_data = await file.read()
        text = docx2txt.process(BytesIO(file_data))
    elif mime_type == "text/csv":
        file_data = await file.read()
        df = pd.read_csv(BytesIO(file_data))
        text = df.to_string(index=False)
    elif mime_type == "application/vnd.openxmlformats-officedocument.presentationml.presentation":
        file_data = await file.read()
        text = convert_pptx_to_text(BytesIO(file_data))
    else:
        text = f"File format not supported. MIME type: {mime_type}"

    processed_text = process_text(text)
    return {"text": processed_text}
