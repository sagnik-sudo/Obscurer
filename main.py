# Importing the required modules
from fastapi import FastAPI, File, UploadFile
import tempfile
import aiofiles
from src.uploader import Uploader
from typing import List
import os

# Create a FastAPI app
app = FastAPI(
    title="Casadona Warriors Data Endpoint",
    description="This project is a FastAPI application that takes image, pdf, or docx files as input and converts them to a readable text file, then applies PII (Personally Identifiable Information) deidentification and finally gives that as the output to the user.",
    version="beta",
    docs_url="/",
    redoc_url="/cw-redoc",
    contact={
        "name": "Developer - Sagnik Das, Somdutta Paul, Tania Rana",
        "email": "sagnik.das03@infosys.com",
    },
)

BUCKET_NAME = "waratcasadona" # Modify bucket name here as per requirement

# Initializing Uploader Module
uploader = Uploader()

@app.get("/health",tags=["Service Status"], name="Application Health Check")
async def health():
    return {"Service Status":"Up and Running"}

@app.post("/upload-file/",tags=["Upload Module"], name="Uploads single file to server")
async def create_upload_file(file: UploadFile):
    """Upload Single File"""
    file_name = file.filename
    temp_dir = tempfile.gettempdir()
    temp_f = f"{temp_dir}\{file_name}"
    async with aiofiles.open(temp_f, 'wb') as out_file:
        content = await file.read()  # async read
        await out_file.write(content)  # async write
    uploader.upload_to_bucket(file_name,temp_f,BUCKET_NAME)
    return {"Upload Status":f"Saved Successfully - {file_name}"}

@app.post("/multi-upload/",tags=["Upload Module"], name="Uploads multiple files to server")
async def multiupload(files: List[UploadFile] = File(...)):
    """Upload Multiple Files"""
    tempdir = tempfile.gettempdir()
    for file in files:
        destination_file_path = os.path.join(tempdir, file.filename)
        async with aiofiles.open(destination_file_path, 'wb') as out_file:
        # async read file chunk
            while content := await file.read(1024):
                await out_file.write(content)  # async write file chunk
    paths = [os.path.join(tempdir, file.filename) for file in files]
    for i in range(len(paths)):
        uploader.upload_to_bucket(files[i].filename,paths[i],BUCKET_NAME)
    return {"Upload Status":{"Files Uploaded":files}}
