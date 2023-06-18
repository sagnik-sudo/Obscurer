# Importing the required modules
from fastapi import FastAPI, File, UploadFile
import tempfile
import aiofiles
from src.uploader import Uploader

# Create a FastAPI app
app = FastAPI(title="Casadona Warriors Data Endpoint",
    description="This is a very cool API that does awesome stuff.",
    version="1.0.0",
    docs_url="/",
    redoc_url="/cw-redoc",
    )

uploader = Uploader()

@app.get("/health")
async def health():
    return {"Service Status":"Up and Running"}

#Upload File
@app.post("/upload-file/")
async def create_upload_file(file: UploadFile):
    file_name = file.filename
    temp_dir = tempfile.gettempdir()
    temp_f = f"{temp_dir}\{file_name}"
    async with aiofiles.open(temp_f, 'wb') as out_file:
        content = await file.read()  # async read
        await out_file.write(content)  # async write
    uploader.upload_to_bucket(file_name,temp_f,"casadonawarriors111234")
    
    return {"Upload Status":f"Saved Successfully - {file_name}"}