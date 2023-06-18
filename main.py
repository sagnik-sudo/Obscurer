# Importing the required modules
from fastapi import FastAPI, File, UploadFile
from zipfile import ZipFile
import os
import shutil
import tempfile

# Import the Uploader class from uploader.py
from src.uploader import Uploader

# Create a FastAPI app
app = FastAPI(title="Casadona Warriors Data Endpoint",
    description="This is a very cool API that does awesome stuff.",
    version="1.0.0",
    docs_url="/",
    redoc_url="/cw-redoc",
    )

# Create an instance of the Uploader class
uploader = Uploader()

temp_folder = temp_folder = tempfile.gettempdir()
os.makedirs(temp_folder, exist_ok=True)

@app.get("/health")
async def health():
    return {"Service Status":"Up and Running"}

# Creating an endpoint to handle file upload
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    # Saving the file to the temp folder
    file_path = os.path.join(temp_folder, file.filename)
    with open(file_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    # Checking if the file is a zip file
    if file.filename.endswith(".zip"):
        # Opening the zip file
        with ZipFile(file_path, "r") as zip_file:
            # Extracting all the files in the zip file to the temp folder
            zip_file.extractall(temp_folder)
            # Looping through the extracted files
            for extracted_file in zip_file.namelist():
                # Getting the extracted file path
                extracted_file_path = os.path.join(temp_folder, extracted_file)
                # Uploading the extracted file to Google Cloud Storage
                uploader.upload_to_gcs(extracted_file_path, "your-bucket-name", extracted_file)
                # Deleting the extracted file from the temp folder
                os.remove(extracted_file_path)
        # Deleting the zip file from the temp folder
        os.remove(file_path)
        # Returning a success message
        return {"message": "Zip file uploaded and extracted successfully."}
    else:
        # Returning an error message
        return {"error": "Please upload a zip file."}