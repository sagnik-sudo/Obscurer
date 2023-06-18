# Import the required packages
from fastapi import FastAPI, File, UploadFile

# Import the Uploader class from uploader.py
from src.uploader import Uploader

# Create a FastAPI app
app = FastAPI()

# Create an instance of the Uploader class with the bucket name
uploader = Uploader("casadona-warriors")

# Define a route for uploading a ZIP file
@app.post("/upload_zip")
async def upload_zip(zip_file: UploadFile = File(...)):
    # Call the upload_zip method of the uploader instance with the zip_file as an argument
    uploader.upload_zip(zip_file)
    # Return a success message
    return {"message": "ZIP file uploaded successfully"}