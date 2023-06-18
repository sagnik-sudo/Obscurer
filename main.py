# Import the required packages
from fastapi import FastAPI, File, UploadFile

# Import the Uploader class from uploader.py
from src.uploader import Uploader

# Create a FastAPI app
app = FastAPI(title="Casadona Warriors Data Endpoint",
    description="This is a very cool API that does awesome stuff.",
    version="1.0.0",
    docs_url="/",
    redoc_url="/cw-redoc",
    openapi_url="/cw-openapi.json",
    contact={
        "name": "Sagnik Das",
        "url": "www.github.com/sagnik-sudo",
        "email": "sagnikdas2305@gmail.com"
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT"
    })

# Create an instance of the Uploader class with the bucket name
uploader = Uploader("casadona-warriors101")

# Define a route for uploading a ZIP file
@app.post("/upload_zip")
async def upload_zip(zip_file: UploadFile = File(...)):
    # Call the upload_zip method of the uploader instance with the zip_file as an argument
    uploader.upload_zip(zip_file)
    # Return a success message
    return {"message": "ZIP file uploaded successfully"}