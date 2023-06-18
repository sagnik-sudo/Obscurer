# Import the required packages
from fastapi import FastAPI, File, UploadFile
from google.cloud import storage
from google.cloud import sql
import zipfile
import os

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

# Create a Google Cloud Storage client
storage_client = storage.Client()

# Create a Google Cloud SQL client
sql_client = sql.SQLAdminServiceClient()

# Define the bucket name and the database name
bucket_name = "your-bucket-name"
database_name = "your-database-name"

# Define a route for uploading a ZIP file
@app.post("/upload_zip")
async def upload_zip(zip_file: UploadFile = File(...)):
    # Check if the file is a ZIP file
    if zip_file.filename.endswith(".zip"):
        # Save the ZIP file to a temporary location
        temp_zip_file = f"temp/{zip_file.filename}"
        with open(temp_zip_file, "wb") as f:
            f.write(await zip_file.read())
        # Open the ZIP file and extract its contents
        with zipfile.ZipFile(temp_zip_file, "r") as z:
            z.extractall("temp")
        # Delete the ZIP file from the temporary location
        os.remove(temp_zip_file)
        # Loop through the extracted files
        for filename in os.listdir("temp"):
            # Get the file size and extension
            file_size = os.path.getsize(f"temp/{filename}")
            file_extension = os.path.splitext(filename)[1]
            # Upload the file to Google Cloud Storage and get its location
            blob = storage_client.bucket(bucket_name).blob(filename)
            blob.upload_from_filename(f"temp/{filename}")
            file_location = blob.public_url
            # Delete the file from the temporary location
            os.remove(f"temp/{filename}")
            # Store the file size, extension and location to Google Cloud SQL
            sql_client.execute_sql(
                database=database_name,
                sql=f"INSERT INTO files (size, extension, location) VALUES ({file_size}, '{file_extension}', '{file_location}')"
            )
        # Return a success message
        return {"message": "ZIP file uploaded successfully"}
    else:
        # Return an error message if the file is not a ZIP file
        return {"error": "Please upload a valid ZIP file"}