from fastapi import FastAPI, UploadFile, File, HTTPException
from google.cloud import storage, bigquery, vision_v1, dlp_v2
from google.protobuf.json_format import MessageToDict
import os
import io
import asyncio
import logging
from typing import List
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
import datetime
import uuid
from google.api_core.client_options import ClientOptions
from google.cloud import documentai
from starlette.responses import StreamingResponse


app = FastAPI(
    title="Obscurer",
    description="This project is a FastAPI application that takes image, pdf, or docx files as input and converts them to a readable text file, then applies PII (Personally Identifiable Information) deidentification and finally gives that as the output to the user.",
    version="1.0.0",
    docs_url="/",
    redoc_url="/cw-redoc",
    contact={
        "name": "Developer - Sagnik Das, Somdutta Paul, Tania Rana",
        "email": "sagnik.das03@infosys.com",
    },
)

PROJECT_ID = "casacasa-390303"
GCS_BUCKET = "sample1112345"
PROCESSOR_ID = "d13502f20685f48"
BQ_DATASET = "metadata"
LOCATION = "us"  # Replace with your processor's location
PRIMARY_BQ_TABLE = "raw_files"

gcs_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)
vision_client = vision_v1.ImageAnnotatorClient()
dlp_client = dlp_v2.DlpServiceClient()

table_ref = bq_client.dataset(BQ_DATASET).table(PRIMARY_BQ_TABLE)
table = bq_client.get_table(table_ref)
schema = table.schema

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Endpoint for uploading and start of data pipeline
@app.post("/upload",tags=["Data Pipeline"], name="Upload Multiple Files and Start Pipeline")
async def upload_files(files: List[UploadFile] = File(...)):
    try:
        for file in files:
            logger.info(f"Processing file: {file.filename}")

            # Store the uploaded file in Google Cloud Storage
            blob = gcs_client.bucket(GCS_BUCKET).blob(file.filename)
            blob.upload_from_file(file.file)

            logger.info(f"Uploaded file '{file.filename}' to Google Cloud Storage")

            # Store metadata information in BigQuery
            metadata = {
                "uuid": str(uuid.uuid4()),
                "filename": file.filename,
                "content_type": file.content_type,
                "size": file.file.seek(0, os.SEEK_END),
                "recordstamp": datetime.datetime.now(),
                "operation" : "I",
            }
            table = bq_client.dataset(BQ_DATASET).table(PRIMARY_BQ_TABLE)
            row = (metadata,)
            bq_client.insert_rows(table, row, selected_fields=schema)  # Provide the table schema explicitly

            logger.info(f"Inserted metadata for file '{file.filename}' into BigQuery")

            # Process the uploaded file asynchronously
            asyncio.create_task(process_file(blob))
        # Update Metadata info to BQ
        asyncio.create_task(metadata_handler())
        return {"process": "Files uploaded and processing pipeline started."}
    except Exception as e:
        logger.error(f"Error occured while upload: {e}")
        raise HTTPException(status_code=412, detail="Couldn't process request at this time. Please try again later")

# Data Pipeline Process
async def process_file(blob):
    try:
        logger.info(f"Processing file: {blob.name}")
        # Read the file into memory
        image_content = blob.download_as_bytes()

            # Determine the file extension from the blob name
        file_extension = blob.name.split(".")[-1].lower()
        
        # Skip Document AI processing for .txt files
        if file_extension == "txt":
            # Read the file content
            text_content = blob.download_as_text()
            
            # Remove PII information using Google Cloud DLP
            dlp_request = {
                "parent": f"projects/{PROJECT_ID}/locations/global",
                "item": {"value": text_content},
                "inspect_config": {
                    "info_types": [
                        {"name": "PHONE_NUMBER"},
                        {"name": "EMAIL_ADDRESS"},
                        {"name": "PERSON_NAME"},
                        {"name": "LOCATION"},
                        {"name": "AGE"},
                    ]
                },
                "deidentify_config": {
                    "info_type_transformations": {
                        "transformations": [
                            {"primitive_transformation": {"replace_with_info_type_config": {}}}
                        ]
                    }
                },
            }
            dlp_response = dlp_client.deidentify_content(dlp_request)
            deidentified_text = dlp_response.item.value

            # Store the deidentified text in a different folder in the same bucket
            deidentified_blob = gcs_client.bucket(GCS_BUCKET).blob(f"deidentified/{blob.name}.txt")
            deidentified_blob.upload_from_string(deidentified_text)

            logger.info(f"Stored deidentified text for file '{blob.name}' in Google Cloud Storage")
        else:
            mime_type = ""
            # Set the mime_type based on the file extension
            if file_extension == "pdf":
                mime_type = "application/pdf"
            elif file_extension == "gif":
                mime_type = "image/gif"
            elif file_extension in ["tiff", "tif"]:
                mime_type = "image/tiff"
            elif file_extension in ["jpg", "jpeg"]:
                mime_type = "image/jpeg"
            elif file_extension == "png":
                mime_type = "image/png"
            elif file_extension == "bmp":
                mime_type = "image/bmp"
            elif file_extension == "webp":
                mime_type = "image/webp"
            else:
                logger.error(f"Unsupported file type: {file_extension}")
                return
            
            # Load Binary Data into Document AI RawDocument Object
            raw_document = documentai.RawDocument(content=image_content, mime_type=mime_type)

            # Configure the process request
            docai_client = documentai.DocumentProcessorServiceClient(
            client_options=ClientOptions(api_endpoint=f"{LOCATION}-documentai.googleapis.com")
        )
            RESOURCE_NAME = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
            request = documentai.ProcessRequest(name=RESOURCE_NAME, raw_document=raw_document)

            # Use the Document AI client to process the document
            result = docai_client.process_document(request=request)

            document_object = result.document
            annotations = document_object.text

            if annotations:
                # Store the processed text in a different folder in the same bucket
                processed_blob = gcs_client.bucket(GCS_BUCKET).blob(f"processed/{blob.name}.txt")
                processed_blob.upload_from_string(annotations)

                logger.info(f"Stored processed text for file '{blob.name}' in Google Cloud Storage")

                # Remove PII information using Google Cloud DLP
                dlp_request = {
                    "parent": f"projects/{PROJECT_ID}/locations/global",
                    "item": {"value": annotations},
                    "inspect_config": {
                        "info_types": [
                            {"name": "PHONE_NUMBER"},
                            {"name": "EMAIL_ADDRESS"},
                            {"name": "PERSON_NAME"},
                            {"name": "LOCATION"},
                            {"name": "AGE"},
                        ]
                    },
                    "deidentify_config": {
                        "info_type_transformations": {
                            "transformations": [
                                {"primitive_transformation": {"replace_with_info_type_config": {}}}
                            ]
                        }
                    },
                }
                dlp_response = dlp_client.deidentify_content(dlp_request)
                deidentified_text = dlp_response.item.value

                # Store the deidentified text in a different folder in the same bucket
                deidentified_blob = gcs_client.bucket(GCS_BUCKET).blob(f"deidentified/{blob.name}.txt")
                deidentified_blob.upload_from_string(deidentified_text)

                logger.info(f"Stored deidentified text for file '{blob.name}' in Google Cloud Storage")
    except Exception as e:
        logger.error(f"Uploader script failed due to {e}")

# Endpoint useful for fetching data as JSON
@app.post("/fetch",tags=["Stream Data"], name="Fetch PII Deidentified Data as JSON")
async def fetch_processed_text(name: str):
    logger.info(f"Fetching processed text for name: {name}")

    # Fetch processed text from Google Cloud Storage based on name
    blobs = gcs_client.bucket(GCS_BUCKET).list_blobs(prefix="deidentified/")
    text_files = [blob.name for blob in blobs if blob.name.endswith(".txt")]

    matching_files = []
    for file in text_files:
        if name in file:
            matching_files.append(file)

    texts = []
    for file in matching_files:
        blob = gcs_client.bucket(GCS_BUCKET).blob(file)
        texts.append(blob.download_as_text())

    logger.info(f"Fetched processed text for name '{name}': {texts}")

    return {"texts": texts}

# Endpoint useful for downloading text
@app.post("/download",tags=["Stream Data"], name="Download PII Deidentified Data")
async def download_processed_text(name: str):
    logger.info(f"Downloading processed text for name: {name}")

    # Fetch processed text from Google Cloud Storage based on name
    blobs = gcs_client.bucket(GCS_BUCKET).list_blobs(prefix="deidentified/")
    text_files = [blob.name for blob in blobs if blob.name.endswith(".txt")]

    matching_files = []
    for file in text_files:
        if name in file:
            matching_files.append(file)

    texts = []
    for file in matching_files:
        blob = gcs_client.bucket(GCS_BUCKET).blob(file)
        texts.append(blob.download_as_text())

    # Create a single text file containing all the fetched texts
    combined_text = "\n".join(texts).encode()

    # Create a file-like stream for the combined text
    stream = io.BytesIO(combined_text)

    # Create a StreamingResponse to return the file as an attachment
    return StreamingResponse(
        stream,
        media_type="text/plain",
        headers={"Content-Disposition": f"attachment; filename={name}_processed.txt"},
    )

# Update metadata content for a given folder in a given BQ table
async def process_bucket(table_name, folder_name=None):
    # Get the bucket
    bucket = gcs_client.get_bucket(GCS_BUCKET)

    # List all blobs in the bucket or folder
    blobs = bucket.list_blobs(prefix=folder_name) if folder_name else bucket.list_blobs()

    # Create a BigQuery table if it doesn't exist
    table_id = f"{bq_client.project}.{BQ_DATASET}.{table_name}"
    schema = [
        bigquery.SchemaField("filename", "STRING"),
        bigquery.SchemaField("size", "INTEGER"),
        bigquery.SchemaField("created", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = bq_client.create_table(table, exists_ok=True)

    # Process each blob and store metadata in BigQuery
    import json
    from datetime import datetime

    # Process each blob and store metadata in BigQuery
    rows_to_insert = []
    for blob in blobs:
        row = {
            "filename": blob.name,
            "size": blob.size,
            "created": blob.time_created.isoformat(),  # Convert datetime to ISO 8601 string
        }
        rows_to_insert.append(row)

    if rows_to_insert:
        # Create a BigQuery table if it doesn't exist
        table_id = f"{bq_client.project}.{BQ_DATASET}.{table_name}"
        schema = [
            bigquery.SchemaField("filename", "STRING"),
            bigquery.SchemaField("size", "INTEGER"),
            bigquery.SchemaField("created", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)

        # Insert rows into the BigQuery table and overwrite existing data
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        load_job = bq_client.load_table_from_json(
            rows_to_insert,
            table_id,
            job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        if load_job.errors:
            logger.error(f"Error occurred while inserting rows: {load_job.errors}")
        else:
            logger.info(f"Metadata stored in BigQuery table {table_name} successfully.")
    else:
        logger.info(f"No files found in the specified bucket {GCS_BUCKET} / folder {folder_name}.")

# Runs metadata updation for each folder
async def metadata_handler():
    try:
        await process_bucket("raw_file_meta_direct")
        await process_bucket("processed_meta_direct","processed")
        await process_bucket("deidentified_meta_direct","deidentified")
    except Exception as e:
        logger.error(f"Uploader script failed due to {e}")

# Endpoint is useful for manual metadata updation
@app.put("/update_bq",tags=["Data Pipeline"], name="Manual Metadata Update")
async def force_update_metadata():
    try:
        asyncio.create_task(metadata_handler())
        return {"process": "Metadata handler enabled, BigQuery tables update started."}
    except Exception as e:
        logger.error(f"Error occured while metadata update: {e}")
        raise HTTPException(status_code=412, detail="Couldn't process request at this time. Please try again later")