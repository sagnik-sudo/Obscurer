from fastapi import FastAPI, UploadFile, File, HTTPException
from google.cloud import storage, bigquery, dlp_v2
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
from io import StringIO
import pandas as pd
import json
import glob
from google.cloud import language_v1

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

PROJECT_ID = "gcds-oht33219u9-2023"
GCS_BUCKET = "obscurer_store"
PROCESSOR_ID = "917f5f3b88b6cd3d"
BQ_DATASET = "obscurer_meta"
LOCATION = "us"  # Replace with your processor's location
PRIMARY_BQ_TABLE = "raw_files"
DRUG_DB_TABLE = "drug_database"
REPORTING_DATASET = "obscurer_reporting"

gcs_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)
language_client = language_v1.LanguageServiceClient()
dlp_client = dlp_v2.DlpServiceClient()
sql_files = glob.glob("./sql/*.sql")

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


@app.post("/upload", tags=["Data Pipeline"],
          name="Upload Multiple Files and Start Pipeline")
async def upload_files(files: List[UploadFile] = File(...)):
    """Endpoint for uploading and start of data pipeline"""
    try:
        for file in files:
            logger.info(f"Upload process started: {file.filename}")

            # Store the uploaded file in Google Cloud Storage
            blob = gcs_client.bucket(GCS_BUCKET).blob(file.filename)
            blob.upload_from_file(file.file)
            logger.info(
                f"SUCCESS: '{file.filename}' uploaded to Google Cloud Storage")

            # Store metadata information in BigQuery
            metadata = {
                "uuid": str(uuid.uuid4()),
                "filename": file.filename,
                "content_type": file.content_type,
                "size": file.file.seek(0, os.SEEK_END),
                "recordstamp": datetime.datetime.now(),
                "operation": "I",
            }
            table = bq_client.dataset(BQ_DATASET).table(PRIMARY_BQ_TABLE)
            row = (metadata,)
            # Provide the table schema explicitly
            bq_client.insert_rows(table, row, selected_fields=schema)

            logger.info(
                f"SUCCESS: Inserted metadata for file '{file.filename}' into BigQuery")

            # Process the uploaded file asynchronously
            asyncio.create_task(process_file(blob))
        # Update Metadata info to BQ
        asyncio.create_task(metadata_handler())
        # Start Medicine Name Extraction Process
        asyncio.create_task(analyze_and_insert_data())
        return {"process": "Files uploaded and processing pipeline started."}
    except Exception as e:
        logger.error(f"CAUTION: Error occured while upload: {e}")
        raise HTTPException(
            status_code=412,
            detail="Couldn't process request at this time. Please try again later")


async def process_file(blob):
    """Data Pipeline Process"""
    try:
        logger.info(f"Currently processing file: {blob.name}")
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

            # Store the deidentified text in a different folder in the same
            # bucket
            deidentified_blob = gcs_client.bucket(
                GCS_BUCKET).blob(f"deidentified/{blob.name}.txt")
            deidentified_blob.upload_from_string(deidentified_text)
            send_text_bq(blob.name,deidentified_text)
            logger.info(
                f"SUCCESS: Stored deidentified text for file '{blob.name}' in Google Cloud Storage")
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
                logger.error(f"CAUTION: Unsupported file type: {file_extension}")
                return

            # Load Binary Data into Document AI RawDocument Object
            raw_document = documentai.RawDocument(
                content=image_content, mime_type=mime_type)

            # Configure the process request
            docai_client = documentai.DocumentProcessorServiceClient(
                client_options=ClientOptions(
                    api_endpoint=f"{LOCATION}-documentai.googleapis.com"))
            RESOURCE_NAME = docai_client.processor_path(
                PROJECT_ID, LOCATION, PROCESSOR_ID)
            request = documentai.ProcessRequest(
                name=RESOURCE_NAME, raw_document=raw_document)

            # Use the Document AI client to process the document
            result = docai_client.process_document(request=request)

            document_object = result.document
            annotations = document_object.text

            if annotations:
                # Store the processed text in a different folder in the same
                # bucket
                processed_blob = gcs_client.bucket(
                    GCS_BUCKET).blob(f"processed/{blob.name}.txt")
                processed_blob.upload_from_string(annotations)

                logger.info(
                    f"SUCCESS: Stored processed text for file '{blob.name}' in Google Cloud Storage")

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

                # Store the deidentified text in a different folder in the same
                # bucket
                deidentified_blob = gcs_client.bucket(
                    GCS_BUCKET).blob(f"deidentified/{blob.name}.txt")
                deidentified_blob.upload_from_string(deidentified_text)
                send_text_bq(blob.name,deidentified_text)
                logger.info(
                    f"SUCCESS: Stored deidentified text for file '{blob.name}' in Google Cloud Storage")
    except Exception as e:
        logger.error(f"CAUTION: Uploader script failed due to {e}")


@app.post("/fetch", tags=["Stream Data"],
          name="Fetch PII Deidentified Data as JSON")
async def fetch_processed_text(name: str):
    """Endpoint useful for fetching data as JSON"""
    logger.info(f"Currently fetching processed text for name: {name}")

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

    logger.info(f"SUCCESS: Fetched processed text for name '{name}': {texts}")

    return {"texts": texts}


@app.post("/download", tags=["Stream Data"],
          name="Download PII Deidentified Data")
async def download_processed_text(name: str):
    """Endpoint useful for downloading text"""
    logger.info(f"Currently downloading processed text for name: {name}")

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
        headers={
            "Content-Disposition": f"attachment; filename={name}_processed.txt"},
    )


async def process_bucket(table_name, folder_name=None):
    """Update metadata content for a given folder in a given BQ table"""
    # Get the bucket
    bucket = gcs_client.get_bucket(GCS_BUCKET)

    # List all blobs in the bucket or folder
    blobs = bucket.list_blobs(
        prefix=folder_name) if folder_name else bucket.list_blobs()

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
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        load_job = bq_client.load_table_from_json(
            rows_to_insert,
            table_id,
            job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        if load_job.errors:
            logger.error(
                f"CAUTION: Error occurred while inserting rows: {load_job.errors}")
        else:
            logger.info(
                f"SUCCESS: Metadata stored in BigQuery table {table_name}.")
    else:
        logger.info(
            f"No files found in the specified bucket {GCS_BUCKET} / folder {folder_name}.")


async def metadata_handler():
    """Runs metadata updation for each folder"""
    try:
        await process_bucket("raw_file_meta_direct")
        await process_bucket("processed_meta_direct", "processed")
        await process_bucket("deidentified_meta_direct", "deidentified")
    except Exception as e:
        logger.error(f"CAUTION: Uploader script failed due to {e}")


@app.patch("/update_metatables", tags=["Data Pipeline"], name="Manual Metadata Update")
async def force_update_metadata():
    """Endpoint is useful for manual metadata updation"""
    try:
        asyncio.create_task(metadata_handler())
        return {
            "process": "Metadata handler enabled, BigQuery tables update started."}
    except Exception as e:
        logger.error(f"CAUTION: Error occured while metadata update: {e}")
        raise HTTPException(
            status_code=412,
            detail="Couldn't process request at this time. Please try again later")


async def get_processed_status():
    """Function to get proccessed status from Big Query"""
    query = f"SELECT * FROM `{PROJECT_ID}.{REPORTING_DATASET}.processed_view`"
    results = bq_client.query(query)
    processed_dict = dict()
    for row in results:
        processed_dict[row['file_name']] = row['size']
    logger.info("SUCCESS: Prepared Dictionary for Processed View")
    query = f"SELECT * FROM `{PROJECT_ID}.{REPORTING_DATASET}.deidentified_view`"
    results = bq_client.query(query)
    deidentified_dict = dict()
    for row in results:
        deidentified_dict[row['file_name']] = row['size']
    logger.info("SUCCESS: Prepared Dictionary for Deidentified View")
    return {"files_deidentified_count":len(deidentified_dict),
            "deidentify_complete_files": deidentified_dict}


@app.post("/processed_files_list",
          tags=["Data Pipeline"],
          name="Fetch List Of File Processed")
async def fetch_processed_status():
    """Endpoint is useful for fetching list of files processed"""
    try:
        result = await get_processed_status()
        logger.info("SUCCESS: Processed File List Fetched")
        return result
    except Exception as e:
        logger.error(f"CAUTION: Error occured while fetching file process status: {e}")
        raise HTTPException(
            status_code=412,
            detail="Couldn't process request at this time. Please try again later")


async def run_sql_file(sql_file):
    """Define an async function to run a sql file in bigquery"""
    with open(sql_file, "r") as f:
        query = f.read()
    logger.info(f"Now Running BigQuery Interactive Query File -> {sql_file}")
    # Run the query asynchronously and return the job object
    job = bq_client.query(query, job_config=bigquery.QueryJobConfig(priority=bigquery.QueryPriority.INTERACTIVE))
    return job


@app.patch("/update_bq_schema", tags=["Data Pipeline"], name="Update/Fix Big Query View Schema")
async def update_bq_schema():
    """Define an endpoint to run all the sql files in parallel"""
    tasks = [asyncio.create_task(run_sql_file(sql_file)) for sql_file in sql_files]
    task_name = [{"task_id": task.get_name()} for task in tasks]
    logger.info(f"BigQuery Interactive SQL Update is processing -> {task_name}")
    return {"process":"Schema update mechanism started. Please check status in sometime."}


def send_text_bq(filename, deidentified_text):
    """Define a function that takes filename and deidentified text as input and inserts them into the table"""
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.deidentified_text"
    # Create a row dictionary with the column names and values
    row = {"filename": filename, "deidentified_text": deidentified_text,"recordstamp": str(datetime.datetime.now())}
    # Insert the row into the table using the insert_rows_json method
    errors = bq_client.insert_rows_json(table_id, [row])
    # Check if there are any errors and print them
    if errors:
        logger.error(f"CAUTION: Error occured while adding {filename} to BQ:", errors)
    else:
        logger.info(f"SUCCESS: Added deidentified text for {filename} to BQ")


async def analyze_and_insert_data():
    try:
        # Define the BigQuery table schema for the output table
        output_table_schema = [
            bigquery.SchemaField("filename", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("deidentified_text", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("medicine_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("recordstamp", "TIMESTAMP", mode="REQUIRED")
        ]

        # Define the output table ID
        output_table_id = f"{PROJECT_ID}.{BQ_DATASET}.medicines_found"

        # Define the SQL query to fetch filenames and text from the input table
        input_table_query = f"""
            SELECT filename, deidentified_text, recordstamp
            FROM `{PROJECT_ID}.{BQ_DATASET}.deidentified_text`
        """

        # Perform the query to fetch filenames and text from the input table
        query_job = bq_client.query(input_table_query)
        results = query_job.result()

        # Prepare the rows for insertion into the output table
        rows = []
        for row in results:
            document = language_v1.Document(content=row.deidentified_text, type_=language_v1.Document.Type.PLAIN_TEXT)
            response = language_client.analyze_entities(request={"document": document})
            entities = response.entities
            medicine_names = [entity.name for entity in entities if entity.type == language_v1.Entity.Type.CONSUMER_GOOD]

            for medicine_name in medicine_names:
                rows.append({
                    "filename": row.filename,
                    "deidentified_text": row.deidentified_text,
                    "medicine_name": medicine_name,
                    "recordstamp": str(row.recordstamp)
                })

        # Define the job config with write disposition to overwrite existing data
        job_config = bigquery.LoadJobConfig(
            schema=output_table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Load the rows into the output table from JSON
        load_job = bq_client.load_table_from_json(
            rows,
            output_table_id,
            job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        if load_job.errors:
            logger.error(f"CAUTION: Error occured while medicine name extraction: {load_job.errors}")
        else:
            logger.info("SUCCESS: Medicine Names have been extracted.")
    except Exception as e:
        logger.error(f"CAUTION: Error occured while medicine name extraction: {e}")

