from fastapi import FastAPI, UploadFile, File
from google.cloud import storage, bigquery, vision_v1, dlp_v2
from google.protobuf.json_format import MessageToDict
import os
import asyncio
from typing import List
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

app = FastAPI()
project_id = "casacasa-390303"
bucket_name = "sample1112345"
gcs_client = storage.Client(project=project_id)
bq_client = bigquery.Client(project=project_id)
vision_client = vision_v1.ImageAnnotatorClient()
dlp_client = dlp_v2.DlpServiceClient()

table_ref = bq_client.dataset("metadata").table("files")
table = bq_client.get_table(table_ref)
schema = table.schema


@app.post("/upload")
async def upload_files(files: List[UploadFile] = File(...)):
    for file in files:
        # Store the uploaded file in Google Cloud Storage
        blob = gcs_client.bucket(bucket_name).blob(file.filename)
        blob.upload_from_file(file.file)

        # Store metadata information in BigQuery
        metadata = {
            "filename": file.filename,
            "content_type": file.content_type,
            "size": file.file.seek(0, os.SEEK_END),
        }
        table = bq_client.dataset("metadata").table("files")
        row = (metadata,)
        bq_client.insert_rows(table, row, selected_fields=schema)  # Provide the table schema explicitly

        # Process the uploaded file asynchronously
        asyncio.create_task(process_file(blob))

    return {"message": "Files uploaded and processing started."}


async def process_file(blob):
    # Process the file using Google Cloud Vision
    response = vision_client.text_detection({
        "source": {"image_uri": f"gs://{bucket_name}/{blob.name}"}
    })

    if response.text_annotations:
        annotations = response.text_annotations[0].description
    else:
        annotations = ""

    if annotations:
        # Store the processed text in a different folder in the same bucket
        processed_blob = gcs_client.bucket(bucket_name).blob(f"processed/{blob.name}.txt")
        processed_blob.upload_from_string(annotations)

        # Remove PII information using Google Cloud DLP
        dlp_request = {
            "parent": f"projects/{project_id}/locations/global",
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
        deidentified_blob = gcs_client.bucket(bucket_name).blob(f"deidentified/{blob.name}.txt")
        deidentified_blob.upload_from_string(deidentified_text)


@app.get("/fetch/{name}")
async def fetch_processed_text(name: str):
    # Fetch processed text from Google Cloud Storage based on name
    blobs = gcs_client.bucket(bucket_name).list_blobs(prefix="deidentified/")
    text_files = [blob.name for blob in blobs if blob.name.endswith(".txt")]

    matching_files = []
    for file in text_files:
        if name in file:
            matching_files.append(file)

    texts = []
    for file in matching_files:
        blob = gcs_client.bucket(bucket_name).blob(file)
        texts.append(blob.download_as_text())

    return {"texts": texts}
