# Import the required packages
from google.cloud import storage
import zipfile
import os

# Define a class for uploading files to Google Cloud Storage
class Uploader:
    # Initialize the class with the bucket name and the storage client
    def __init__(self):
        pass

    # Creating a function to upload a file to Google Cloud Storage
    def upload_to_gcs(self,file_path, bucket_name, destination_blob_name):
        # Initializing a storage client
        storage_client = storage.Client()
        # Getting the bucket object
        bucket = storage_client.bucket(bucket_name)
        # Creating a blob object
        blob = bucket.blob(destination_blob_name)
        # Uploading the file to the blob
        blob.upload_from_filename(file_path)
        # Printing the public URL of the blob
        print(f"File {file_path} uploaded to {blob.public_url}.")
