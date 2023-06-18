# Import the required packages
from google.cloud import storage
import zipfile
import os

# Define a class for uploading files to Google Cloud Storage
class Uploader:
    # Initialize the class with the bucket name and the storage client
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    # Define a method for uploading a ZIP file and extracting its contents
    def upload_zip(self, zip_file):
        # Check if the file is a ZIP file
        if zip_file.filename.endswith(".zip"):
            # Save the ZIP file to a temporary location
            temp_zip_file = f"temp/{zip_file.filename}"
            with open(temp_zip_file, "wb") as f:
                f.write(zip_file.file.read())
            # Open the ZIP file and extract its contents
            with zipfile.ZipFile(temp_zip_file, "r") as z:
                z.extractall("temp")
            # Delete the ZIP file from the temporary location
            os.remove(temp_zip_file)
            # Loop through the extracted files
            for filename in os.listdir("temp"):
                # Upload the file to Google Cloud Storage and get its location
                blob = self.storage_client.bucket(self.bucket_name).blob(filename)
                blob.upload_from_filename(f"temp/{filename}")
                file_location = blob.public_url
                # Delete the file from the temporary location
                os.remove(f"temp/{filename}")
                # Print the file location
                print(f"Uploaded {filename} to {file_location}")
        else:
            # Print an error message if the file is not a ZIP file
            print("Please upload a valid ZIP file")