from google.cloud import storage

# Define a class for uploading files to Google Cloud Storage
class Uploader:
    # Initialize the class with the bucket name and the storage client
    def __init__(self):
        pass

    # Creating a function to upload a file to Google Cloud Storage
    def upload_to_bucket(self,blob_name, path_to_file, bucket_name):
        """ Upload data to a bucket"""
        
        # Explicitly use service account credentials by specifying the private key file.
        storage_client = storage.Client.from_service_account_json('creds.json')
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path_to_file)
        #returns a public url
        return blob.public_url
