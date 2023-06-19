from google.cloud import storage

storage_client = storage.Client()

def blob_metadata(bucket_name, blob_name):
    """Prints out a blob's metadata."""
    # bucket_name = 'your-bucket-name'
    # blob_name = 'your-object-name'
    bucket = storage_client.bucket(bucket_name)

    # Retrieve a blob, and its metadata, from Google Cloud Storage.
    # Note that `get_blob` differs from `Bucket.blob`, which does not
    # make an HTTP request.
    blob = bucket.get_blob(blob_name)
    blob_dict = {
        "Blob": blob.name,
        "Bucket": blob.bucket.name,
        "Storage class": blob.storage_class,
        "ID": blob.id,
        "Size": f"{blob.size} bytes",
        "Updated": blob.updated,
        "Generation": blob.generation,
        "Metageneration": blob.metageneration,
        "Etag": blob.etag,
        "Owner": blob.owner,
        "Component count": blob.component_count,
        "Crc32c": blob.crc32c,
        "md5_hash": blob.md5_hash,
        "Cache-control": blob.cache_control,
        "Content-type": blob.content_type,
        "Content-disposition": blob.content_disposition,
        "Content-encoding": blob.content_encoding,
        "Content-language": blob.content_language,
        "Metadata": blob.metadata,
        "Medialink": blob.media_link,
        "Custom Time": blob.custom_time,
        "Temporary hold": "enabled" if blob.temporary_hold else "disabled",
        "Event based hold": "enabled" if blob.event_based_hold else "disabled",
    }
    if blob.retention_expiration_time:
        blob_dict["retentionExpirationTime"] = blob.retention_expiration_time

    return blob_dict

def get_blob_names(bucket_name):
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs())
    return blobs

def compile_metadata(bucket_name):
    blobs = get_blob_names(bucket_name)
    blob_dict = dict()
    for blob in blobs:
        blob_dict[blob.name] = blob_metadata(bucket_name,blob.name)
    return blob_dict

print(compile_metadata("caca123456"))