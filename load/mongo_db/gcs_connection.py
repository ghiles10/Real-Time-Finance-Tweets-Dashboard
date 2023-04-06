from google.cloud import storage


def list_blobs(bucket_name, prefix):
    
    """list objects in a bucket with a given prefix"""
        
    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    for blob in blobs:
        if "_spark_metadata" not in blob.name:
            yield blob.name 
