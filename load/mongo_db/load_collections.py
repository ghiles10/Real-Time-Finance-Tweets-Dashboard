import pymongo
import json
from google.cloud import storage
from utils import client, bucket_name
from gcs_connection import list_blobs


def insert_json_file(bucket_name :str, file_path : str, db : pymongo.database.Database):
    
    """ Insert json file from gcs in MongoDB collection"""
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
          
    for objet in blob.download_as_text().splitlines(): 
        file_data = json.loads(objet)

        if isinstance(file_data, list):
            db.tweet.insert_many(file_data)
          
        else:
            db.tweet.insert_one(file_data)
            
            
def load_collections(client  = client, bucket = bucket_name): 
    """ Load all json tweets files from gcs in MongoDB collection """

  
    db = client.twitter

    for filename in list_blobs('financed-data',"data/tweets/"):
        
        if filename.endswith('.json'):
            insert_json_file( bucket,filename, db)

    db.close()


if __name__ == "__main__":
    load_collections()