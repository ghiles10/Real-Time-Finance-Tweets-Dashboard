import sys 
import pymongo
import json
from google.cloud import storage
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent 
sys.path.append(f"{ROOT}")

from config import schema, core
from gcs_connection import list_blobs


# Load Kafka configuration from the config file
APP_CONFIG = schema.MongoDB(**core.load_config().data["monog_db"])


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
            
            
def load_collections(): 

    # Connect to MongoDB
    client = pymongo.MongoClient( str(APP_CONFIG.db_connection)   ) 

    # connect to database and send json files     
    bucket = APP_CONFIG.bucket_name
    db = client.twitter

    for filename in list_blobs('finance-dashbord',"data/tweets/"):
        
        if filename.endswith('.json'):
            insert_json_file( bucket,filename, db)

    db.close()




