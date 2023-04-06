import pymongo
import sys
from pathlib import Path 

ROOT = Path(__file__).parent.parent.parent 
sys.path.append(f"{ROOT}")

from config import schema, core

APP_CONFIG = schema.MongoDB(**core.load_config().data["monog_db"])

# Connect to MongoDB
client = pymongo.MongoClient(APP_CONFIG.db_connection) 
bucket_name = APP_CONFIG.bucket_name
