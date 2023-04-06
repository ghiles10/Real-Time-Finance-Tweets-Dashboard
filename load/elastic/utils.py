import sys
from pathlib import Path 
from elasticsearch import Elasticsearch

ROOT = Path(__file__).parent.parent.parent 
sys.path.append(f"{ROOT}")

from config import schema, core

# Load Kafka configuration from the config file
APP_CONFIG = schema.ElasticSearch(**core.load_config().data["elastic_search"])


# elastic connection
es = Elasticsearch(
    cloud_id= APP_CONFIG.cloud_id,
    basic_auth=( APP_CONFIG.user  , APP_CONFIG.password )
)

index_name = APP_CONFIG.index_name
