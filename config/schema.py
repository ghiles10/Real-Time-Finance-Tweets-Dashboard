from typing import Dict, List, Optional, Sequence
from pydantic import BaseModel


class TwitterConfig(BaseModel): 
    
    """ twitter credential class """
    
    ACCES_KEY : str
    ACCES_SECRET : str
    CONSUMER_KEY : str
    CONSUMER_SECRET : str
    

class StocksConfig(BaseModel):
    
    """ stocks config class """
    
    url_symbols : str
    timeout : int
    url_data : str 

class kafkaConfig(BaseModel):
    
    """ kafka config class """
  
    topic_data: str 
    topic_tweets: str
    bootstrap_servers: str
    
class SparkConfig(kafkaConfig, BaseModel): 
    """spark config class  """
    
    master: str
    app_name: str
    checkpoint_path: str
    data_path: str
    batch_duration: str
    outputMode: str 

class MongoDB(BaseModel):
    
    """ mongoDB config class """
    
    db_connection: str
    bucket_name : str
    
class ElasticSearch(BaseModel):
    
    """ ElasticSearch config class """
    
    cloud_id: str
    user: str
    password : str
    index_name: str
    
class BigQuery(BaseModel):
    
    """ BigQuery config class """
    
    project_id: str
    dataset_id: str
    table_id: str
    bucket_name: str
    google_application_credentials: str