import sys 
import datetime
from airflow.operators.dummy_operator import DummyOperator 
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(r'/opt/airflow/src') 

from load.mongo_db.laod_collections import load_collections 
from load.elastic.index_collections import index_tweets 
from config import schema, core

APP_CONFIG = schema.BigQuery(**core.load_config().data["monog_db"])

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
} 

# Define DAG
with DAG(
    dag_id="NoSQL_tweets",
    default_args=default_args,
    description="data pipeline to load and index tweets in elastic search and mongo db",
    schedule_interval="30 * * * *",
    start_date=datetime.datetime.today(),
    catchup=False,
    max_active_runs=3,
) as dag:
    
 
    load_tweet_mongodb = PythonOperator(
        python_callable=load_collections,
        task_id="load tweet in mongo db"
    )
    
    index_tweet_elastic = PythonOperator( 
        python_callable=index_tweets, 
        task_id="index tweet in elastic search" 
    ) 
    

load_tweet_mongodb >> index_tweet_elastic 