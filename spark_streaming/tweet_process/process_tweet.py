from pyspark.sql import SparkSession
from pathlib import Path
import sys

# Project Directories
ROOT = Path(__file__).parent.parent.parent
# Append the path
sys.path.append(f'{ROOT}')

from spark_streaming.utils import read_kafka_streams
from process_tweet_functions import transform_tweet_info 
from config import core, schema

 
# config file
APP_CONFIG = schema.SparkConfig(**core.load_config().data["spark_config"])

# spark session
spark = SparkSession.builder.appName(APP_CONFIG.app_name ).getOrCreate() 

# read data stream from kafka 
data_stream = read_kafka_streams(
                                spark = spark,
                                address = APP_CONFIG.bootstrap_servers,
                                topic = APP_CONFIG.topic_tweets
                                 ) 

data_stream = transform_tweet_info(data_stream)

# Écrivez les résultats dans la console pour le débogage
query = data_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
