from pathlib import Path
import sys
from pyspark.sql import SparkSession

# Project Directories
ROOT = Path(__file__).parent.parent.parent
# Append the path
sys.path.append(f'{ROOT}')

from process_finance_functions import preprocess_finance_stream, nested_data_finance_stream
from spark_streaming.utils import read_kafka_streams
from config import core, schema 


# config file
APP_CONFIG = schema.SparkConfig(**core.load_config().data["spark_config"])

# spark session
spark = SparkSession.builder.appName(APP_CONFIG.app_name ).getOrCreate() 

# read data stream from kafka 
data_stream = read_kafka_streams(spark = spark, address = APP_CONFIG.bootstrap_servers, topic = APP_CONFIG.topic_data) 

# pre process data stream 
processed_stream = preprocess_finance_stream(data_stream)

# nested data stream to json format
data_json = nested_data_finance_stream(processed_stream)

query = (
    data_json.writeStream.outputMode(APP_CONFIG.outputMode)
    .format("json")
    .option("path", APP_CONFIG.data_path)s
    .option("checkpointLocation", APP_CONFIG.checkpoint_path)
    .trigger( APP_CONFIG.batch_duration )
    .start()
)

query.awaitTermination()
