from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType

from pathlib import Path
import sys

# Project Directories
ROOT = Path(__file__).parent.parent.parent
# Append the path
sys.path.append(f'{ROOT}')

from spark_streaming.utils import read_kafka_streams
from config import core, schema 

# config file
APP_CONFIG = schema.SparkConfig(**core.load_config().data["spark_config"])

# spark session
spark = SparkSession.builder.appName(APP_CONFIG.app_name ).getOrCreate() 

# read data stream from kafka 
data_stream = read_kafka_streams(spark = spark, address = APP_CONFIG.bootstrap_servers, topic = APP_CONFIG.topic_tweets) 

# Convertissez les données JSON au format texte pur
data_stream_text = data_stream.selectExpr("CAST(value AS STRING)")

# Inférez le schéma JSON à partir des données (vous pouvez également définir le schéma manuellement)
json_schema = data_stream_text.select("value")

# Écrivez les résultats dans la console pour le débogage
query = json_schema \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
