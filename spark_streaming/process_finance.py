from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from process_finance_functions import  *


spark = SparkSession.builder.appName("SparkStreaming").getOrCreate() 

# Définition du flux d'entrée
data_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "10.200.0.2:9092")
    .option("subscribe", 'finance')
    .option("startingOffsets", "earliest")
    .load()
)


# Transformation du flux d'entrée 
processed_stream = preprocess_finance_stream(data_stream)
nested_data_finance_stream(processed_stream)
