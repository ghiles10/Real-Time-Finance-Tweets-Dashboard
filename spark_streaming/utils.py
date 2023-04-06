from pyspark.sql import SparkSession

def read_kafka_streams(address : str, spark : SparkSession, topic :str):
    """ read data from kafka topic and return a data stream"""

    data_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", address)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    return data_stream