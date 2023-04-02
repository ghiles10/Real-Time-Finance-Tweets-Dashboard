from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkStreaming").getOrCreate() 

# Définition du flux d'entrée
data_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "10.200.0.2:9092")
    .option("subscribe", 'finance')
    .option("startingOffsets", "earliest")
    .load()
)

data_stream.writeStream.format("console").start().awaitTermination() 

