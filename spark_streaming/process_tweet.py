from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    expr,
    from_json,
    month,
    hour,
    dayofmonth,
    col,
    year,
    split,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder.appName("SparkStreaming").getOrCreate() 

# Définition du flux d'entrée
data_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "10.200.0.2:9092")
    .option("subscribe", 'finance')
    .option("startingOffsets", "earliest")
    .load()
)


def preprocess_stream(data_stream, topic="finance"):
    """Fonction pour prétraiter un flux de données à partir d'un topic Kafka"""

    # Définition du schéma des données JSON
    json_schema = StructType(
        [
            StructField("time", LongType()),
            StructField("symbol", StringType()),
            StructField("buy", DoubleType()),
            StructField("sell", DoubleType()),
            StructField("changeRate", DoubleType()),
            StructField("changePrice", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("vol", DoubleType()),
            StructField("volValue", DoubleType()),
            StructField("last", DoubleType()),
            StructField("averagePrice", DoubleType()),
            StructField("takerFeeRate", DoubleType()),
            StructField("makerFeeRate", DoubleType()),
            StructField("takerCoefficient", DoubleType()),
            StructField("makerCoefficient", DoubleType()),
        ]
    )

    # Transformation de la colonne "value" de string à JSON et sélection des champs
    data_stream_json = data_stream.select(
        from_json(expr("CAST(value AS STRING)"), json_schema).alias("json_data")
    ).selectExpr(
        "json_data.time",
        "json_data.symbol",
        "json_data.buy",
        "json_data.sell",
        "json_data.changeRate",
        "json_data.changePrice",
        "json_data.high",
        "json_data.low",
        "json_data.vol",
        "json_data.volValue",
        "json_data.last",
        "json_data.averagePrice",
        "json_data.takerFeeRate",
        "json_data.makerFeeRate",
        "json_data.takerCoefficient",
        "json_data.makerCoefficient",
    )
    
    data_stream_json.writeStream.format("console").start().awaitTermination() 

preprocess_stream(data_stream)