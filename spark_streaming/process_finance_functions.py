from pyspark.sql.functions import (
    expr,
    from_json,
    month,
    hour,
    dayofmonth,
    col,
    year,
    split,
    struct
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def preprocess_finance_stream(data_stream):
    
    """process finance stream data"""

    # Json schema for the data stream
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

    # Transformation of the data stream 
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

    # Transformation of the "time" column 
    data_stream_json = (
        data_stream_json.withColumn("time", (col("time") / 1000).cast("timestamp"))
        .withColumn("year", year("time"))
        .withColumn("month", month("time"))
        .withColumn("day", dayofmonth("time"))
        .withColumn("hour", hour("time"))
    )

    # Split the "symbol" column to keep only the name of the crypto-currency
    data_stream_json = data_stream_json.withColumn(
        "symbol", split(data_stream_json["symbol"], "-")[0]
    )
    

    return data_stream

def nested_data_finance_stream(data_stream_json) : 
    
    """ denormalize data stream """
        
    # write to nested json
    data_stream_json = data_stream_json.withColumn("prices", struct(
        col("buy"), col("sell"), col("changeRate"), col("changePrice"),
        col("high"), col("low"), col("last"), col("averagePrice")
        
    )).withColumn("fees", struct(
        col("takerFeeRate"), col("makerFeeRate"), col("takerCoefficient"), col("makerCoefficient")
        
    )).withColumn("volume", struct(
        col("vol"), col("volValue")
        
    )).withColumn("time", struct(
        col("year"), col("month"), col("day"), col("hour")
        
    )).drop(
        "buy", "sell", "changeRate", "changePrice", "high", "low", "last", "averagePrice",
        "takerFeeRate", "makerFeeRate", "takerCoefficient", "makerCoefficient", "vol", "volValue", 
        "year", "month", "day", "hour"  
    )

    data_stream_json.writeStream.format("console").start().awaitTermination() 