def read_kafka_streams(address, spark, topic="finance"):
    """lire un flux de données à partir d'un topic Kafka"""


    # Définition du flux d'entrée
    data_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", address)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    return data_stream