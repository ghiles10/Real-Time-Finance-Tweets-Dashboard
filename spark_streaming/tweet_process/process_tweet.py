import sys 
from pathlib import Path
from pyspark.sql import SparkSession 

# Project Directories
ROOT = Path(__file__).parent.parent.parent
# Append the path
sys.path.append(f'{ROOT}')

from spark_streaming.utils import read_kafka_streams
from spark_streaming.tweet_process.process_tweet_functions import transform_tweet_info 
from config import core, schema

 
# config file
APP_CONFIG = schema.SparkConfig(**core.load_config().data["spark_config"])

# spark session
spark = SparkSession.builder.appName(APP_CONFIG.app_name ).getOrCreate() 

def process_tweet(spark) -> None: 

    # read data stream from kafka 
    data_stream = read_kafka_streams(
                                    address = APP_CONFIG.bootstrap_servers,
                                    topic = APP_CONFIG.topic_tweets
                                    ) 

    data_stream = transform_tweet_info(data_stream)

    ( data_stream.writeStream.outputMode(APP_CONFIG.outputMode)
    .format("json")
    .option("path", str(APP_CONFIG.data_path) + "/" + "tweets" )
    .option("checkpointLocation", APP_CONFIG.checkpoint_path + '/' + "tweets")
    .trigger( processingTime= str(APP_CONFIG.batch_duration )) 
    .start().awaitTermination()
    )


if __name__ == "__main__":
    process_tweet(spark) 