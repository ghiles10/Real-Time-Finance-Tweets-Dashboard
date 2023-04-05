import datetime
from pyspark.sql.functions import regexp_replace, udf, lower
from pyspark.sql.types import TimestampType


def transform_tweet_info(data_stream) : 
    
    """ this function transform the tweets to exploitable data """
    
    # Get actual time 
    def current_time():
        return datetime.datetime.now()

    udf_current_time = udf(current_time, TimestampType())
    
    #convert data stream to string
    data_stream_value = data_stream.selectExpr("CAST(value AS STRING)")
    json_schema = data_stream_value.select("value")
        
    # delete special characters
    tweet_df = json_schema.withColumn("value", regexp_replace("value", "[^a-zA-Z\\s]", "")) \
            .withColumn("value", regexp_replace("value", "^@\\w+", "")) 
    
    # convert to lower case
    tweet_df = tweet_df.withColumn("value", lower(tweet_df["value"])) 
    
    # delete url
    tweet_df = tweet_df.withColumn("value", regexp_replace("value", "http\\S+", "")) 
    
    # get the date of the tweet when retieved
    tweet_df = tweet_df.withColumn("date", udf_current_time())

    return tweet_df