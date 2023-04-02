import sys
import time
import threading
import json

# Append the path
sys.path.append('/workspaces/Finance-dashbord')

# Import required libraries
from kafka import KafkaProducer
from extract import stocks_api
from extract.tweets_api import ExtractTweets
from config import schema, core
import logging_config

# Initialize logging configuration
logger = logging_config.logger

# Load Kafka configuration from the config file
API_CONFIG = schema.kafkaConfig(**core.load_config().data["kafka_config"])
logger.info("Kafka config loaded from config file")


class SendToKafka:
    """Send data to Kafka for further processing for each topic."""

    def __init__(self, producer: KafkaProducer) -> None:
        self.producer = producer
        self.finance_extractor = stocks_api.ExtractStock()
        self.finance_extractor.extract_symbols()

        logger.info("Symbols extracted from API finance")

    def send_tweets(self, topic: str = None) -> None:
        tweet_extractor = ExtractTweets()
        logger.info("Class ExtractTweets initialized")

        while True:
            time.sleep(3)
            for tweet in tweet_extractor.retrieve_tweets(stocks_symbols=self.finance_extractor):
                self.producer.send(topic, tweet)

    def send_finance_data(self, topic: str = None) -> None:
        while True:
            time.sleep(3)
            for data in self.finance_extractor.extract_data():
                self.producer.send(topic, data)


# Main block
if __name__ == "__main__":
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=API_CONFIG.bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

    # Create SendKafka object
    send_kafka = SendToKafka(producer)

    # Create threads to send extracted data to Kafka producer
    finance_data_thread = threading.Thread(target=send_kafka.send_finance_data, args=(API_CONFIG.topic_data,))
    tweets_thread = threading.Thread(target=send_kafka.send_tweets, args=(API_CONFIG.topic_tweets,))

    # Start threads
    finance_data_thread.start()
    tweets_thread.start()

    # Wait for threads to finish
    finance_data_thread.join()
    tweets_thread.join()

    producer.flush()
