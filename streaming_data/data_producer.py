import sys
sys.path.append('/workspaces/Finance-dashbord')
from extract import stocks_api
from extract.tweets_api import ExtractTweets
from config import schema, core

import threading
import json
from kafka import KafkaProducer
import time
import logging_config

# Initialisation de la configuration de logging
logger = logging_config.logger

# Init of the config file 
API_CONFIG  = schema.kafkaConfig( **core.load_config().data["kafka_config"] )
logger.info("kafka config loaded from config file")

class SendToKafka:
    """Send data to Kafka for further processing for each topic"""
    

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


# Bloc principal
if __name__ == "__main__":
    
    # Initialisation du producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers=API_CONFIG.bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
    
    # Création de l'objet SendKafka
    send_kafka = SendToKafka(producer)
    
    # Création des threads pour envoyer les données extraites au producteur Kafka
    finance_data_thread = threading.Thread(target=send_kafka.send_finance_data, args=(API_CONFIG.topic_data,))
    tweets_thread = threading.Thread(target=send_kafka.send_tweets, args=(API_CONFIG.topic_tweets,))

    # Démarrage des threads
    finance_data_thread.start()
    tweets_thread.start()

    # Attente de la fin des threads
    finance_data_thread.join()
    tweets_thread.join()

    producer.flush()