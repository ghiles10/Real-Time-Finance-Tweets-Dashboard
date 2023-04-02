import sys
sys.path.append('/workspaces/Finance-dashbord') 

from extract import stocks_api 
from extract.tweets_api import ExtractTweets  

import json
from kafka import KafkaProducer
import time
import logging_config

# Initialisation de la configuration de logging
logger = logging_config.logger


class SendToKafka():

    """ Send data to Kafka for further processing for each topic"""

    def __init__(self) -> None:
        
        # extract symbols
        self.finance_extractor = stocks_api.ExtractStock()
        self.finance_extractor.extract_symbols()
        
        logger.info("Symbols extracted from API finance") 
        
        
    def send_tweets(self, topic :str = None , producer : KafkaProducer = None) : 
        
        """ this method is used to send tweets to kafka broker"""
               
        tweet_extractor = ExtractTweets()
        logger.info(" class ExtractTweets initialized") 
        
        while True : 
            time.sleep(3)
            for tweet in tweet_extractor.retrieve_tweets(stocks_symbols =self.finance_extractor ) : 
                producer.send(topic, tweet)
            
            
    def send_finance_data(self, topic :str = None , producer : KafkaProducer = None) : 
        
        """ this method is used to send finance data to kafka broker """
        while True : 
            time.sleep(3)
            for data in self.finance_extractor.extract_data() : 
                producer.send(topic, data)
            
            
        
            
    
# Bloc principal
if __name__ == "__main__":
    
    # Création de l'objet SendKafka
    send_kafka = SendToKafka()
    
    # Initialisation du producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
    
    # Envoi des données extraites au producteur Kafka
    send_kafka.send_tweets(topic ='tweet' ,producer = producer)
    send_kafka.send_finance_data(topic ='finance' ,producer = producer)
    producer.flush()