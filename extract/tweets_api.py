import tweepy
import sys
sys.path.append("/workspaces/Finance-dashbord")
from extract import stocks_api
from config import core, schema
import logging_config


# init logger 
logger = logging_config.logger

class ExtractTweets : 
    
    """ this class is used to extract tweets matching with hashtag from twitter API """

    CONFIG_TWEETS  = schema.TwitterConfig( **core.load_config().data["twitter_credentials"] ) 
    
    logger.info("Twitter credentials loaded from config file") 

    def __init__(self, fetch_config = CONFIG_TWEETS ) -> None:
        
        self.__ACCES_KEY = fetch_config.ACCES_KEY
        self.__ACCES_SECRET = fetch_config.ACCES_SECRET
        self.__CONSUMER_KEY = fetch_config.CONSUMER_KEY
        self.__CONSUMER_SECRET = fetch_config.CONSUMER_SECRET
        self.__auth  = None


    def authentification(self) -> None:   
        
        """ this method is used to authenticate to twitter API """

        # Twitter authentication
        self.__auth = tweepy.OAuthHandler( self.__ACCES_KEY, self.__ACCES_SECRET)   
        self.__auth.set_access_token(self.__CONSUMER_KEY, self.__CONSUMER_SECRET)
         
        logger.info("Twitter authentication successful")
               
        return self.__auth 
    
    
    def retrieve_tweets(self, count : int = 3, lang :str ='en',stocks_symbols :stocks_api.ExtractStock  = None ) -> list:
        
        """ this method is used to retrieve tweets  """
        
        # Twitter authentication
        if not self.__auth : 
            self.authentification()
        
        # Twitter API
        try:
            api = tweepy.API(self.__auth)
            
            logger.info("Twitter API successfully initialized")

        except tweepy.TweepError as e:
            print(f"An error occurred: {e.response.status_code}")
            
        # process symbols 
        # stocks_symbols = stocks_api.ExtractStock() 
        # stocks_symbols.extract_symbols()
                
        for symbol in stocks_symbols.symbols: 
            if  symbol == 'BTCUSDT'  or symbol == 'ETHUSDT' or symbol == "DOGEUSDT" :   
                hashtag = symbol.split("USDT")[0]
        
                # Retrieve tweets
                tweets = api.search_tweets(q = hashtag, count = count, lang = lang)
                
                for tweet in tweets : 
                    yield tweet.text 

if __name__ == "__main__" : 
    test = ExtractTweets() 
    for tweet in test.retrieve_tweets() : 
        print(tweet)