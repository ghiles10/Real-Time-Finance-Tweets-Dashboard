import tweepy
from config import core, schema
from stocks_api import ExtractStock
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
    
    def retrieve_tweets(self, count : int = 2, lang :str ='en') -> list:
        
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
        stocks_symbols = ExtractStock() 
        stocks_symbols.extract_symbols()
        
        logger.info("Stock symbols successfully extracted") 
        
        for symbol in stocks_symbols.symbols[:3]: 
            hashtag = symbol.split("USDT")[0]
    
            # Retrieve tweets
            tweets = api.search_tweets(q = hashtag, count = count, lang = lang)
            for tweet in tweets : 
                yield tweet.text 
