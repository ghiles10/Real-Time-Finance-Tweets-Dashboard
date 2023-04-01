import tweepy
from config import core, schema

class ExtractTweets : 
    
    CONFIG_TWEETS  = schema.TwitterConfig( **core.load_config().data["twitter_credentials"] ) 

    def __init__(self, fetch_config = CONFIG_TWEETS ) -> None:
        
        self.__ACCES_KEY = fetch_config.ACCES_KEY
        self.__ACCES_SECRET = fetch_config.ACCES_SECRET
        self.__CONSUMER_KEY = fetch_config.CONSUMER_KEY
        self.__CONSUMER_SECRET = fetch_config.CONSUMER_SECRET
        self.__auth  = None


    def authentification(self) -> None:   

        # Twitter authentication
        self.__auth = tweepy.OAuthHandler( self.__ACCES_KEY, self.__ACCES_SECRET)   
        self.__auth.set_access_token(self.__CONSUMER_KEY, self.__CONSUMER_SECRET) 
               
        return self.__auth 
    