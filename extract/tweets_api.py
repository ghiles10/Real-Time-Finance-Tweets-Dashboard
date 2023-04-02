import tweepy
from config import core, schema
from stocks_api import StocksApi

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
    
    def retrieve_tweets(self, hashtag : str, count : int = 100) -> list:
        
        # Twitter authentication
        if not self.__auth : 
            self.authentification()
        
        # Twitter API
        try:
            api = tweepy.API(self.__auth)

        except tweepy.TweepError as e:
            print(f"An error occurred: {e.response.status_code}")
            
        # Retrieve tweets
        tweets = api.search_tweets(q = hashtag, count = count)
    
            