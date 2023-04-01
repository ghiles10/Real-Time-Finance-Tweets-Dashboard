import tweepy
from config import core, schema

class ExtractTweets : 
    
    CONFIG_TWEETS  = schema.TwitterConfig( **core.load_config().data["twitter_credentials"] ) 

    def __init__(self, fetch_config = CONFIG_TWEETS ) -> None:
        
        self.__ACCES_KEY = fetch_config.ACCES_KEY
        self.__ACCES_SECRET = fetch_config.ACCES_SECRET
        self.__CONSUMER_KEY = fetch_config.CONSUMER_KEY
        self.__CONSUMER_SECRET = fetch_config.CONSUMER_SECRET
        
        print('*' *50) 
        print(type(self.__ACCES_KEY))
        print('*' *50) 


    def authentification(self) -> None:   

        # Twitter authentication
        auth = tweepy.OAuthHandler( self.__ACCES_KEY, self.__ACCES_SECRET)   
        auth.set_access_token(self.__CONSUMER_KEY, self.__CONSUMER_SECRET) 
               
        # Creating an API object 
        api = tweepy.API(auth)
        
        # get user tweets 
        tweets = api.user_timeline(screen_name='@'+str('elonmusk'), 
                                count=50,
                                include_rts = False,
                                tweet_mode = 'extended') 
        print("Authentification successfull")
        

ExtractTweets().authentification()