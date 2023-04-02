from typing import Dict, List, Optional, Sequence
from pydantic import BaseModel


class TwitterConfig(BaseModel): 
    
    """ twitter credential class """
    
    ACCES_KEY : str
    ACCES_SECRET : str
    CONSUMER_KEY : str
    CONSUMER_SECRET : str
    

class StocksConfig(BaseModel):
    
    """ stocks config class """
    
    url_symbols : str
    timeout : int
    url_data : str 