import sys 
import pymongo
from elasticsearch import Elasticsearch
from pathlib import Path


# elastic connection
es = Elasticsearch(
    cloud_id="4b14cabeeeff4e59a05853191d8aa21b:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDM4Y2MxMWU1NTIzZjQ4YzE4MTY0NWVjMGQzYTU3NzZhJDk4NzM3MTkyZDJmMDQ4MjY4ZDNlMGJkZTY1NmY2NDNk",
    basic_auth=("ghiles", "ghiles10")
)


# Connect to MongoDB
client = pymongo.MongoClient("mongodb+srv://ghiles:ghiles10@cluster0.ru8hmhi.mongodb.net/?retryWrites=true&w=majority")   
mongo_collection = client.twitter.tweet

# Créez un index dans Elasticsearch (si nécessaire)
index_name = "tweets"

try :   
    es.indices.create(index=index_name)
except Exception as e:  # if index already exists
    pass 

# retrieve all documents from MongoDB and index them into Elasticsearch 
for tweet in mongo_collection.find():
    
    # suppress MongoDB's object id from the document before indexing 
    del tweet['_id'] 
    
    es.index(index = index_name, document = tweet  ) 
    print('tweet indexed') 

def verify_index():
    # Effectuez une recherche dans l'index avec la requête
    response = es.search(index=index_name, query={"match_all": {}})

    # Affichez le nombre total de documents trouvés
    assert  response['hits']['total']['value'] > 0 
        
 
