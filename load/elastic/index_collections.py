from load.elastic.utils import es, index_name
from load.mongo_db.utils import client


def verify_index():
    
    # Effectuez une recherche dans l'index avec la requête
    response = es.search(index=index_name, query={"match_all": {}})

    # Affichez le nombre total de documents trouvés
    assert  response['hits']['total']['value'] > 0 


def index_tweets() : 

    """ index tweets from MongoDB into Elasticsearch """
    
    # Connect to MongoDB   
    mongo_collection = client.twitter.tweet

    try :   
        es.indices.create(index=index_name)
        
    except Exception as e:  # if index already exists
        pass

    # retrieve all documents from MongoDB and index them into Elasticsearch 
    for tweet in mongo_collection.find():
        
        # suppress MongoDB's object id from the document before indexing 
        del tweet['_id'] 
        es.index(index = index_name, document = tweet  ) 

    verify_index()

if __name__ == "__main__":
    index_tweets()
        
 
