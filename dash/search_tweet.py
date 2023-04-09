import sys
sys.path.append(r'/workspaces/Finance-dashbord')
from load.elastic.utils import es, index_name


def search_tweet(crypto : str, date: str = "2023-04-09"): 
    
    """ search tweet by crypto and date in elastic search indices"""
        
        
    query = {
        "bool": {
            "must": {
                "match": {
                    "value": crypto
                }
            },
            "filter": {
                "range": {
                    "date": {
                        "gte": date
                    }
                }
            }
        }
    }


    # cherche dans l'index avec la requête
    response = es.search(index=index_name, query=query) 

    return response['hits']['hits'][0]['_source']['value']


if __name__ == '__main__':
    print(search_tweet('bitcoin', '2023-04-09'))
   
