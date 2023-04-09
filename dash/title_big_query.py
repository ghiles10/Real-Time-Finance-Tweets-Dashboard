from google.cloud import bigquery
from config import schema, core


# load configuration 
APP_CONFIG = schema.BigQuery(**core.load_config().data["big_query"])


def symbols_list() : 
    
    """ retrieve the list of symbols from the big query table"""
    
    symbols = []
    project_id = APP_CONFIG.project_id 
    table_id = APP_CONFIG.dataset_id + "." + "fact"
    query = f"SELECT DISTINCT SYMBOL FROM {table_id}"

    #BigQuery client 
    client = bigquery.Client(project=project_id)

    # run query 
    query_job = client.query(query)

    # fetch
    results = query_job.result()
    
    for row in results:
        symbols.append(row.SYMBOL)

    return symbols


