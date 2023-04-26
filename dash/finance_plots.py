import sys
sys.path.append(r'/workspaces/Finance-dashbord')

from google.cloud import bigquery
from config import schema, core


# load configuration 
APP_CONFIG = schema.BigQuery(**core.load_config().data["big_query"])


def plot_histogram_cryptos() : 
    
    """ this function return stats about cryptos """ 
    
    project_id = APP_CONFIG.project_id 
    table_id = APP_CONFIG.dataset_id + "." + "fact"
    
    query = f"""SELECT
        symbol,
        AVG(high - low) AS avg_difference
        FROM
        {table_id}
        WHERE
        EXTRACT(DATE FROM TIMESTAMP(hour)) = "1970-01-01"
        AND EXTRACT(HOUR FROM TIMESTAMP(hour)) >= 0
        AND EXTRACT(HOUR FROM TIMESTAMP(hour)) < 10
        GROUP BY symbol;
        """

    #BigQuery client 
    client = bigquery.Client(project=project_id)

    # run query and convert to pandas
    query_job = client.query(query).to_dataframe()

    return query_job

if __name__ == "__main__" : 
    
    # for row in plot_histogram_cryptos():
    #     print( row.symbol,row.avg_difference)
    print(plot_histogram_cryptos())
 



    
    