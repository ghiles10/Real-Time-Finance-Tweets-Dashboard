from google.cloud import bigquery


class BigQueryService :

    def __init__(self, project_id :str  ) :
        
        self.project_id = project_id


    def symbols_list(self, table_id : str ) : 
        
        """ retrieve the list of symbols from the big query table"""

        symbols = [] 
        query = f"SELECT DISTINCT symbol FROM {table_id}"

        #BigQuery client 
        client = bigquery.Client(project=self.project_id)

        # run query 
        query_job = client.query(query)

        # fetch
        results = query_job.result()
        
        for row in results:
            symbols.append(row.symbol)

        return symbols
    
    def avg_hour_cryptos(self, table_id:str) : 
    
        """ this function return stats about cryptos """ 
        
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
        client = bigquery.Client(project=self.project_id)

        # run query and convert to pandas
        query_job = client.query(query).to_dataframe()

        return query_job


if __name__ == "__main__" : 
    
    import sys
    sys.path.append(r'/workspaces/Finance-dashbord')
    
    from config import schema, core


    # load configuration 
    APP_CONFIG = schema.BigQuery(**core.load_config().data["big_query"])
    project_id = APP_CONFIG.project_id 
    table_id  = APP_CONFIG.dataset_id + "." + "fact"
    
    bq = BigQueryService(project_id) 
    print(bq.plot_histogram_cryptos(table_id))