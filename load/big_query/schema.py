from google.cloud import bigquery

SCHEMA_FACT = [
    bigquery.SchemaField("hour", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),  
    bigquery.SchemaField("high", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("low", "FLOAT64", mode="NULLABLE"),
]

SCHEMA_DIM_TIME = [
    bigquery.SchemaField("time", "RECORD", mode="REQUIRED", fields=[
        bigquery.SchemaField("hour", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("month", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("day", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("year", "INT64", mode="REQUIRED"),
    ]),
]

SCHEMA_DIM_STOCK = [
    bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("buy", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("sell", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("vol", "FLOAT64", mode="NULLABLE"),
]