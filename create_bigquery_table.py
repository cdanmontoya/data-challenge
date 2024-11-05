import logging

import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = 'meli-challenge-440721'
TABLE_NAME = "names.names_dataset"

client = bigquery.Client(project=PROJECT_ID)

schema = [
    bigquery.SchemaField("id_number", "INTEGER"),
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("birth_year", "INTEGER"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("ranking", "STRING"),
    bigquery.SchemaField("prefix", "STRING"),
    bigquery.SchemaField("full_name", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("first_last_name", "STRING"),
    bigquery.SchemaField("second_last_name", "STRING"),
]


def create_clustered_table() -> None:
    try:
        table = bigquery.Table(f"{PROJECT_ID}.{TABLE_NAME}", schema=schema)
        table.clustering_fields = ["country", ]
        table = client.create_table(table)
        logger.info(f"Created clustered table {table.project}.{table.dataset_id}.{table.table_id}")

    except Exception as e:
        logger.error(f"Error while creating clustered table: {str(e)}")


def save_dataframe(df: pd.DataFrame):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, f"{PROJECT_ID}.{TABLE_NAME}", job_config=job_config)
    job.result()
    table = client.get_table(TABLE_NAME)
    logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {TABLE_NAME}")
