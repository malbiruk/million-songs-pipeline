"""Load Parquet files from GCS processed/ into BigQuery."""

import os

from dotenv import load_dotenv
from google.cloud import bigquery
from prefect import flow, task

load_dotenv()

GCS_BUCKET = os.environ["GCS_BUCKET"]
BQ_DATASET = os.environ["BQ_DATASET"]

TABLES = [
    ("processed/tracks.parquet", "tracks"),
    ("processed/genres.parquet", "genres"),
    ("processed/lyrics.parquet", "lyrics"),
]


@task(log_prints=True)
def load_parquet_to_bq(blob_name: str, table_name: str) -> None:
    """Load a single Parquet file from GCS into a BigQuery table."""
    client = bigquery.Client()
    table_id = f"{client.project}.{BQ_DATASET}.{table_name}"
    uri = f"gs://{GCS_BUCKET}/{blob_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Loading {uri} → {table_id}")
    job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_id}")


@flow(name="load-parquet-to-bq", log_prints=True)
def load_bq():
    """Load all processed Parquet files into BigQuery."""
    for blob_name, table_name in TABLES:
        load_parquet_to_bq.submit(blob_name, table_name)


if __name__ == "__main__":
    load_bq()
