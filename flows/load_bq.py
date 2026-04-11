"""Load Parquet files from GCS processed/ into BigQuery."""

import os

from dotenv import load_dotenv
from google.cloud import bigquery
from prefect import flow, task

load_dotenv()

GCS_BUCKET = os.environ["GCS_BUCKET"]
BQ_DATASET = os.environ["BQ_DATASET"]

TABLES: list[dict] = [
    {
        "blob": "processed/tracks.parquet",
        "table": "tracks",
        "partition": bigquery.RangePartitioning(
            field="year",
            range_=bigquery.PartitionRange(start=1920, end=2030, interval=10),
        ),
        "clustering": ["artist_name"],
    },
    {
        "blob": "processed/genres.parquet",
        "table": "genres",
        "clustering": ["genre"],
    },
    {
        "blob": "processed/lyrics.parquet",
        "table": "lyrics",
        "clustering": ["track_id", "word"],
    },
]


@task(log_prints=True)
def load_parquet_to_bq(table_spec: dict) -> None:
    """Load a single Parquet file from GCS into a BigQuery table."""
    client = bigquery.Client()
    table_id = f"{client.project}.{BQ_DATASET}.{table_spec['table']}"
    uri = f"gs://{GCS_BUCKET}/{table_spec['blob']}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        clustering_fields=table_spec.get("clustering"),
        range_partitioning=table_spec.get("partition"),
    )

    print(f"Loading {uri} → {table_id}")
    job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_id}")


@flow(name="load-parquet-to-bq", log_prints=True)
def load_bq():
    """Load all processed Parquet files into BigQuery."""
    for table_spec in TABLES:
        load_parquet_to_bq.submit(table_spec)


if __name__ == "__main__":
    load_bq()
