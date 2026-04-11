"""Main pipeline: ingest → transform → load → dbt."""

from prefect import flow

from flows.ingest import ingest
from flows.load_bq import load_bq
from flows.run_dbt import run_dbt
from flows.transform import transform


@flow(name="million-songs-pipeline", log_prints=True)
def pipeline():
    """Run the full pipeline end-to-end."""
    ingest()
    transform()
    load_bq()
    run_dbt()


if __name__ == "__main__":
    pipeline()
