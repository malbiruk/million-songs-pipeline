"""Ingest flow: fan out 3 parallel Cloud Run Job executions (one per dataset)."""

import os

from dotenv import load_dotenv
from prefect import flow

from flows._cloud import trigger_cloud_run_job

load_dotenv()

JOB_NAME = os.environ["CLOUD_RUN_INGEST_JOB"]
DATASETS = ["tracks", "genres", "lyrics"]


@flow(name="ingest-raw-to-gcs", log_prints=True)
def ingest():
    """Trigger one Cloud Run Job execution per raw dataset, in parallel."""
    futures = [
        trigger_cloud_run_job.submit(JOB_NAME, args=["--dataset", d]) for d in DATASETS
    ]
    for f in futures:
        f.result()


if __name__ == "__main__":
    ingest()
