"""Transform flow: fan out 3 parallel Cloud Run Job executions (one per dataset)."""

import os

from dotenv import load_dotenv
from prefect import flow

from flows._cloud import trigger_cloud_run_job

load_dotenv()

JOB_NAME = os.environ["CLOUD_RUN_TRANSFORM_JOB"]
DATASETS = ["genres", "tracks", "lyrics"]


@flow(name="transform-raw-to-parquet", log_prints=True)
def transform():
    """Trigger one Cloud Run Job execution per dataset, in parallel."""
    futures = [
        trigger_cloud_run_job.submit(JOB_NAME, args=["--dataset", d]) for d in DATASETS
    ]
    for f in futures:
        f.result()


if __name__ == "__main__":
    transform()
