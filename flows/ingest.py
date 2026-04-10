"""Ingest raw datasets into GCS bucket."""

import os

import requests
from dotenv import load_dotenv
from google.cloud import storage
from prefect import flow, task

load_dotenv()

GCS_BUCKET = os.environ["GCS_BUCKET"]

SOURCES = [
    (
        "http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/msd_summary_file.h5",
        "raw/msd_summary_file.h5",
    ),
    (
        "https://www.tagtraum.com/genres/msd_tagtraum_cd2c.cls.zip",
        "raw/msd_tagtraum_cd2c.cls.zip",
    ),
    (
        "http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset.db",
        "raw/mxm_dataset.db",
    ),
]


@task(log_prints=True, retries=2)
def upload_url_to_gcs(url: str, blob_name: str, bucket_name: str = GCS_BUCKET) -> str:
    """Stream a file from URL directly into GCS, never holding the full file."""
    print(f"Uploading {url} → gs://{bucket_name}/{blob_name}")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with blob.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)

    print(f"Done: gs://{bucket_name}/{blob_name}")
    return f"gs://{bucket_name}/{blob_name}"


@flow(name="ingest-raw-to-gcs", log_prints=True)
def ingest():
    """Download all source datasets and upload to GCS raw zone."""
    futures = []
    for url, blob_name in SOURCES:
        futures.append(upload_url_to_gcs.submit(url, blob_name))

    for future in futures:
        future.result()


if __name__ == "__main__":
    ingest()
