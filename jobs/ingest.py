"""Ingest worker: stream a source URL into GCS. Runs inside a Cloud Run Job."""

import argparse
import os

import requests
from google.cloud import storage

GCS_BUCKET = os.environ["GCS_BUCKET"]

SOURCES = {
    "tracks": (
        "http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/msd_summary_file.h5",
        "raw/msd_summary_file.h5",
    ),
    "genres": (
        "https://www.tagtraum.com/genres/msd_tagtraum_cd2c.cls.zip",
        "raw/msd_tagtraum_cd2c.cls.zip",
    ),
    "lyrics": (
        "http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset.db",
        "raw/mxm_dataset.db",
    ),
}


def stream_url_to_gcs(url: str, blob_name: str, bucket_name: str) -> None:
    print(f"Uploading {url} -> gs://{bucket_name}/{blob_name}")
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with blob.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
    print(f"Done: gs://{bucket_name}/{blob_name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, choices=sorted(SOURCES.keys()))
    args = parser.parse_args()
    url, blob = SOURCES[args.dataset]
    stream_url_to_gcs(url, blob, GCS_BUCKET)


if __name__ == "__main__":
    main()
