"""Spark job: parse raw datasets from GCS and write Parquet to processed/ zone.

Submitted to Dataproc. Reads from gs://<bucket>/raw/, writes to gs://<bucket>/processed/.

Usage:
    gcloud dataproc jobs submit pyspark spark/raw_to_parquet.py \
        --cluster=million-songs-spark \
        --region=europe-central2 \
        -- --bucket million-songs-pipeline-data
"""

import argparse
import io
import sqlite3
import tarfile
import tempfile
import zipfile

import h5py
from pyspark.sql import Row, SparkSession


def parse_h5(h5_bytes: bytes) -> dict:
    """Extract relevant fields from one HDF5 file."""
    with h5py.File(io.BytesIO(h5_bytes), "r") as h5:
        analysis = h5["analysis/songs"][0]
        metadata = h5["metadata/songs"][0]
        musicbrainz = h5["musicbrainz/songs"][0]

        return {
            "track_id": analysis["track_id"].decode("utf-8"),
            "title": metadata["title"].decode("utf-8"),
            "artist_name": metadata["artist_name"].decode("utf-8"),
            "release": metadata["release"].decode("utf-8"),
            "year": int(musicbrainz["year"]),
            "duration": float(analysis["duration"]),
            "tempo": float(analysis["tempo"]),
            "loudness": float(analysis["loudness"]),
            "key": int(analysis["key"]),
            "mode": int(analysis["mode"]),
            "time_signature": int(analysis["time_signature"]),
            "danceability": float(analysis["danceability"]),
            "energy": float(analysis["energy"]),
            "song_hotttnesss": float(metadata["song_hotttnesss"]),
            "artist_hotttnesss": float(metadata["artist_hotttnesss"]),
            "artist_latitude": float(metadata["artist_latitude"]),
            "artist_longitude": float(metadata["artist_longitude"]),
            "artist_location": metadata["artist_location"].decode("utf-8"),
        }


def process_tracks(spark: SparkSession, bucket: str) -> None:
    """Parse HDF5 files from tar.gz archive using distributed processing."""
    sc = spark.sparkContext

    # Read tar.gz as binary — this runs on the driver
    gcs_path = f"gs://{bucket}/raw/millionsongsubset.tar.gz"
    blob_rdd = sc.binaryFiles(gcs_path)
    tar_bytes = blob_rdd.first()[1]

    # Extract individual HDF5 files from the tar
    h5_files = []
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.name.endswith(".h5"):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            h5_files.append(f.read())

    # Distribute HDF5 parsing across workers
    h5_rdd = sc.parallelize(h5_files, numSlices=min(len(h5_files), 100))
    rows_rdd = h5_rdd.map(parse_h5).map(lambda d: Row(**d))

    df = spark.createDataFrame(rows_rdd)
    df.write.parquet(f"gs://{bucket}/processed/tracks", mode="overwrite")
    print(f"Wrote {df.count()} tracks to processed/tracks")


def process_genres(spark: SparkSession, bucket: str) -> None:
    """Parse genre annotations from zip file."""
    sc = spark.sparkContext

    gcs_path = f"gs://{bucket}/raw/msd_tagtraum_cd2c.cls.zip"
    blob_rdd = sc.binaryFiles(gcs_path)
    zip_bytes = blob_rdd.first()[1]

    rows = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z, z.open(z.namelist()[0]) as f:
        for raw_line in f:
            line = raw_line.decode("utf-8").strip()
            if line.startswith("#"):
                continue
            parts = line.split("\t")
            rows.append(
                Row(
                    track_id=parts[0],
                    genre=parts[1],
                    minority_genre=parts[2] if len(parts) > 2 else None,
                ),
            )

    df = spark.createDataFrame(rows)
    df.write.parquet(f"gs://{bucket}/processed/genres", mode="overwrite")
    print(f"Wrote {df.count()} genres to processed/genres")


def process_lyrics(spark: SparkSession, bucket: str) -> None:
    """Parse MusixMatch SQLite database."""
    sc = spark.sparkContext

    gcs_path = f"gs://{bucket}/raw/mxm_dataset.db"
    blob_rdd = sc.binaryFiles(gcs_path)
    db_bytes = blob_rdd.first()[1]

    # SQLite requires a file on disk
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        tmp.write(db_bytes)
        tmp.flush()
        con = sqlite3.connect(tmp.name)
        db_rows = con.execute("SELECT track_id, word, count FROM lyrics").fetchall()
        con.close()

    rows = [Row(track_id=r[0], word=r[1], count=r[2]) for r in db_rows]
    df = spark.createDataFrame(rows)
    df.write.parquet(f"gs://{bucket}/processed/lyrics", mode="overwrite")
    print(f"Wrote {df.count()} lyrics rows to processed/lyrics")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("msd-raw-to-parquet").getOrCreate()

    process_tracks(spark, args.bucket)
    process_genres(spark, args.bucket)
    process_lyrics(spark, args.bucket)

    spark.stop()
