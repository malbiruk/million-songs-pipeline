"""Transform raw datasets from GCS into Parquet and upload to processed/."""

import io
import os
import sqlite3
import tempfile
import zipfile

import h5py
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import storage
from prefect import flow, task

load_dotenv()

GCS_BUCKET = os.environ["GCS_BUCKET"]


@task(log_prints=True)
def transform_genres(bucket_name: str) -> None:
    """Parse tagtraum genre annotations (zip/text) → Parquet."""
    bucket = storage.Client().bucket(bucket_name)
    print("Downloading raw/msd_tagtraum_cd2c.cls.zip")
    raw = bucket.blob("raw/msd_tagtraum_cd2c.cls.zip").download_as_bytes()

    track_ids = []
    genres = []
    minority_genres = []

    with zipfile.ZipFile(io.BytesIO(raw)) as z, z.open(z.namelist()[0]) as f:
        for line_ in f:
            line = line_.decode("utf-8").strip()
            if line.startswith("#"):
                continue
            parts = line.split("\t")
            track_ids.append(parts[0])
            genres.append(parts[1])
            minority_genres.append(parts[2] if len(parts) > 2 else None)

    table = pa.table(
        {
            "track_id": pa.array(track_ids, type=pa.string()),
            "genre": pa.array(genres, type=pa.string()),
            "minority_genre": pa.array(minority_genres, type=pa.string()),
        },
    )

    _upload_parquet(bucket, table, "processed/genres.parquet")
    print(f"Wrote {len(table)} rows to processed/genres.parquet")


@task(log_prints=True)
def transform_tracks(bucket_name: str) -> None:
    """Parse MSD summary HDF5 → Parquet (columnar read, no row-by-row loop)."""
    bucket = storage.Client().bucket(bucket_name)
    print("Downloading raw/msd_summary_file.h5")
    raw = bucket.blob("raw/msd_summary_file.h5").download_as_bytes()

    with h5py.File(io.BytesIO(raw), "r") as h5:
        analysis = h5["analysis/songs"]
        metadata = h5["metadata/songs"]
        musicbrainz = h5["musicbrainz/songs"]

        table = pa.table(
            {
                "track_id": _decode_col(analysis["track_id"]),
                "title": _decode_col(metadata["title"]),
                "artist_name": _decode_col(metadata["artist_name"]),
                "release": _decode_col(metadata["release"]),
                "artist_location": _decode_col(metadata["artist_location"]),
                "year": pa.array(musicbrainz["year"][:], type=pa.int16()),
                "duration": pa.array(analysis["duration"][:], type=pa.float32()),
                "tempo": pa.array(analysis["tempo"][:], type=pa.float32()),
                "loudness": pa.array(analysis["loudness"][:], type=pa.float32()),
                "key": pa.array(analysis["key"][:], type=pa.int8()),
                "mode": pa.array(analysis["mode"][:], type=pa.int8()),
                "time_signature": pa.array(
                    analysis["time_signature"][:],
                    type=pa.int8(),
                ),
                "danceability": pa.array(
                    analysis["danceability"][:],
                    type=pa.float32(),
                ),
                "energy": pa.array(analysis["energy"][:], type=pa.float32()),
                "song_hotttnesss": pa.array(
                    metadata["song_hotttnesss"][:],
                    type=pa.float32(),
                ),
                "artist_hotttnesss": pa.array(
                    metadata["artist_hotttnesss"][:],
                    type=pa.float32(),
                ),
                "artist_latitude": pa.array(
                    metadata["artist_latitude"][:],
                    type=pa.float32(),
                ),
                "artist_longitude": pa.array(
                    metadata["artist_longitude"][:],
                    type=pa.float32(),
                ),
            },
        )

    _upload_parquet(bucket, table, "processed/tracks.parquet")
    print(f"Wrote {len(table)} rows to processed/tracks.parquet")


@task(log_prints=True)
def transform_lyrics(bucket_name: str) -> None:
    """Parse MusixMatch SQLite → Parquet."""
    bucket = storage.Client().bucket(bucket_name)
    print("Downloading raw/mxm_dataset.db")
    raw = bucket.blob("raw/mxm_dataset.db").download_as_bytes()

    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        tmp.write(raw)
        tmp.flush()
        con = sqlite3.connect(tmp.name)
        rows = con.execute(
            # sql
            "SELECT track_id, word, count FROM lyrics",
        ).fetchall()
        con.close()

    table = pa.table(
        {
            "track_id": pa.array([r[0] for r in rows], type=pa.string()),
            "word": pa.array([r[1] for r in rows], type=pa.string()),
            "count": pa.array([r[2] for r in rows], type=pa.int32()),
        },
    )

    _upload_parquet(bucket, table, "processed/lyrics.parquet")
    print(f"Wrote {len(table)} rows to processed/lyrics.parquet")


def _decode_col(h5_dataset) -> pa.Array:
    """Decode a bytes column from HDF5 into a pyarrow string array."""
    return pa.array([v.decode("utf-8") for v in h5_dataset[:]], type=pa.string())


def _upload_parquet(bucket: storage.Bucket, table: pa.Table, blob_name: str) -> None:
    """Write a pyarrow Table as Parquet directly to GCS."""
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    bucket.blob(blob_name).upload_from_string(
        buf.getvalue().to_pybytes(),
        content_type="application/octet-stream",
    )


@flow(name="transform-raw-to-parquet", log_prints=True)
def transform():
    """Transform all raw datasets into Parquet in GCS processed/."""
    transform_genres.submit(GCS_BUCKET)
    transform_tracks.submit(GCS_BUCKET)
    transform_lyrics.submit(GCS_BUCKET)


if __name__ == "__main__":
    transform()
