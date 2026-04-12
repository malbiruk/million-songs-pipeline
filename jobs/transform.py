"""Transform worker: raw GCS blob -> Parquet in GCS processed/. Runs inside a Cloud Run Job."""

import argparse
import io
import os
import sqlite3
import tempfile
import zipfile

import h5py
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

GCS_BUCKET = os.environ["GCS_BUCKET"]


def transform_genres(bucket: storage.Bucket) -> None:
    print("Downloading raw/msd_tagtraum_cd2c.cls.zip")
    raw = bucket.blob("raw/msd_tagtraum_cd2c.cls.zip").download_as_bytes()

    track_ids, genres, minority_genres = [], [], []
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


def transform_tracks(bucket: storage.Bucket) -> None:
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


def transform_lyrics(bucket: storage.Bucket) -> None:
    """Stream MusixMatch SQLite -> Parquet in bounded-memory batches."""
    schema = pa.schema(
        [
            pa.field("track_id", pa.string()),
            pa.field("word", pa.string()),
            pa.field("count", pa.int32()),
        ],
    )
    batch_size = 500_000
    total = 0

    with (
        tempfile.NamedTemporaryFile(suffix=".db") as db_tmp,
        tempfile.NamedTemporaryFile(suffix=".parquet") as pq_tmp,
    ):
        print("Downloading raw/mxm_dataset.db -> local tempfile")
        bucket.blob("raw/mxm_dataset.db").download_to_filename(db_tmp.name)

        con = sqlite3.connect(db_tmp.name)
        try:
            cursor = con.execute(
                # sql
                "SELECT track_id, word, count FROM lyrics",
            )
            with pq.ParquetWriter(pq_tmp.name, schema) as writer:
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    track_ids, words, counts = zip(*rows, strict=True)
                    batch = pa.record_batch(
                        [
                            pa.array(track_ids, type=pa.string()),
                            pa.array(words, type=pa.string()),
                            pa.array(counts, type=pa.int32()),
                        ],
                        schema=schema,
                    )
                    writer.write_batch(batch)
                    total += batch.num_rows
                    print(f"  batch written: {batch.num_rows} rows (total {total})")
        finally:
            con.close()

        print(f"Uploading processed/lyrics.parquet ({total} rows)")
        bucket.blob("processed/lyrics.parquet").upload_from_filename(pq_tmp.name)


def _decode_col(h5_dataset) -> pa.Array:
    return pa.array([v.decode("utf-8") for v in h5_dataset[:]], type=pa.string())


def _upload_parquet(bucket: storage.Bucket, table: pa.Table, blob_name: str) -> None:
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    bucket.blob(blob_name).upload_from_string(
        buf.getvalue().to_pybytes(),
        content_type="application/octet-stream",
    )


DISPATCH = {
    "genres": transform_genres,
    "tracks": transform_tracks,
    "lyrics": transform_lyrics,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, choices=sorted(DISPATCH.keys()))
    args = parser.parse_args()
    bucket = storage.Client().bucket(GCS_BUCKET)
    DISPATCH[args.dataset](bucket)


if __name__ == "__main__":
    main()
