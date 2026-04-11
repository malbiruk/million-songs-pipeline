# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: title,-all
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% Imports
import io
import sqlite3
import tempfile
import zipfile

import h5py
from google.cloud import storage

# %% Connect to GCS
client = storage.Client()
bucket = client.bucket("million-songs-pipeline-data")

tagtraum = bucket.blob("raw/msd_tagtraum_cd2c.cls.zip")
msd = bucket.blob("raw/msd_summary_file.h5")
mxm = bucket.blob("raw/mxm_dataset.db")

# %% Explore genres (1.6MB zip)
genre_bytes = tagtraum.download_as_bytes()
with zipfile.ZipFile(io.BytesIO(genre_bytes)) as z:
    print(z.namelist())  # see what's inside
    # read the first file
    with z.open(z.namelist()[0]) as f:
        for i, line in enumerate(f):
            print(line.decode("utf-8").strip())
            if i > 20:
                break

# %% Explore MSD summary file (300MB, all 1M tracks in one HDF5)
msd_bytes = msd.download_as_bytes()

with h5py.File(io.BytesIO(msd_bytes), "r") as h5:
    # Show top-level structure
    def print_structure(name, _obj):
        print(name)

    h5.visititems(print_structure)

# %% Inspect columns and sample rows
with h5py.File(io.BytesIO(msd_bytes), "r") as h5:
    print("=== analysis/songs columns ===")
    print(h5["analysis/songs"].dtype.names)
    print(f"Rows: {len(h5['analysis/songs'])}")
    print()
    print("=== metadata/songs columns ===")
    print(h5["metadata/songs"].dtype.names)
    print(f"Rows: {len(h5['metadata/songs'])}")
    print()
    print("=== musicbrainz/songs columns ===")
    print(h5["musicbrainz/songs"].dtype.names)
    print(f"Rows: {len(h5['musicbrainz/songs'])}")
    print()
    print("=== sample row ===")
    print("analysis:", h5["analysis/songs"][0])
    print("metadata:", h5["metadata/songs"][0])
    print("musicbrainz:", h5["musicbrainz/songs"][0])

# %% Explore lyrics (2.4GB SQLite)
mxm_bytes = mxm.download_as_bytes()
with tempfile.NamedTemporaryFile(suffix=".db", dir="/home/klim") as tmp:
    tmp.write(mxm_bytes)
    tmp.flush()
    con = sqlite3.connect(tmp.name)
    tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print("Tables:", tables)
    for table in tables:
        print(f"\n--- {table[0]} ---")
        print(con.execute(f"SELECT * FROM {table[0]} LIMIT 5").fetchall())
    print("\nRow count:", con.execute("SELECT COUNT(*) FROM lyrics").fetchone())
    print(
        "Distinct tracks:",
        con.execute("SELECT COUNT(DISTINCT track_id) FROM lyrics").fetchone(),
    )
    con.close()


# %% Parse functions
def parse_genres(zip_bytes: bytes) -> list[dict]:
    """Parse genre file. Returns list of {track_id, genre, minority_genre}."""
    output = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z, z.open(z.namelist()[0]) as f:
        for line_ in f:
            line = line_.decode("utf-8").strip()
            if line.startswith("#"):
                continue
            parts = line.split("\t")
            output.append(
                {
                    "track_id": parts[0],
                    "genre": parts[1],
                    "minority_genre": parts[2] if len(parts) > 2 else None,
                },
            )

    return output


def parse_msd_summary(h5_bytes: bytes) -> list[dict]:
    """Extract relevant fields from MSD summary HDF5. Returns all tracks."""
    with h5py.File(io.BytesIO(h5_bytes), "r") as h5:
        analysis = h5["analysis/songs"]
        metadata = h5["metadata/songs"]
        musicbrainz = h5["musicbrainz/songs"]
        n = len(analysis)

        return [
            {
                "track_id": analysis[i]["track_id"].decode("utf-8"),
                "title": metadata[i]["title"].decode("utf-8"),
                "artist_name": metadata[i]["artist_name"].decode("utf-8"),
                "release": metadata[i]["release"].decode("utf-8"),
                "year": int(musicbrainz[i]["year"]),
                "duration": float(analysis[i]["duration"]),
                "tempo": float(analysis[i]["tempo"]),
                "loudness": float(analysis[i]["loudness"]),
                "key": int(analysis[i]["key"]),
                "mode": int(analysis[i]["mode"]),
                "time_signature": int(analysis[i]["time_signature"]),
                "danceability": float(analysis[i]["danceability"]),
                "energy": float(analysis[i]["energy"]),
                "song_hotttnesss": float(metadata[i]["song_hotttnesss"]),
                "artist_hotttnesss": float(metadata[i]["artist_hotttnesss"]),
                "artist_latitude": float(metadata[i]["artist_latitude"]),
                "artist_longitude": float(metadata[i]["artist_longitude"]),
                "artist_location": metadata[i]["artist_location"].decode("utf-8"),
            }
            for i in range(n)
        ]


def parse_lyrics(db_bytes: bytes) -> list[dict]:
    """Parse SQLite lyrics. Returns list of {track_id, word, count}."""
    with tempfile.NamedTemporaryFile(suffix=".db", dir="/home/klim") as tmp:
        tmp.write(db_bytes)
        tmp.flush()
        con = sqlite3.connect(tmp.name)
        rows = con.execute(
            "SELECT track_id, word, count FROM lyrics WHERE is_test = 0",
        ).fetchall()
        con.close()
    return [{"track_id": r[0], "word": r[1], "count": r[2]} for r in rows]


# %% Test parse functions
genres = parse_genres(genre_bytes)
print(f"Genres: {len(genres)} rows")
print(genres[:3])

# %%
tracks = parse_msd_summary(msd_bytes)
print(f"Tracks: {len(tracks)} rows")
print(tracks[:2])

# %%
lyrics = parse_lyrics(mxm_bytes)
print(f"Lyrics: {len(lyrics)} rows")
print(lyrics[:3])

# %%
