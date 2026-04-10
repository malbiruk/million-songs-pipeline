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

# %%
import io
import sqlite3
import tarfile
import tempfile
import zipfile

import h5py
from google.cloud import storage

# %%
client = storage.Client()
bucket = client.bucket("million-songs-pipeline-data")

tagtraum = bucket.blob("raw/msd_tagtraum_cd2c.cls.zip")
msd = bucket.blob("raw/millionsongsubset.tar.gz")
mxm = bucket.blob("raw/mxm_dataset.db")

# %%
genre_bytes = tagtraum.download_as_bytes()
with zipfile.ZipFile(io.BytesIO(genre_bytes)) as z:
    print(z.namelist())  # see what's inside
    # read the first file
    with z.open(z.namelist()[0]) as f:
        for i, line in enumerate(f):
            print(line.decode("utf-8").strip())
            if i > 20:
                break

# %%
msd_bytes = msd.download_as_bytes()  # ~2GB, will take a moment
with tarfile.open(fileobj=io.BytesIO(msd_bytes), mode="r:gz") as tar:
    # list first few files
    members = tar.getmembers()[:20]
    for m in members:
        print(m.name, m.size)

    # find first .h5 file and peek inside
    h5_member = next(m for m in tar.getmembers() if m.name.endswith(".h5"))
    f = tar.extractfile(h5_member)
    with h5py.File(io.BytesIO(f.read()), "r") as h5:

        def print_structure(name, _obj):
            print(name)

        h5.visititems(print_structure)

# %%
with tarfile.open(fileobj=io.BytesIO(msd_bytes), mode="r:gz") as tar:
    h5_member = next(m for m in tar.getmembers() if m.name.endswith(".h5"))
    f = tar.extractfile(h5_member)
    with h5py.File(io.BytesIO(f.read()), "r") as h5:
        print("=== analysis/songs columns ===")
        print(h5["analysis/songs"].dtype.names)
        print()
        print("=== metadata/songs columns ===")
        print(h5["metadata/songs"].dtype.names)
        print()
        print("=== musicbrainz/songs columns ===")
        print(h5["musicbrainz/songs"].dtype.names)
        print()
        print("=== analysis/songs row 0 ===")
        print(h5["analysis/songs"][0])
        print()
        print("=== metadata/songs row 0 ===")
        print(h5["metadata/songs"][0])

# %%
mxm_bytes = mxm.download_as_bytes()  # ~2.4GB
with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
    tmp.write(mxm_bytes)
    tmp.flush()
    con = sqlite3.connect(tmp.name)
    # see tables
    tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(tables)
    # peek at first table
    for table in tables:
        print(f"\n--- {table[0]} ---")
        print(con.execute(f"SELECT * FROM {table[0]} LIMIT 5").fetchall())


# %%
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


def parse_h5(h5_bytes: bytes) -> dict:
    """Extract relevant fields from one HDF5 file. Returns one row."""
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


def parse_all_h5(tar_bytes: bytes) -> list[dict]:
    """Extract all tracks from the tar.gz archive."""
    rows = []
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.name.endswith(".h5"):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            rows.append(parse_h5(f.read()))
    return rows


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


# %%
genres = parse_genres(genre_bytes)
print(f"Genres: {len(genres)} rows")
print(genres[:3])

# %%
tracks = parse_all_h5(msd_bytes)
print(f"Tracks: {len(tracks)} rows")
print(tracks[:2])

# %%
lyrics = parse_lyrics(mxm_bytes)
print(f"Lyrics: {len(lyrics)} rows")
print(lyrics[:3])

# %%
