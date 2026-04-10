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

msd_blob = bucket.blob("raw/msd_summary_file.h5")
genre_blob = bucket.blob("raw/msd_tagtraum_cd2c.cls.zip")
mxm_blob = bucket.blob("raw/mxm_dataset.db")

# %% Explore MSD summary file (300MB, all 1M tracks in one HDF5)
msd_bytes = msd_blob.download_as_bytes()

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

# %% Explore genres (1.6MB zip)
genre_bytes = genre_blob.download_as_bytes()

with zipfile.ZipFile(io.BytesIO(genre_bytes)) as z:
    print(z.namelist())
    with z.open(z.namelist()[0]) as f:
        for i, line in enumerate(f):
            print(line.decode("utf-8").strip())
            if i > 15:
                break

# %% Explore lyrics (2.4GB SQLite)
mxm_bytes = mxm_blob.download_as_bytes()

with tempfile.NamedTemporaryFile(suffix=".db", dir="/home/klim") as tmp:
    tmp.write(mxm_bytes)
    tmp.flush()
    con = sqlite3.connect(tmp.name)
    tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print("Tables:", tables)
    print("Row count:", con.execute("SELECT COUNT(*) FROM lyrics").fetchone())
    print(
        "Distinct tracks:",
        con.execute("SELECT COUNT(DISTINCT track_id) FROM lyrics").fetchone(),
    )
    print("Sample:", con.execute("SELECT * FROM lyrics LIMIT 5").fetchall())
    con.close()

# %%
