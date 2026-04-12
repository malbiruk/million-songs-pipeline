"""Tests for the lyrics batching loop in jobs/transform.py.

Mocking strategy: the real transform_lyrics function allocates two tempfiles
internally (one .db, one .parquet). We intercept at the GCS blob boundary:
- download_to_filename receives the already-created tempfile path and we
  copy a real SQLite DB into it.
- upload_from_filename receives the already-written parquet path and we
  copy it out to a location we control so assertions can read it back.

No GCS network calls are made.
"""

import os
import shutil
import sqlite3
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

os.environ.setdefault("GCS_BUCKET", "test-bucket")

from jobs.transform import transform_lyrics  # noqa: E402


_NUM_ROWS = 2000


def _build_sqlite_db(path: str, num_rows: int) -> None:
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE lyrics (track_id TEXT, word TEXT, count INTEGER)")
    con.executemany(
        "INSERT INTO lyrics VALUES (?, ?, ?)",
        [(f"TRK{i:06d}", f"word{i % 50}", i % 100 + 1) for i in range(num_rows)],
    )
    con.commit()
    con.close()


@pytest.fixture()
def mock_bucket(tmp_path):
    """Mock GCS bucket that serves a real SQLite DB and captures the output parquet."""
    source_db = tmp_path / "source.db"
    _build_sqlite_db(str(source_db), _NUM_ROWS)
    captured: list[str] = []

    def _download(dest_path: str) -> None:
        shutil.copy(str(source_db), dest_path)

    def _upload(src_path: str) -> None:
        out = tmp_path / "output.parquet"
        shutil.copy(src_path, str(out))
        captured.append(str(out))

    db_blob = MagicMock()
    db_blob.download_to_filename.side_effect = _download
    pq_blob = MagicMock()
    pq_blob.upload_from_filename.side_effect = _upload

    bucket = MagicMock()
    bucket.blob.side_effect = lambda name: db_blob if name.endswith(".db") else pq_blob
    bucket._captured = captured
    return bucket


def _read_output(mock_bucket) -> pq.ParquetFile:
    assert mock_bucket._captured, "upload_from_filename was never called"
    return pq.ParquetFile(mock_bucket._captured[0])


def test_lyrics_row_count(mock_bucket):
    """Written parquet contains exactly as many rows as the source SQLite table."""
    transform_lyrics(mock_bucket)
    pf = _read_output(mock_bucket)
    assert pf.metadata.num_rows == _NUM_ROWS


def test_lyrics_schema(mock_bucket):
    """Parquet schema matches (track_id: string, word: string, count: int32)."""
    transform_lyrics(mock_bucket)
    schema = _read_output(mock_bucket).schema_arrow
    assert schema.field("track_id").type == pa.string()
    assert schema.field("word").type == pa.string()
    assert schema.field("count").type == pa.int32()


def test_lyrics_values_round_trip(mock_bucket):
    """A sampled row in the parquet matches the value inserted into SQLite."""
    transform_lyrics(mock_bucket)
    table = _read_output(mock_bucket).read().to_pydict()
    idx = table["track_id"].index("TRK000042")
    assert table["word"][idx] == "word42"
    assert table["count"][idx] == 43  # (42 % 100) + 1
