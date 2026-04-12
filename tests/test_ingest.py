"""Tests for jobs/ingest.py CLI argument validation."""

import os
import subprocess
import sys
from pathlib import Path


def test_ingest_rejects_unknown_dataset():
    """Passing --dataset with an unknown value exits non-zero (argparse choices guard)."""
    env = {**os.environ, "GCS_BUCKET": "dummy-bucket"}
    result = subprocess.run(
        [sys.executable, "-m", "jobs.ingest", "--dataset", "bogus"],
        capture_output=True,
        env=env,
        cwd=Path(__file__).resolve().parent.parent,
        check=False,
    )
    assert result.returncode != 0
