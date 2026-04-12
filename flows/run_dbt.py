"""Run dbt transformations and data-quality tests."""

import subprocess
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

DBT_DIR = str(Path(__file__).resolve().parent.parent / "dbt")


@task(log_prints=True)
def dbt_build() -> None:
    """Run dbt models and tests (`dbt build` = run + test in dependency order)."""
    result = subprocess.run(
        ["dbt", "build", "--profiles-dir", "."],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("dbt build failed")


@flow(name="dbt-transform", log_prints=True)
def run_dbt():
    """Run all dbt transformations and their schema tests."""
    dbt_build()


if __name__ == "__main__":
    run_dbt()
