"""Run dbt transformations."""

import subprocess
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

DBT_DIR = str(Path(__file__).resolve().parent.parent / "dbt")


@task(log_prints=True)
def dbt_run() -> None:
    """Run dbt models."""
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("dbt run failed")


@flow(name="dbt-transform", log_prints=True)
def run_dbt():
    """Run all dbt transformations."""
    dbt_run()


if __name__ == "__main__":
    run_dbt()
