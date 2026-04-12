"""Shared helper: trigger a Cloud Run Job and block until its execution finishes."""

import os

from google.cloud import run_v2
from prefect import task

PROJECT = os.environ["GCP_PROJECT_ID"]
REGION = os.environ["REGION"]


@task(log_prints=True, retries=1)
def trigger_cloud_run_job(job_name: str, args: list[str] | None = None) -> None:
    """Run a Cloud Run Job (optionally with per-execution arg overrides) and wait.

    Raises RuntimeError if the execution reports any failed tasks.
    """
    client = run_v2.JobsClient()
    name = f"projects/{PROJECT}/locations/{REGION}/jobs/{job_name}"

    request = run_v2.RunJobRequest(name=name)
    if args:
        request.overrides = run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(args=args),
            ],
        )

    label = f"{job_name}{' ' + ' '.join(args) if args else ''}"
    print(f"Triggering Cloud Run Job: {label}")
    execution = client.run_job(request=request).result()

    if execution.failed_count or execution.succeeded_count != execution.task_count:
        raise RuntimeError(
            f"Cloud Run Job {label} failed: "
            f"succeeded={execution.succeeded_count} "
            f"failed={execution.failed_count} "
            f"conditions={list(execution.conditions)}",
        )
    print(f"Cloud Run Job {label} succeeded")
