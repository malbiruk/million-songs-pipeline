"""Tests for trigger_cloud_run_job in flows/_cloud.py."""

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("REGION", "us-central1")

from flows._cloud import trigger_cloud_run_job  # noqa: E402

_PROJECT = "test-project"
_REGION = "us-central1"
_JOB = "my-job"
_EXPECTED_NAME = f"projects/{_PROJECT}/locations/{_REGION}/jobs/{_JOB}"


def _make_execution(*, succeeded: int = 1, failed: int = 0, total: int = 1):
    ex = MagicMock()
    ex.succeeded_count = succeeded
    ex.failed_count = failed
    ex.task_count = total
    ex.conditions = []
    return ex


@pytest.fixture()
def mock_jobs_client():
    with patch("flows._cloud.run_v2.JobsClient") as cls:
        client = MagicMock()
        cls.return_value = client
        yield client


def test_run_job_called_with_correct_name(mock_jobs_client):
    """run_job receives a RunJobRequest whose name encodes project/region/job."""
    mock_jobs_client.run_job.return_value.result.return_value = _make_execution()
    trigger_cloud_run_job.fn(_JOB)
    request = mock_jobs_client.run_job.call_args.kwargs["request"]
    assert request.name == _EXPECTED_NAME


def test_run_job_passes_args_as_container_overrides(mock_jobs_client):
    """When args are provided, they appear in container_overrides."""
    mock_jobs_client.run_job.return_value.result.return_value = _make_execution()
    trigger_cloud_run_job.fn(_JOB, args=["--dataset", "lyrics"])
    request = mock_jobs_client.run_job.call_args.kwargs["request"]
    assert request.overrides.container_overrides[0].args == ["--dataset", "lyrics"]


def test_failed_execution_raises_runtime_error(mock_jobs_client):
    """RuntimeError is raised when the execution reports failed_count > 0."""
    mock_jobs_client.run_job.return_value.result.return_value = _make_execution(
        succeeded=0,
        failed=1,
        total=1,
    )
    with pytest.raises(RuntimeError):
        trigger_cloud_run_job.fn(_JOB)


def test_successful_execution_does_not_raise(mock_jobs_client):
    """Happy path completes without raising when succeeded_count == task_count."""
    mock_jobs_client.run_job.return_value.result.return_value = _make_execution(
        succeeded=2,
        failed=0,
        total=2,
    )
    trigger_cloud_run_job.fn(_JOB)
