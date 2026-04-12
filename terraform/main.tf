terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

# --- Data lake + warehouse ---

resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "million_songs" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.location
  delete_contents_on_destroy = true
}

# --- Artifact Registry (Docker image host for Cloud Run Jobs) ---

resource "google_artifact_registry_repository" "pipeline" {
  location      = var.region
  repository_id = var.ar_repo_name
  format        = "DOCKER"
  description   = "Million Songs pipeline container images"
}

# --- Cloud Run Jobs ---

locals {
  common_env = [
    { name = "GCS_BUCKET", value = var.gcs_bucket_name },
  ]
}

resource "google_cloud_run_v2_job" "ingest" {
  name                = "million-songs-ingest"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = var.sa_email
      timeout         = "1800s"
      max_retries     = 0

      containers {
        image   = var.image_uri
        command = ["python", "-m", "jobs.ingest"]

        dynamic "env" {
          for_each = local.common_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }
      }
    }
  }

  depends_on = [google_artifact_registry_repository.pipeline]
}

resource "google_cloud_run_v2_job" "transform" {
  name                = "million-songs-transform"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = var.sa_email
      timeout         = "1800s"
      max_retries     = 0

      containers {
        image   = var.image_uri
        command = ["python", "-m", "jobs.transform"]

        dynamic "env" {
          for_each = local.common_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }
      }
    }
  }

  depends_on = [google_artifact_registry_repository.pipeline]
}
