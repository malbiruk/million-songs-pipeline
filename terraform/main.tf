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
  dataset_id = var.bq_dataset_name
  location   = var.location
}

# resource "google_dataproc_cluster" "spark_cluster" {
#   name   = "million-songs-spark"
#   region = var.region

#   cluster_config {
#     master_config {
#       num_instances = 1
#       machine_type  = "n1-standard-2"
#     }

#     worker_config {
#       num_instances = 2
#       machine_type  = "n1-standard-2"
#     }

#     software_config {
#       image_version = "2.2-debian12"
#     }

#     lifecycle_config {
#       idle_delete_ttl = "1800s"
#     }
#   }
# }
