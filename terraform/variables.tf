variable "project" {
  description = "GCP Project ID"
  default     = "million-songs-pipeline"
}

variable "region" {
  description = "GCP region"
  default     = "europe-central2"
}

variable "location" {
  description = "GCP location for multi-region resources"
  default     = "EU"
}

variable "gcs_bucket_name" {
  description = "GCS bucket for raw and processed data"
  default     = "million-songs-pipeline-data"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset"
  default     = "million_songs"
}
