
variable "credentials" {
  description = "Path to GCP service account credentials JSON file"
  type = string
  default = "../keys/my_creds.json"
}


variable "project" {
  description = "GCP Project ID"
  type        = string
  default     = "eastern-amp-449614-e1"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "asia-south1"
}

variable "location" {
  description = "GCP resource location"
  type        = string
  default     = "ASIA"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  type        = string
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name"
  type        = string
  default     = "eastern-amp-449614-e1-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Storage class for GCS bucket"
  type        = string
  default     = "STANDARD"
}  