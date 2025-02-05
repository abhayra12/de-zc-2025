terraform {
  required_providers {
    google = {
      source      = "hashicorp/google"
      version     = "6.19.0"
    }
  }
}

provider "google" {
  # Configuration options
  credentials =  file(var.credentials)
  project = var.project
  region  = var.region
}

# create gcp bucket 
resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  storage_class = var.gcs_storage_class

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}

# create bigquery dataset 
resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id = var.bq_dataset_name

}