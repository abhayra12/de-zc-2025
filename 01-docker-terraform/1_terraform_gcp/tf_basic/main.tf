terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.19.0"
    }
  }
}

provider "google" {
  project = "eastern-amp-449614-e1"
  region  = "asia-south1"
}

# create gcp bucket 
resource "google_storage_bucket" "demo-bucket" {
  name          = "eastern-amp-449614-e1-terra-bucket"
  location      = "ASIA"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}