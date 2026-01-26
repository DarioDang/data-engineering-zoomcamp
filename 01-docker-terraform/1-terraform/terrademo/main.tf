terraform {
    required_providers {
      google = {
        source = "hashicorp/google"
        version = "5.6.0"
      }
    }
}

provider "google" {
    # Configuration options 
    credentials = "./keys/my-creds.json"
    project = "de-zoomcamp-project-485521"
    region = "australia-southeast1" 
}


resource "google_storage_bucket" "terraform-demo-bucket" {
  name          = "dario-terraform-bucket-zoomcamp"
  location      = "australia-southeast1"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}