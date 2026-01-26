variable "credentials"{
    description = "My Credentials"
    default = "./keys/my-creds.json"
}

variable "project" {
    description = "Project"
    default = "de-zoomcamp-project-485521"
}

variable "region" {
    description = "Region"
    default = "australia-southeast1"
}

variable "gcs_location" {
    description = "GCS Bucket Location"
    default = "australia-southeast1"
}

variable "bq_dataset_name" {
    description = "My BigQuery Dataset Name"
    default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "My Storage Bucket Name"
    default = "dario-terraform-bucket-zoomcamp"
}

variable "gcs_storage_class"{
    description = "Bucket Storage Class"
    default = "STANDARD"
}