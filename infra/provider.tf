terraform {
    required_providers {
        google = {
            source  = "hashicorp/google"
            version = ">= 4.0.0" # Use a stable version
        }
    }
}

provider "google" {
    # We will pass these in via a secure file later
    project = var.gcp_project
    region  = var.gcp_region
}