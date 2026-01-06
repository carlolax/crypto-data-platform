# ---------------------------------------------------------
# BRONZE LAYER: The Data Lake
# ---------------------------------------------------------

resource "google_storage_bucket" "data_lake" {
    name    = var.bucket_name
    location = var.gcp_region
    force_destroy = true # Allows us to delete the bucket even if it has files (Practice mode only)

    # FREE TIER OPTIMIZATION:
    # Standard storage class is cheap.
    storage_class = "STANDARD"

    # Security: Block public access (No accidental leaks)
    public_access_prevention = "enforced"

    # Versioning: If we overwrite a file, keep the old one (Backup)
    versioning {
        enabled = true
    }
}
