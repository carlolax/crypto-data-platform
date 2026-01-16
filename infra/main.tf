# ==========================================
# 1. SECURITY & IDENTITY
# ==========================================

resource "google_service_account" "function_runner" {
  account_id   = "crypto-runner-sa"
  display_name = "Crypto Ingestion Runner"
}

resource "google_project_iam_member" "runner_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin" 
  member  = "serviceAccount:${google_service_account.function_runner.email}"
}

# ==========================================
# 2. STORAGE BUCKETS (The Layers)
# ==========================================

# --- BRONZE LAYER (Raw Data) ---
resource "google_storage_bucket" "data_lake" {
  name                     = var.bucket_name
  location                 = var.region
  force_destroy            = true
  storage_class            = "STANDARD"
  public_access_prevention = "enforced"
  
  versioning { enabled = true }
}

# --- SILVER LAYER (Clean Data) ---
resource "google_storage_bucket" "silver_layer" {
  name                     = "crypto-silver-${var.project_id}"
  location                 = var.region
  force_destroy            = true
  storage_class            = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning { enabled = true }

  labels = {
    environment = "dev"
    layer       = "silver"
  }
}

# --- GOLD LAYER (Aggregated Data) ---
resource "google_storage_bucket" "gold_layer" {
  name                     = "crypto-gold-${var.project_id}"
  location                 = var.region
  force_destroy            = true
  storage_class            = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning { enabled = true }

  labels = {
    environment = "dev"
    layer       = "gold"
  }
}

# ==========================================
# 3. CLOUD FUNCTIONS INFRASTRUCTURE
# ==========================================

resource "google_storage_bucket" "function_source" {
  name                        = "${var.project_id}-function-source"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

data "archive_file" "bronze_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/bronze"
  output_path = "${path.module}/bronze_function.zip"
}

resource "google_storage_bucket_object" "bronze_zip_upload" {
  name   = "bronze-ingest-${data.archive_file.bronze_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.bronze_zip.output_path
}

resource "google_cloudfunctions_function" "bronze_ingest" {
  name        = "bronze-ingest-func"
  description = "Ingests Crypto Data to Bronze Bucket"
  runtime     = "python310"

  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.bronze_zip_upload.name
  trigger_http          = true
  entry_point           = "process_data_ingestion"
  
  service_account_email = google_service_account.function_runner.email

  environment_variables = {
    BRONZE_BUCKET_NAME = google_storage_bucket.data_lake.name
  }
}

resource "google_cloud_scheduler_job" "daily_bronze_trigger" {
  name        = "daily-crypto-ingest"
  description = "Triggers the Bronze Ingestion Function daily"
  schedule    = "0 6 * * *"
  time_zone   = "Australia/Brisbane"

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions_function.bronze_ingest.https_trigger_url
    
    oidc_token {
      service_account_email = google_service_account.function_runner.email
    }
  }
}

# ==============================================================================
# SILVER LAYER: EVENT-DRIVEN FUNCTION
# ==============================================================================

# 1. Zip the Silver Source Code
data "archive_file" "silver_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/silver"
  output_path = "${path.module}/silver_function.zip"
}

# 2. Upload Zip to Source Bucket
resource "google_storage_bucket_object" "silver_zip_upload" {
  name   = "silver-process-${data.archive_file.silver_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.silver_zip.output_path
}

# 3. The Silver Cloud Function
resource "google_cloudfunctions_function" "silver_process" {
  name        = "silver-process-func"
  description = "Event-driven: Process Bronze JSON to Silver Parquet"
  runtime     = "python310"
  region      = var.region
  project     = var.project_id

  available_memory_mb   = 512 
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.silver_zip_upload.name

  entry_point           = "process_data_cleaning"
  service_account_email = google_service_account.function_runner.email

  event_trigger {
    event_type = "google.storage.object.finalize" 
    resource   = google_storage_bucket.data_lake.name
  }

  environment_variables = {
    SILVER_BUCKET_NAME = google_storage_bucket.silver_layer.name
  }
}

# ==============================================================================
# GOLD LAYER: ANALYTICS FUNCTION
# ==============================================================================

# 1. Zip the Gold Source Code
data "archive_file" "gold_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/cloud_functions/gold"
  output_path = "${path.module}/gold_function.zip"
}

# 2. Upload Zip to Source Bucket
resource "google_storage_bucket_object" "gold_zip_upload" {
  name   = "gold-analyze-${data.archive_file.gold_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.gold_zip.output_path
}

# 3. The Gold Cloud Function
resource "google_cloudfunctions_function" "gold_analyze" {
  name        = "gold-analyze-func"
  description = "Event-driven: Calculate Market Signals"
  runtime     = "python310"
  region      = var.region
  project     = var.project_id

  available_memory_mb   = 512 
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.gold_zip_upload.name

  entry_point           = "process_data_analytics"
  service_account_email = google_service_account.function_runner.email

  event_trigger {
    event_type = "google.storage.object.finalize" 
    resource   = google_storage_bucket.silver_layer.name
  }

  environment_variables = {
    GOLD_BUCKET_NAME = google_storage_bucket.gold_layer.name
  }
}
