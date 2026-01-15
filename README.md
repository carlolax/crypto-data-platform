# ☁️ Crypto Data Platform (GCP + Python + Terraform)

A serverless, event-driven data engineering platform that ingests, processes, and analyzes cryptocurrency market data. This project uses **Infrastructure as Code (IaC)** to deploy a scalable, self-healing architecture on Google Cloud Platform and includes a **Strategy Command Center** for visualization.

## 🏗 Architecture

**Region:** `australia-southeast1` (Sydney)

The pipeline follows a "Medallion Architecture" (Bronze → Silver → Gold), where each stage automatically triggers the next, ending in a visualization layer.

1.  **Ingestion (Bronze Layer):**
    * **Source:** CoinGecko API.
    * **Compute:** Google Cloud Function (Python 3.10).
    * **Trigger:** Cloud Scheduler (Daily cron job).
    * **Storage:** Google Cloud Storage (Raw JSON).
    * **Function:** `bronze-ingest-func`

2.  **Processing (Silver Layer):**
    * **Trigger:** Event-Driven (Fires immediately when data lands in Bronze).
    * **Logic:** Pandas (Local) / DuckDB (Cloud) for cleaning and unpivoting.
    * **Transformation:** Handles missing fields, normalizes paths, and converts JSON to Columnar format (Parquet).
    * **Storage:** Google Cloud Storage (Parquet).
    * **Function:** `silver-process-func`

3.  **Analytics (Gold Layer):**
    * **Trigger:** Event-Driven (Fires immediately when data lands in Silver).
    * **Logic:** Aggregation & Window Functions.
        * Calculates **Avg/Min/Max Prices**.
        * Generates **Market Summary Reports**.
    * **Storage:** Google Cloud Storage (Aggregated CSV/Parquet).
    * **Function:** `gold-analyze-func`

4.  **Visualization (The Command Center):**
    * **Tool:** Streamlit (Python-based UI).
    * **Charts:** Plotly Interactive Graphs.
    * **Feature:** Connects directly to the Gold Bucket to visualize signals and price trends in real-time.

## 🛠 Tech Stack

* **Language:** Python 3.10
* **Infrastructure:** Terraform
* **Data Processing:** Pandas (Local), DuckDB (Cloud/OLAP)
* **Cloud:** Google Cloud Platform (Functions, Storage, Scheduler, IAM, Pub/Sub)
* **Visualization:** Streamlit, Plotly
* **Testing:** Pytest, Mocks (unittest.mock)
* **Data Format:** JSON (Raw) → Parquet (Compressed) → CSV (Analytics)

## 📂 Project Structure

```text
.
├── infra/                  # Terraform Infrastructure code
│   ├── main.tf             # Resource definitions (Buckets, Functions, IAM)
│   ├── variables.tf        # Input variable declarations
│   └── terraform.tfvars    # Configuration values (Region, IDs)
├── src/
│   ├── cloud_functions/    # Production-ready Cloud Functions
│   │   ├── bronze/         # Ingestion Logic (main.py)
│   │   ├── silver/         # Transformation Logic (Event-Driven)
│   │   └── gold/           # Analytics & Signals Logic (Event-Driven)
│   ├── pipeline/           # Local Data Pipeline Logic
│   │   ├── bronze/         # Local ingestion script (ingest.py)
│   │   ├── silver/         # Local cleaning script (clean.py)
│   │   ├── gold/           # Local analytics script (analyze.py)
│   │   └── run_pipeline.py # Pipeline Orchestrator (Runs all layers)
│   └── dashboard.py        # Streamlit Strategy Dashboard
├── tests/                  # Unit Test Suite
│   ├── test_bronze.py      # Bronze Layer Tests (Mocked API)
│   └── test_silver.py      # Silver Layer Tests (Mocked GCS + Real DuckDB)
├── data/                   # Local data storage (for testing)
│   ├── bronze/             # Raw JSON files
│   ├── silver/             # Cleaned Parquet files
│   └── gold/               # Final Aggregated CSVs
└── README.md
```

## 🚀 Deployment Guide

### Prerequisites
- Google Cloud SDK (gcloud) installed and authenticated.
- Terraform installed.
- Python 3.10+ installed.

### 1. Infrastructure Setup
Navigate to the infrastructure folder and apply the Terraform configuration.

```bash
cd infra
terraform init
terraform plan
terraform apply
```

### 2. Manual Trigger (The "Domino Effect")
You only need to trigger the Bronze function. The rest of the pipeline is fully automated.
1. Trigger Bronze (Ingests API data).
2. Silver auto-starts (Cleans & Converts to Parquet).
3. Gold auto-starts (Calculates Financial Signals).

```bash
gcloud functions call bronze-ingest-func \
  --region=australia-southeast1 \
  --data='{}'
```

### 3. Verification & Visualization
To see the results in the Strategy Command Center:
```bash
# Authenticate locally to read from GCS
gcloud auth application-default login

# Launch the Dashboard
streamlit run src/dashboard.py
```

## 🧪 Unit Testing
The project includes a robust test suite using `pytest` and `mocks` to verify logic without incurring cloud costs or hitting API rate limits.
```bash
# Set Python Path (Important for imports)
export PYTHONPATH=$PYTHONPATH:$(pwd)/src

# Run all tests with verbose output
python -m pytest tests/ -v
```

## 🧪 Local Development
To run the logic locally without deploying to the cloud:
```bash
# Activate environment
source crypto-env/bin/activate

# Run the Orchestrator
python src/pipeline/run_pipeline.py
```
*Alternatively, you can run individual layers manually:*
```bash
python src/pipeline/bronze/ingest.py
python src/pipeline/silver/clean.py
python src/pipeline/gold/analyze.py
```

## 🛡 Security
- **Service Account**: Uses a dedicated `crypto-runner-sa` with restricted permissions (`storage.admin`).
- **Data Sovereignity**: All resources confined to `australia-southeast1`.
- **Secrets**: No API keys committed to the repository.
- **Circuit Breaker**: API ingestion includes error handling to halt pipeline on 4xx/5xx errors.
- **Schema Enforcement**: Strict typing in DuckDB prevents pipeline crashes from bad API data.
