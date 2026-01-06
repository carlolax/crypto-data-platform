# 🪙 Crypto Data Platform

A serverless end-to-end data engineering pipeline that extracts cryptocurrency market data, processes it, and stores it in a Data Lake on Google Cloud Platform (GCP).

# 🪙 Crypto Data Platform (GCP Edition)

A serverless end-to-end data engineering pipeline that extracts cryptocurrency market data, processes it, and stores it in a Data Lake on Google Cloud Platform (GCP).

## 🏗️ Architecture
**ETL Flow:**
1.  **Extract (Bronze):** Python script fetches real-time prices (Bitcoin, Ethereum, Solana) from CoinGecko API.
2.  **Load:** Raw JSON data is uploaded to **Google Cloud Storage (GCS)**.
3.  **Transform (Silver):** *[In Progress]* Cleaning and flattening data into CSV format.
4.  **Infrastructure:** All cloud resources are provisioned via **Terraform**.

## 🛠️ Tech Stack
* **Language:** Python 3.12
* **Cloud:** Google Cloud Platform (GCS)
* **IaC:** Terraform
* **Containerization:** *[Planned]* Docker
* **Orchestration:** *[Planned]* GitHub Actions

## 🚀 Setup & Usage

### 1. Prerequisites
* Google Cloud SDK (`gcloud`)
* Terraform
* Python 3.x

### 2. Infrastructure Setup
```bash
cd infra
# Initialize and Apply Terraform
terraform init
terraform apply
```

### 3. Run the Pipeline
```bash
# Ingest Data (Bronze Layer)
python src/bronze/ingest.py
```

## 📂 Project Structure

```bash
├── data/           # Local data (gitignored)
├── infra/          # Terraform IaaC code
├── src/
│   ├── bronze/     # Ingestion scripts (API -> GCS)
│   ├── silver/     # Transformation scripts (GCS -> GCS)
│   └── gold/       # Feature Engineering (ML Ready)
└── README.md
```