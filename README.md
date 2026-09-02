# ?? Real-Time Enterprise Stock Data Pipeline

A production-grade, highly concurrent streaming data pipeline for live stock market data. This project demonstrates end-to-end data engineering, cloud-native infrastructure, and a Bloomberg-style terminal UI.

`mermaid
graph LR
    A[Finnhub WS / Mocks] --> B(Python Producer)
    B --> C{Apache Kafka}
    C --> D[PySpark Streaming]
    D --> E[(TimescaleDB / Postgres)]
    E --> F[Flask / Gevent API]
    F -- Server-Sent Events --> G[TradingView UI]
`

---

## ?? Key Enterprise Features (10/10 Architecture)

* **Infrastructure as Code (Terraform):** Fully codified AWS deployment via Terraform (VPC, MSK, RDS, ECS). 
* **Kubernetes Ready:** Included k8s/ manifests for deploying Spark Jobs and Flask APIs to EKS.
* **Asynchronous Concurrency:** The Flask API runs on gevent greenlets, safely serving thousands of concurrent Server-Sent Event (SSE) streams without blocking the WSGI thread pool.
* **Race Condition Prevention:** The Vanilla JS frontend utilizes AbortController to cancel in-flight API requests when rapidly switching tabs, eliminating UI state mutations.
* **Resilient API Caching:** Yahoo Finance API rates are mitigated via cachetools.TTLCache, providing lightning-fast frontend responses while preventing 429 Too Many Requests errors.
* **CI/CD Pipelines:** Automated GitHub Actions (.github/workflows/ci.yml) for linting and Pytest validation.

---

## ? Quick Start (Local Docker Compose)

### Prerequisites
- Docker Desktop (8 GB RAM recommended for Kafka + Spark)

### 1. Configure
`ash
cd stock-pipeline
cp .env.example .env
# Add your FINNHUB_API_KEY to .env for real live trade ticks
`

### 2. Launch
`ash
docker compose up -d --build
`

### 3. Open the Dashboard
| Service | URL |
|---|---|
| **Terminal Dashboard** | http://localhost:5000 |
| **Spark UI** | http://localhost:8080 |

---

## ??? Project Structure

`	ext
stock-pipeline/
+-- .github/workflows/    # CI/CD Pipelines
+-- terraform/            # AWS IaC (VPC, MSK, RDS, ECS)
+-- k8s/                  # Kubernetes Deployment Manifests
+-- docker-compose.yml    # Local Orchestration
+-- producer/             # Python Kafka Producer (Finnhub WS)
+-- spark_processor/      # PySpark Structured Streaming (1m OHLCV Aggregation)
+-- db/                   # PostgreSQL/TimescaleDB Schemas
+-- dashboard/            # Flask API & Frontend
    +-- app.py            # Gevent SSE endpoints & TTLCache
    +-- static/js/        # TradingView Integration & AbortControllers
    +-- static/css/       # Premium Trading Terminal UI (Tabular Nums, Dark Mode)
`

---

## ?? Cloud Deployment (AWS)

To deploy to a production AWS environment, use the provided Terraform configurations:

`ash
cd terraform
terraform init
terraform plan -var="db_user=admin" -var="db_password=super_secret"
terraform apply
`
*Deploys: Amazon MSK (Kafka), Amazon RDS (PostgreSQL), and Amazon ECS.*

## ?? Kubernetes Deployment

To deploy the Spark streaming job and Flask frontend to a Kubernetes cluster (e.g., EKS or Minikube):

`ash
cd k8s
kubectl apply -f dashboard-deployment.yaml
kubectl apply -f spark-job.yaml
`

---

## ??? DevOps & Testing

The repository automatically runs tests on every push and pull_request to main.
To run tests locally:
`ash
pip install pytest flake8
flake8 dashboard/ producer/ spark/
pytest tests/
`

## ?? Troubleshooting
- **No data on the chart?** Spark Structured Streaming aggregates on 1-minute windows. Wait 60 seconds after startup for the first candle to appear.
- **Kafka Unhealthy?** Kafka takes ~20 seconds to boot. The Producer and Spark job will automatically retry connections.
