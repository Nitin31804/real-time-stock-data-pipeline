# Stock Price Predictor - Real-Time Data Pipeline

![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-Processing-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Cassandra](https://img.shields.io/badge/Cassandra-NoSQL-1287B1?style=for-the-badge&logo=apache-cassandra&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Orchestration-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white)

> A production-grade, fault-tolerant distributed data pipeline for ingesting, processing and analyzing high-frequency stock market data in real-time — built with the same architecture used at Uber and Netflix.

---

## Architecture

```
[Stock Market API]
        |
        v
[Apache Kafka] ---- Message Broker / Event Stream
        |
        v
[Apache Spark] ---- Distributed Stream Processing / Prediction Engine
        |
        v
[Cassandra DB] ---- Fault-Tolerant Distributed NoSQL Storage
        |
        v
[Dashboard] ------- Real-Time Visualization
        
All orchestrated by: Kubernetes + Docker Compose
```

---

## Key Features

- **High-Throughput Ingestion** — Kafka handles millions of price tick events per second
- **Distributed Processing** — Spark parallelizes computation across multiple worker nodes
- **Zero-Downtime Storage** — Cassandra replicates data across nodes for 99.99% availability
- **Pre-Flight Safety Checks** — Makefile validates Kubernetes cluster health before deployment
- **Infrastructure as Code** — Fully declarative Docker Compose + Kubernetes manifests

---

## Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Message Broker | Apache Kafka | Ingest high-frequency tick data |
| Stream Processor | Apache Spark | Real-time distributed computation |
| Database | Apache Cassandra | Fault-tolerant distributed storage |
| Orchestration | Kubernetes | Container management and scaling |
| Containerization | Docker | Service isolation and portability |

---

## Getting Started

> Requires: Docker, Docker Compose, and a running Kubernetes cluster (Minikube or Docker Desktop K8s)

```bash
# Clone the repository
git clone https://github.com/Nitin31804/Stock-price-predictor.git
cd Stock-price-predictor

# Boot the full distributed cluster (with K8s pre-flight check)
make up

# Check pod health
make status

# Tear down safely
make down
```

---

## Why This Stack?

| Requirement | Solution |
|---|---|
| Handle millions of events/sec | Apache Kafka |
| Process data faster than one machine can | Apache Spark (distributed) |
| Never lose data, even if a server crashes | Apache Cassandra (replication) |
| Deploy anywhere consistently | Docker + Kubernetes |

---

## Infrastructure Notes

This is an **enterprise-grade backend system** designed for cloud deployment (AWS, GCP, Azure). Running locally requires Docker with at least **16GB RAM** assigned to simulate the distributed cluster on a single machine.

