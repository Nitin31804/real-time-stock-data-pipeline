# Stock Price Predictor: Enterprise Real-Time Data Pipeline

## Overview
A production-grade, distributed data engineering pipeline designed to ingest, process, and analyze high-throughput stock market data in real-time.

## Architecture & Tech Stack
- **Message Broker:** Apache Kafka
- **Stream Processing:** Apache Spark Streaming
- **Database:** Apache Cassandra (NoSQL)
- **Container Orchestration:** Docker Compose & Kubernetes
- **Infrastructure as Code:** Terraform

## Key Features
- **High-Throughput Ingestion:** Capable of handling massive volumes of financial tick data.
- **Fault-Tolerant Distributed Storage:** Cassandra ensures data sovereignty and zero downtime.
- **Automated Deployments:** A unified `Makefile` orchestrates Docker and Kubernetes seamlessly, featuring built-in pre-flight cluster checks.

## Getting Started
Ensure you have Docker, `docker-compose`, and a local Kubernetes cluster (like Minikube or Docker Desktop K8s) actively running.
```bash
make up       # Boot the distributed cluster and apply manifests
make status   # Verify pod health
make down     # Tear down infrastructure safely
```
