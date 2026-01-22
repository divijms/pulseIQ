# PulseIQ — Project Context & Execution Notes

## Project Vision
PulseIQ is a real-time behavioral intelligence platform that ingests high-volume product events, detects anomalies in near real time, and explains their root causes using a modern cloud-native data engineering architecture.

This project is designed to demonstrate production-grade data engineering skills, not as a toy or academic pipeline.

---

## Core Problem Statement
Modern digital products struggle to detect and explain abnormal user or system behavior in real time (traffic spikes, conversion drops, latency regressions).

PulseIQ answers:
- Is something abnormal happening right now?
- What dimension caused it (region, feature, device)?
- How severe is the anomaly?

---

## Architectural Philosophy
- Event-driven, streaming-first design
- Lakehouse architecture (Bronze / Silver / Gold)
- Explainable anomaly detection (not black-box ML)
- Fault-tolerant, replayable, cost-aware pipelines
- Local-first development with cloud parity

---

## Data Characteristics
- Real-time product telemetry events (user actions, transactions, system metrics)
- Event-time processing with late data handling
- Controlled anomaly simulation for validation

---

## Technology Stack (Local, AWS-Compatible)
- Kafka (streaming ingestion)
- Spark Structured Streaming (processing)
- Delta Lake (lakehouse)
- Airflow (orchestration)
- Docker + Docker Compose (infrastructure)
- LocalStack / MinIO (S3-compatible storage)

Cloud Mapping (Design-Level):
- S3 → Object storage
- EMR → Spark
- MSK → Kafka
- MWAA → Airflow

No real AWS resources are used to avoid cost.

---

## Execution Environment
- Windows + WSL2 (Ubuntu)
- Docker Desktop with WSL integration enabled
- All development performed inside Ubuntu (Linux parity)

---

## Engineering Principles Followed
- Clear separation of concerns
- Schema-first event design
- Idempotent, replayable pipelines
- Quarantine for bad data
- Observability and failure recovery
- Interview-defensible design decisions

---

## Portfolio Positioning
PulseIQ complements an existing domain-specific pipeline (e.g., CricPulse) by showcasing:
- Product-level thinking
- Real-time intelligence systems
- Senior-level data engineering execution
