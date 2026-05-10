# Lambda Architecture

## Overview

The fraud workload uses a Lambda-style architecture.

The speed layer handles transaction movement and decisions.  
The batch layer builds stable analytical tables.  
The serving layer exposes those results to SQL, BI, dashboards, metrics, and the custom UI.

---

## Diagram

![Lambda Architecture](diagrams/lambda-architecture.svg)

---

## Layers

| Layer | Components | Responsibility |
|---|---|---|
| Speed layer | Kafka + Spark Structured Streaming | Process transaction events through validation, enrichment, scoring, and decisioning |
| Batch layer | Airflow + Trino | Build gold-layer analytical tables from silver data |
| Storage layer | MinIO + Iceberg | Store bronze, silver, and gold datasets |
| Serving layer | Trino, Superset, Grafana, Prometheus exporter, Command Center | Expose analytics and operational views |

---

## Speed Layer

The speed layer produces operational transaction decisions.

It is responsible for:

- Kafka topic movement
- schema-managed messages
- Spark streaming transformations
- fraud scoring
- final decision events
- audit event generation

---

## Batch Layer

The batch layer is responsible for analytical outputs that do not need to be calculated inside every streaming micro-batch.

Validated gold outputs:

| Gold Output | Rows |
|---|---:|
| `daily_fraud_summary` | 182 |
| `uid_risk_profile` | 37,531 |
| `hourly_fraud_pattern` | 24 |
| `rule_performance` | 8 |

---

## Serving Layer

Serving consumers include:

- Trino SQL
- Grafana dashboards
- Superset analytics
- fraud metrics exporter
- Platform Command Center

The serving layer makes the platform demoable and inspectable from multiple angles.
