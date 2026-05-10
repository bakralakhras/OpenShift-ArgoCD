# Platform Architecture

## Overview

This platform is a GitOps-managed data lakehouse running on OpenShift.

It combines infrastructure, streaming, processing, storage, orchestration, observability, BI, and a custom operations console into one working environment.

The fraud detection pipeline is the main workload running on the platform, but the platform itself is the larger engineering artifact.

---

## Architecture Diagram

![Platform Architecture](diagrams/platform-architecture.svg)

---

## Validated Environment

| Item | Value |
|---|---|
| OpenShift | 4.21.7 |
| Kubernetes | v1.34.5 |
| Cluster size | 8 nodes |
| Node layout | 3 schedulable masters + 5 workers |
| Main repo | `OpenShift-ArgoCD` |
| ArgoCD applications | 27 |

---

## Platform Layers

| Layer | Components | Responsibility |
|---|---|---|
| Runtime | OpenShift | Scheduling, networking, routes, storage, RBAC, namespaces |
| GitOps | ArgoCD / OpenShift GitOps | Declarative reconciliation of platform components |
| Streaming | Kafka KRaft via Strimzi | Fraud event bus and DLQ topic model |
| Processing | Spark + Spark Operator | Batch bootstrap, replay, and streaming jobs |
| Lakehouse | MinIO + Iceberg | Bronze, silver, and gold object-backed tables |
| Catalog | Hive Metastore | Metadata layer for lakehouse tables |
| Query | Trino | SQL access over Iceberg tables |
| Orchestration | Airflow | Scheduled silver and gold workflows |
| Schema | Apicurio Registry | Avro schema management |
| Secrets | Vault + Vault Secrets Operator | Secret delivery pattern and credential isolation |
| Observability | Prometheus, Thanos, Grafana | Metrics, alerts, dashboards |
| BI | Superset | Business-facing analytics |
| Operations | Platform Command Center | Custom operator-facing interface |

---

## Namespace Model

Important namespaces:

```text
openshift-gitops
kafka
spark
minio
lakehouse
trino
airflow
monitoring
superset
vault
schema-registry
platform-command-ui
postgresql
rook-ceph
```

The main platform namespaces are running their core workloads. The wider OpenShift lab cluster still contains some non-platform pods in transitional states, so the docs avoid claiming that the whole cluster is perfectly clean.

---

## Storage Model

The platform currently uses multiple storage classes:

```text
local-path
local-path-immediate
local-storage
nfs-storage
rook-ceph-block
rook-cephfs
```

Rook-Ceph block and CephFS storage classes exist and are used by several workloads. The active MinIO PVC observed during validation is a 50Gi local-storage PVC, so the project does not claim a larger MinIO capacity unless separately verified from MinIO itself.

---

## Why This Architecture Matters

The platform demonstrates real ownership across:

- OpenShift operations
- GitOps delivery
- Kafka topic design
- Spark job execution
- Iceberg lakehouse layout
- Trino analytics
- Airflow orchestration
- Prometheus/Grafana observability
- Vault-based secret architecture
- custom platform UI development

It is intentionally documented as a real system, including the rough edges.
