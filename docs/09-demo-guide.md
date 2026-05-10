# Demo Guide

## Demo Goal

Show the project as a platform engineering case study, not only a fraud pipeline.

The demo should tell this story:

> This is a GitOps-managed OpenShift data platform. Fraud detection is the workload proving the platform.

---

## Recommended Flow

### 1. README and Architecture

Show:

- architecture diagram
- validated metrics
- current validation state

Make it clear that the project is honest about active-development drift.

---

### 2. ArgoCD

```bash
oc get applications -n openshift-gitops
```

Explain:

- App of Apps model
- 27 applications
- some drift exists because this is an active lab

---

### 3. OpenShift Nodes

```bash
oc get nodes -o wide
```

Show:

- 8 Ready nodes
- 3 schedulable masters
- 5 workers

---

### 4. Kafka Topics

```bash
oc get kafkatopics -n kafka
```

Show the 6 pipeline topics and 6 DLQ companions.

---

### 5. Trino Lakehouse Proof

```bash
TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')

oc exec -n trino "$TRINO_POD" -- \
  trino --execute "SHOW SCHEMAS FROM iceberg"

oc exec -n trino "$TRINO_POD" -- \
  trino --execute "SELECT count(*) FROM iceberg.warehouse_bronze.transactions"

oc exec -n trino "$TRINO_POD" -- \
  trino --execute "SELECT count(*) FROM iceberg.warehouse_silver.decisions"
```

---

### 6. Airflow DAGs

```bash
oc exec -n airflow sts/airflow-scheduler -- \
  bash -lc 'ls -lah /opt/airflow/dags'
```

Show the 5 DAG files.

---

### 7. Observability

Show:

- Grafana
- fraud exporter metrics
- PrometheusRule
- ServiceMonitor

---

### 8. Platform Command Center

Show:

- Overview
- Infrastructure
- GitOps
- Data Platform
- Incidents

This is one of the strongest parts of the project. Explain why it was built.

---

## Closing Line

Use this:

> The fraud pipeline matters, but the bigger result is the platform underneath it: GitOps, OpenShift, Kafka, Spark, Iceberg, Trino, Airflow, observability, secrets, and a custom operational UI working together.
