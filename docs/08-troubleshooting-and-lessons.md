# Troubleshooting and Lessons

## Overview

This project became useful because it broke in real ways.

These are the engineering problems that shaped the final platform.

---

## External Image Pull Issues

Some images were unreliable from the cluster network.

Resolution:

- build custom images with Podman
- push to the OpenShift internal registry
- reference internal image paths from platform manifests

Lesson:

> For controlled OpenShift environments, internal image registry usage is often more reliable than depending on external registries.

---

## Spark / Hive Metastore Compatibility

Spark and Hive Metastore 4.x behavior caused compatibility issues for Iceberg workflows.

Resolution:

- use Hadoop catalog for Spark writes
- use Trino and Hive Metastore for querying/serving

Lesson:

> Spark, Hive, Iceberg, and Trino version boundaries matter.

---

## S3A Write Behavior Against MinIO

Spark object storage writes required careful S3A committer configuration.

Resolution included using:

```python
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2
```

Lesson:

> Object storage is not a filesystem. Spark commit behavior must be configured intentionally.

---

## ArgoCD and One-Shot Jobs

ArgoCD can re-apply completed one-shot jobs if they remain in desired state.

Resolution:

- remove completed one-shot jobs from active reconciliation
- use Airflow for recurring workflows

Lesson:

> GitOps should manage platform state. Workflow engines should manage recurring data jobs.

---

## SparkApplication CPU Validation

SparkApplication specs need valid relationships between cores and CPU limits.

Lesson:

> Spark Operator validation rules are part of the deployment contract.

---

## Schema Registry Performance

Fetching schema data per message caused avoidable overhead.

Resolution:

- fetch schema once
- broadcast or reuse schema in the job

Lesson:

> Never hide network calls in row-level Spark logic.

---

## Airflow 3.x Operational Changes

Airflow 3.x changed familiar CLI/logging behavior compared to Airflow 2.x.

Lesson:

> Version upgrades affect runbooks, not just code.

---

## Trino / Iceberg Schema Mismatch

Gold table writes required exact schema alignment.

Resolution:

- inspect tables with `DESCRIBE`
- update SQL to match actual columns

Lesson:

> Lakehouse schemas and transformation SQL must stay tightly aligned.

---

## Grafana Operator Plugin Limitations

Grafana plugin behavior was too limiting for the intended operations console.

Resolution:

- stop forcing Grafana to be the product UI
- build a custom React + FastAPI Command Center

Lesson:

> Sometimes the right engineering decision is to stop bending a tool and build the missing layer.
