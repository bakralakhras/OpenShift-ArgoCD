# Validation Summary

## Overview

This page summarizes what was validated from the live environment and what still needs cleanup.

Generated validation date:

```text
May 10, 2026
```

---

## Validated

| Area | Result |
|---|---|
| OpenShift | Client/server 4.21.7 |
| Kubernetes | v1.34.5 |
| Nodes | 8 Ready nodes |
| Masters | 3 schedulable masters |
| Workers | 5 workers |
| ArgoCD app manifests | 27 |
| Kafka topics | 12 total: 6 pipeline + 6 DLQ |
| Airflow DAG files | 5 |
| Trino catalogs | `hive`, `iceberg`, `jmx`, `memory`, `system`, `tpcds`, `tpch` |
| Iceberg schemas | `warehouse_bronze`, `warehouse_silver`, `warehouse_gold` |
| Bronze transactions | 590,540 |
| Silver decisions | 994,376 |
| Silver transactions | 994,376 |
| Gold daily summaries | 182 |
| Gold UID risk profiles | 37,531 |
| Gold hourly patterns | 24 |
| Gold rule performance rows | 8 |
| Fraud total transactions metric | 994,376 |
| Fraud count metric | 34,391 |
| Average fraud rate metric | 3.604% |
| Blocked decisions metric | 7 |
| Platform Command Center | Frontend/backend running and serving API traffic |

---

## Known Gaps / Active Cleanup

| Area | Current Behavior |
|---|---|
| ArgoCD | Several apps are OutOfSync, Progressing, Missing, or Degraded |
| Rook-Ceph | Storage components running, but ArgoCD application degraded |
| VaultStaticSecret | Several sync resources are not healthy |
| Schema Registry | Running but had high restart count and Progressing ArgoCD state |
| Wider OpenShift cluster | Many non-platform system pods in transitional states |
| DLQ metric | Topic structure exists; clean DLQ count should be validated using offsets, not console-consumer line count |

---

## Documentation Position

The project should be described as:

> A working OpenShift data platform with validated lakehouse, streaming, orchestration, observability, and custom UI components, currently packaged as an engineering case study with known active-development cleanup items.

It should not be described as:

> A perfectly healthy production cluster with every component fully reconciled.

That distinction makes the project more credible.
