<div align="center">

# Sovereign Data Platform on OpenShift

### A production-grade, GitOps-managed data lakehouse — declared in Git, deployed by ArgoCD, secured by Vault.

[![OpenShift](https://img.shields.io/badge/OpenShift-4.18-EE0000?style=flat-square&logo=redhat&logoColor=white)](https://www.redhat.com/en/technologies/cloud-computing/openshift)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.31-326CE5?style=flat-square&logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![ArgoCD](https://img.shields.io/badge/ArgoCD-GitOps-F37636?style=flat-square&logo=argo&logoColor=white)](https://argo-cd.readthedocs.io/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-3.7%20KRaft-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Iceberg-Lakehouse-1F6FEB?style=flat-square&logo=apacheiceberg&logoColor=white)](https://iceberg.apache.org/)
[![Trino](https://img.shields.io/badge/Trino-435-DD00A1?style=flat-square&logo=trino&logoColor=white)](https://trino.io/)
[![Vault](https://img.shields.io/badge/Vault-1.17-FFEC6E?style=flat-square&logo=vault&logoColor=black)](https://www.vaultproject.io/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

</div>

---

## Overview

This repository contains the complete infrastructure-as-code for a **sovereign data lakehouse** built on OpenShift 4.18. Every component — from object storage to BI dashboards — is declared in Git and reconciled by ArgoCD using the **app-of-apps pattern**. After bootstrap, a single `oc apply` command deploys the entire platform.

> **Engineering pitch:** Streaming ingestion with Kafka, an Iceberg lakehouse on MinIO, distributed SQL via Trino, BI with Superset, centralized secrets with Vault + VSO, and full observability via Prometheus + Thanos + Grafana — all reconciled by ArgoCD.

---

## Table of Contents

- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Repository Layout](#repository-layout)
- [Deployment](#deployment)
- [Platform Phases](#platform-phases)
- [End-to-End Data Flow](#end-to-end-data-flow)
- [Observability](#observability)
- [Security & Secrets Management](#security--secrets-management)
- [Engineering Highlights](#engineering-highlights)
- [Routes & Access](#routes--access)
- [Documentation](#documentation)
- [Author](#author)

---

## Architecture
![Sovereign Data Platform Architecture](Arch.png)
```

---

## Technology Stack

| Layer | Component | Version | Purpose |
|---|---|---|---|
| **Platform** | OpenShift | 4.18 | Enterprise Kubernetes |
| **GitOps** | ArgoCD (OpenShift GitOps) | — | Declarative app-of-apps reconciliation |
| **Object Storage** | MinIO | RELEASE.2024 | S3-compatible data lake backend |
| **Storage** | Local Storage Operator | — | Block storage from raw worker disks |
| **Lakehouse Format** | Apache Iceberg | — | Open table format with schema evolution |
| **Catalog** | Hive Metastore | 3.1.3 | Iceberg metadata backend |
| **Metadata DB** | PostgreSQL | 15 | Hive + Airflow + Superset backend |
| **Query Engine** | Trino | 435 | Federated distributed SQL |
| **Streaming** | Apache Kafka (KRaft) | 3.7 | Event streaming (no ZooKeeper) |
| **Stream Connectors** | Kafka Connect + S3 Sink | — | Stream-to-lake materialization |
| **Ingestion** | Apache NiFi | 1.27.0 | Visual data flow design |
| **Orchestration** | Apache Airflow | 1.20.0 chart | DAG-based workflow scheduling |
| **Processing** | Spark Operator | — | Distributed batch processing |
| **Visualization** | Apache Superset | 5.0.0 | BI dashboards & SQL Lab |
| **Secrets** | HashiCorp Vault | 1.17.2 | Centralized secret management |
| **Secret Sync** | Vault Secrets Operator | 1.3.0 | Vault → Kubernetes secret bridge |
| **Metrics** | Prometheus + Thanos | — | Metrics + long-term storage |
| **Dashboards** | Grafana | 12.4.1 | Observability visualization |

---

## Repository Layout

```
OpenShift-ArgoCD/
├── apps/                          # ArgoCD Application manifests (one per component)
│   ├── root.yaml                  # The only manually applied manifest — bootstraps everything
│   ├── namespaces.yaml
│   ├── minio.yaml
│   ├── postgresql.yaml
│   ├── hive-metastore.yaml
│   ├── trino.yaml
│   ├── kafka.yaml
│   ├── nifi.yaml
│   ├── monitoring.yaml
│   ├── vault.yaml
│   ├── vault-secrets-operator.yaml
│   ├── vault-config.yaml
│   ├── superset.yaml
│   └── airflow.yaml
│
├── platform/                      # Component-level manifests
│   ├── namespaces/                # Namespace definitions with ArgoCD labels
│   ├── hive-metastore/            # Deployment + Service + ConfigMap
│   ├── trino/                     # Deployment + Service + Catalog config
│   ├── kafka/                     # Strimzi KafkaNodePool + Kafka + Connect
│   ├── nifi/                      # NiFi 1.27.0 with passthrough route
│   ├── monitoring/                # Grafana Operator + Datasource + ServiceMonitors
│   ├── vault/                     # Vault Helm values + VSO subscription
│   ├── vault-config/              # VaultConnection / VaultAuth / VaultStaticSecrets
│   └── superset/                  # Superset 5.0.0 with Trino driver bootstrap
│
└── README.md
```

**Design principle:** Adding a new platform component = one new file in `apps/` + one new folder in `platform/`. Nothing else.

---

## Deployment

### Prerequisites

- OpenShift 4.x cluster (3 masters + 2+ workers recommended)
- `oc` CLI authenticated as `cluster-admin`
- OpenShift GitOps Operator installed
- Worker nodes with raw block devices for local storage

### Bootstrap

```bash
# Clone the repo on the helper node
git clone https://github.com/bakralakhras/OpenShift-ArgoCD.git
cd OpenShift-ArgoCD

# Set kubeconfig
export KUBECONFIG=/path/to/auth/kubeconfig

# Grant ArgoCD cluster-admin (lab scope — restrict in production)
oc adm policy add-cluster-role-to-user cluster-admin \
  -z openshift-gitops-argocd-application-controller \
  -n openshift-gitops

# THE ONLY MANUAL APPLY IN THE ENTIRE PROJECT
oc apply -f apps/root.yaml
```

That's it. ArgoCD now reconciles every other component in dependency order.

### Verification

```bash
# Watch all ArgoCD apps converge to Synced/Healthy
watch oc get applications -n openshift-gitops

# Verify pods across the platform
oc get pods -A | grep -E 'minio|kafka|trino|nifi|vault|superset|grafana'
```

---

## Platform Phases

The platform was built in nine deliberate phases. Each phase is documented with its own runbook covering goals, manifests, errors encountered, root causes, and fixes.

| Phase | Component | Outcome |
|:---:|---|---|
| **1** | GitOps Foundation | ArgoCD app-of-apps, namespace governance, single bootstrap command |
| **2** | Object Storage (MinIO) | LSO-provisioned local PVs, MinIO with `lakehouse` bucket exposed via OpenShift Route |
| **3** | Lakehouse Core | PostgreSQL + Hive Metastore + Trino querying Iceberg tables on MinIO |
| **4** | Streaming (Kafka) | KRaft Kafka cluster (3 brokers) + Kafka Connect with custom S3 Sink build |
| **5** | Ingestion (NiFi) | NiFi 1.27.0 with passthrough TLS route, single-user auth, NFS-backed state |
| **6** | Observability | User Workload Monitoring + Thanos Querier + Grafana Operator with cross-namespace datasource |
| **7** | Security & Governance | Vault (KV v2) + Vault Secrets Operator with AppRole auth, zero hardcoded credentials |
| **8** | Visualization (Superset) | Superset 5.0.0 connected to Trino, runtime driver injection pattern |
| **9** | Documentation & Portfolio | Architecture diagrams, end-to-end demo pipeline, screenshots |

---

## End-to-End Data Flow

```
External API / Dataset
         │
         ▼
       NiFi  ────────┐
         │           │   (direct path — JDBC writes to Trino)
         ▼           │
      Kafka          │
         │           │
         ▼           │
   Kafka Connect     │
   (S3 Sink)         │
         │           │
         ▼           ▼
      MinIO  ◀────  Trino  ◀────  Hive Metastore  ◀────  PostgreSQL
   (Iceberg files)   (SQL)         (Catalog)              (Backend)
                       ▲
                       │
              ┌────────┴────────┐
              │                 │
          Superset          Airflow
         (Dashboards)    (Orchestration)
```

A producer publishes to a Kafka topic → Kafka Connect S3 Sink writes Parquet files into `s3://lakehouse/kafka/<topic>/` → Hive Metastore registers the path as an Iceberg table → Trino federates queries → Superset visualizes the result. Every hop is declared in Git.

---

## Observability

The platform piggybacks on **OpenShift's built-in monitoring stack** (Prometheus + Thanos Ruler + Thanos Querier) by enabling User Workload Monitoring rather than deploying a parallel Prometheus.

- **Custom Grafana** (Grafana Operator v5) deployed in the `monitoring` namespace
- **Datasource** points at `https://thanos-querier.openshift-monitoring.svc:9091`
- **Authentication** via long-lived ServiceAccount token with `cluster-monitoring-view` + per-namespace `view` roles
- **Component metrics** via:
  - `PodMonitor` for Kafka brokers (JMX exporter on `:9404`)
  - `ServiceMonitor` for Trino (`/v1/info`)
  - `ServiceMonitor` for MinIO (`/minio/v2/metrics/cluster`)

Verified `up{}` queries return live data for `kafka`, `trino`, and `minio` namespaces in Grafana Explore.

---

## Security & Secrets Management

**Zero hardcoded credentials.** Every platform component reads its secrets from a Kubernetes Secret that is materialized by the **Vault Secrets Operator** from a Vault KV v2 path.

```
Vault (KV v2 at secret/platform/*)
        │
        │  AppRole login (role-id + secret-id)
        ▼
Vault Secrets Operator (cluster-scoped)
        │
        │  reconciles every 30s
        ▼
Kubernetes Secrets (synced into target namespaces)
        │
        ▼
Application pods (read via `secretKeyRef`)
```

**Key design decisions:**

- **AppRole over Kubernetes auth** — VSO v1.3.0 dropped token auth; AppRole gives explicit, auditable role-based access.
- **Per-namespace `vault-approle` secrets** — VSO looks for the secret-id in the *target* namespace, not the VaultAuth namespace.
- **Split ArgoCD apps for operator + CRDs** — sync waves don't solve missing CRDs because ArgoCD validates *before* it applies. The fix is two separate apps: one for the Subscription, one for the CRD-dependent resources.

This pattern is documented in detail in `Phase-07 Security Governance.md`.

---

## Engineering Highlights

A non-exhaustive list of real engineering problems encountered and resolved during the build:

- **ArgoCD CRD chicken-and-egg** — split operator subscription and CRD-dependent resources into separate ArgoCD apps because sync waves do not bypass ArgoCD's pre-apply validation step.
- **OpenShift `WaitForFirstConsumer` deadlock** — Helm-installed StorageClass shipped with `WaitForFirstConsumer`, which is incompatible with local storage. `volumeBindingMode` is immutable, so the fix was full StorageClass recreation with `Immediate` binding.
- **Kafka KRaft requires `KafkaNodePool`** — without the CRD the cluster never forms. Added Strimzi annotations `strimzi.io/node-pools: enabled` and `strimzi.io/kraft: enabled`.
- **NiFi 2.x SNI rejection on OpenShift Routes** — Jetty 10 enforces strict SNI validation against an auto-generated keystore with `CN=localhost`. Resolved by downgrading to NiFi 1.27.0 (Jetty 9, no SNI enforcement).
- **Superset 5.0.0 hardened image has no pip in venv** — runtime driver install via `pip install --target=/tmp/pkgs --no-deps` + `PYTHONPATH=/tmp/pkgs` to avoid SQLAlchemy 2.0 conflicting with the venv's compatible version.
- **OpenShift service env injection collision** — OpenShift injects `SUPERSET_PORT=tcp://...` for every service; Superset's `run-server.sh` expected a numeric port. Fix: explicitly override `SUPERSET_PORT=8088` in the deployment env.
- **Grafana Operator v5.22.2 datasource bug** — `NoMatchingInstance` despite correct label selectors. Workaround: `allowCrossNamespaceImport: true`.
- **User Workload Monitoring namespace selector** — Prometheus uses `NotIn` logic on `openshift.io/cluster-monitoring`. Adding the label *excludes* a namespace from scraping. Removed the mislabel.
- **VSO secret caching** — updating a Kubernetes secret with a new AppRole secret-id does not invalidate VSO's internal client cache. Required `oc rollout restart` of the VSO controller manager.

Each error is captured with symptom, root cause, and remediation in the per-phase runbooks.

---

## Routes & Access

| Service | Route |
|---|---|
| ArgoCD | `https://openshift-gitops-server-openshift-gitops.apps.<base-domain>` |
| MinIO Console | `https://minio-console-minio.apps.<base-domain>` |
| Trino UI | `https://trino-trino.apps.<base-domain>` |
| NiFi | `https://nifi-nifi.apps.<base-domain>/nifi` |
| Grafana | `https://grafana-monitoring.apps.<base-domain>` |
| Vault | `https://vault.apps.<base-domain>` |
| Superset | `https://superset-superset.apps.<base-domain>` |

---

## Documentation

Each phase has a dedicated runbook capturing the goal, architecture, manifests, errors encountered, fixes, and key commands. The runbooks are written as cold-start guides — runnable on any fresh OpenShift cluster.

- Phase 1 — GitOps Foundation
- Phase 2 — Storage + MinIO
- Phase 3 — Lakehouse Core (Hive + Trino + PostgreSQL)
- Phase 4 — Streaming (Kafka KRaft + Connect)
- Phase 5 — Ingestion (NiFi)
- Phase 6 — Observability (Prometheus + Thanos + Grafana)
- Phase 7 — Security & Governance (Vault + VSO)
- Phase 8 — Visualization (Superset)
- Phase 9 — Documentation & Portfolio

---

## Author

**Baker Al-Akhras**
Infrastructure & Data Platform Engineer
Amman, Jordan

[LinkedIn](https://www.linkedin.com/in/baker-al-akhras) · [GitHub](https://github.com/bakralakhras)

> Built as a flagship platform engineering project demonstrating end-to-end ownership of an enterprise data platform — from raw OpenShift install through production-grade lakehouse with secrets management, observability, and BI.

---

<div align="center">

**If this project is useful or interesting, consider starring the repo ⭐**

</div>
