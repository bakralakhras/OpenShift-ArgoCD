# Platform Command Center UI

## Overview

The Platform Command Center is a custom operations console built with React and FastAPI.

It was created because Grafana alone was not enough as the main operational surface for the platform.

---

## Screenshot

![Platform Command Center Overview](screenshots/sovereign-overview.png)

---

## Why It Exists

Grafana is good for:

- dashboards
- metric panels
- Prometheus queries
- trend visualization

But it was limited for:

- platform topology
- GitOps status
- namespace health
- incident cards
- mixed infrastructure and data signals
- operator-focused navigation

The Command Center was built to fill that gap.

---

## Architecture

| Part | Technology |
|---|---|
| Frontend | React + Vite |
| Backend | FastAPI |
| Runtime | OpenShift |
| Deployment | ArgoCD |
| Namespace | `platform-command-ui` |
| Data sources | Thanos/Prometheus, Kubernetes APIs, ArgoCD-related state |

Validated deployment state:

- `command-api` pod running
- `command-ui` pod running
- frontend route present
- API logs serving live traffic

---

## Views

| View | Purpose |
|---|---|
| Overview | High-level platform health and incidents |
| Infrastructure | Topology and namespace health |
| GitOps | ArgoCD application status |
| Data Platform | Fraud KPIs and pipeline stage visualization |
| Incidents | Operational issue detection |

---

## Validated API Activity

Backend logs showed successful requests for:

```text
/api/overview
/api/namespaces
/api/argocd/apps
/api/topology
/api/events
/api/incidents
```

That confirms the UI was not a static mockup. It was actively calling the backend and receiving live responses.

---

## Engineering Value

This part of the project matters because it moves beyond “I installed Grafana.”

It shows:

- product thinking
- API design
- frontend implementation
- platform signal aggregation
- OpenShift deployment
- RBAC integration
- GitOps-managed custom software
