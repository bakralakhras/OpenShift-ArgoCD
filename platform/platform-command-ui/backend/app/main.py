import os
from datetime import datetime
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


PROMETHEUS_URL = os.getenv(
    "PROMETHEUS_URL",
    "https://thanos-querier.openshift-monitoring.svc.cluster.local:9091",
)

PROMETHEUS_TOKEN_FILE = os.getenv(
    "PROMETHEUS_TOKEN_FILE",
    "/var/run/secrets/kubernetes.io/serviceaccount/token",
)

PROMETHEUS_VERIFY_SSL = (
    os.getenv("PROMETHEUS_VERIFY_SSL", "false").lower() == "true"
)

app = FastAPI(title="Sovereign Platform Command API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


SERVICES = [
    {
        "key": "argocd",
        "name": "ArgoCD",
        "category": "GitOps & Orchestration",
        "namespace": "openshift-gitops",
        "description": "GitOps control plane for platform applications.",
        "href": "https://openshift-gitops-server-openshift-gitops.apps.ocp4.infrahub.com",
    },
    {
        "key": "airflow",
        "name": "Airflow",
        "category": "GitOps & Orchestration",
        "namespace": "airflow",
        "description": "DAG orchestration for Silver and Gold analytics workflows.",
        "href": "http://airflow-api-server-airflow.apps.ocp4.infrahub.com",
    },
    {
        "key": "vault",
        "name": "Vault",
        "category": "Security & Identity",
        "namespace": "vault",
        "description": "Secrets management and platform credential storage.",
        "href": "https://vault.apps.ocp4.infrahub.com",
    },
    {
        "key": "keycloak",
        "name": "Keycloak",
        "category": "Security & Identity",
        "namespace": "keycloak",
        "description": "Identity provider and authentication control plane.",
        "href": "https://keycloak.apps.ocp4.infrahub.com",
    },
    {
        "key": "minio",
        "name": "MinIO Console",
        "category": "Data Platform",
        "namespace": "minio",
        "description": "S3-compatible object storage for Iceberg lakehouse data.",
        "href": "https://minio-console-minio.apps.ocp4.infrahub.com",
    },
    {
        "key": "trino",
        "name": "Trino",
        "category": "Data Platform",
        "namespace": "trino",
        "description": "Distributed SQL query engine over Iceberg tables.",
        "href": "https://trino-trino.apps.ocp4.infrahub.com",
    },
    {
        "key": "superset",
        "name": "Superset",
        "category": "Data Platform",
        "namespace": "superset",
        "description": "Business intelligence and analytical dashboard layer.",
        "href": "https://superset-superset.apps.ocp4.infrahub.com",
    },
    {
        "key": "nifi",
        "name": "NiFi",
        "category": "Data Platform",
        "namespace": "nifi",
        "description": "Ingestion UI retained for platform extensibility.",
        "href": "https://nifi-nifi.apps.ocp4.infrahub.com",
    },
    {
        "key": "grafana",
        "name": "Grafana",
        "category": "Observability",
        "namespace": "monitoring",
        "description": "Platform dashboards and operational visualization.",
        "href": "https://grafana-monitoring.apps.ocp4.infrahub.com",
    },
    {
        "key": "prometheus",
        "name": "Prometheus",
        "category": "Observability",
        "namespace": "openshift-monitoring",
        "description": "Cluster metrics and monitoring backend.",
        "href": "https://prometheus-k8s-openshift-monitoring.apps.ocp4.infrahub.com/api",
    },
    {
        "key": "thanos",
        "name": "Thanos Querier",
        "category": "Observability",
        "namespace": "openshift-monitoring",
        "description": "Long-term and federated metrics query layer.",
        "href": "https://thanos-querier-openshift-monitoring.apps.ocp4.infrahub.com/api",
    },
]


def get_token() -> str:
    with open(PROMETHEUS_TOKEN_FILE, "r") as f:
        return f.read().strip()


async def prom_query_raw(query: str) -> Any:
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(
        timeout=10,
        verify=PROMETHEUS_VERIFY_SSL,
    ) as client:
        response = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            headers=headers,
        )

        response.raise_for_status()
        data = response.json()
        return data["data"]["result"]


async def prom_query(query: str) -> Any:
    return await prom_query_raw(query)


def value(result: Any, default: float = 0) -> float:
    try:
        return float(result[0]["value"][1])
    except Exception:
        return default


def component_status(metric: float, warning: float, critical: float):
    if metric >= critical:
        return "critical"
    if metric >= warning:
        return "warning"
    return "healthy"


async def namespace_health(namespace: str) -> dict:
    running = int(
        value(
            await prom_query(
                f'count(kube_pod_status_phase{{namespace="{namespace}",phase="Running"}} == 1)'
            )
        )
    )

    bad = int(
        value(
            await prom_query(
                f'count(kube_pod_status_phase{{namespace="{namespace}",phase=~"Failed|Pending|Unknown"}} == 1)'
            )
        )
    )

    if running == 0:
        status = "critical"
    elif bad >= 3:
        status = "critical"
    elif bad > 0:
        status = "warning"
    else:
        status = "healthy"

    return {
        "running_pods": running,
        "bad_pods": bad,
        "status": status,
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/services")
async def services():
    output = []

    for service in SERVICES:
        health_data = await namespace_health(service["namespace"])
        output.append({**service, **health_data})

    return output


@app.get("/api/overview")
async def overview():
    queries = {
        "nodes_ready": 'sum(kube_node_status_condition{condition="Ready",status="true"})',
        "bad_pods": 'count(kube_pod_status_phase{phase=~"Failed|Pending|Unknown"} == 1)',
        "targets_up": "sum(up)",
        "cpu": 'avg(1 - rate(node_cpu_seconds_total{mode="idle"}[5m]))',
        "memory": '1 - (sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))',
        "argo_synced": 'sum(argocd_app_info{sync_status="Synced"})',
        "transactions": "fraud_total_transactions_all",
        "fraud_cases": "fraud_total_fraud_count_all",
        "fraud_rate": "fraud_avg_fraud_rate_all",
        "decisions": "fraud_silver_decisions_count",
    }

    results = {}

    for key, query in queries.items():
        results[key] = value(await prom_query(query))

    bad_pods = results["bad_pods"]
    cpu = results["cpu"]
    memory = results["memory"]

    health_score = max(
        0,
        min(
            100,
            100
            - (bad_pods * 0.02)
            - (cpu * 10)
            - (memory * 8),
        ),
    )

    return {
        "platform": {
            "health_score": round(health_score, 1),
            "nodes_ready": int(results["nodes_ready"]),
            "bad_pods": int(results["bad_pods"]),
            "targets_up": int(results["targets_up"]),
            "argo_synced": int(results["argo_synced"]),
            "cpu": round(results["cpu"], 4),
            "memory": round(results["memory"], 4),
        },
        "fraud": {
            "transactions": int(results["transactions"]),
            "fraud_cases": int(results["fraud_cases"]),
            "fraud_rate": round(results["fraud_rate"], 4),
            "decisions": int(results["decisions"]),
        },
    }


@app.get("/api/topology")
async def topology():
    cpu = value(await prom_query('avg(1 - rate(node_cpu_seconds_total{mode="idle"}[5m]))'))
    memory = value(await prom_query('1 - (sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))'))
    bad_pods = value(await prom_query('count(kube_pod_status_phase{phase=~"Failed|Pending|Unknown"} == 1)'))

    return [
        {"title": "Ingress", "subtitle": "OpenShift routes", "status": "healthy"},
        {"title": "Spark Batch", "subtitle": "Bronze → Silver", "status": component_status(bad_pods, 40, 80)},
        {"title": "Kafka", "subtitle": "Streaming backbone", "status": component_status(cpu * 100, 65, 85)},
        {"title": "Spark Engine", "subtitle": "Fraud scoring", "status": component_status(memory * 100, 75, 90)},
        {"title": "MinIO", "subtitle": "Iceberg lake", "status": "healthy"},
        {"title": "Trino", "subtitle": "Analytics SQL", "status": component_status(cpu * 100, 70, 90)},
    ]


@app.get("/api/events")
async def events():
    bad_pods = int(value(await prom_query('count(kube_pod_status_phase{phase=~"Failed|Pending|Unknown"} == 1)')))
    cpu = round(value(await prom_query('avg(1 - rate(node_cpu_seconds_total{mode="idle"}[5m]))')) * 100, 1)
    memory = round(value(await prom_query('1 - (sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))')) * 100, 1)

    now = datetime.utcnow().strftime("%H:%M:%S UTC")

    return [
        {"level": "healthy", "text": "Spark bronze-to-silver pipeline operational", "time": now},
        {"level": "healthy", "text": "Kafka brokers responding normally", "time": now},
        {"level": "healthy", "text": f"Cluster CPU utilization stable at {cpu}%", "time": now},
        {"level": "healthy" if memory < 85 else "critical", "text": f"Cluster memory utilization at {memory}%", "time": now},
        {"level": "critical" if bad_pods > 40 else "healthy", "text": f"{bad_pods} non-running workloads detected", "time": now},
    ]


@app.get("/api/namespaces")
async def namespaces():
    namespace_names = [
        "kafka",
        "spark",
        "lakehouse",
        "minio",
        "trino",
        "airflow",
        "rook-ceph",
        "schema-registry",
        "platform-command-ui",
        "openshift-gitops",
    ]

    output = []

    for namespace in namespace_names:
        health_data = await namespace_health(namespace)
        output.append({"name": namespace, "running": health_data["running_pods"], "bad": health_data["bad_pods"], "status": health_data["status"]})

    return output


@app.get("/api/namespaces/{namespace}/pods")
async def namespace_pods(namespace: str):
    pod_info = await prom_query(f'kube_pod_info{{namespace="{namespace}"}}')
    output = []

    for pod in pod_info:
        labels = pod.get("metric", {})
        pod_name = labels.get("pod", "unknown")
        node = labels.get("node", "unknown")

        phase_result = await prom_query(f'kube_pod_status_phase{{namespace="{namespace}",pod="{pod_name}"}}')
        restart_result = await prom_query(f'sum(kube_pod_container_status_restarts_total{{namespace="{namespace}",pod="{pod_name}"}})')

        phase = "Unknown"

        for phase_item in phase_result:
            try:
                if float(phase_item["value"][1]) == 1:
                    phase = phase_item["metric"].get("phase", "Unknown")
                    break
            except Exception:
                pass

        output.append(
            {
                "name": pod_name,
                "phase": phase,
                "restarts": int(value(restart_result)),
                "node": node,
            }
        )

    return output


@app.get("/api/incidents")
async def incidents():
    bad_pods = int(value(await prom_query('count(kube_pod_status_phase{phase=~"Failed|Pending|Unknown"} == 1)')))
    cpu = round(value(await prom_query('avg(1 - rate(node_cpu_seconds_total{mode="idle"}[5m]))')) * 100, 1)
    memory = round(value(await prom_query('1 - (sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))')) * 100, 1)

    severity = "healthy"
    title = "No active incident detected"
    root_cause = "Platform telemetry is within normal operating range."
    impact = "No immediate user-facing or data-platform impact detected."
    suggestion = "Continue monitoring namespace health and pod restarts."

    if bad_pods > 40:
        severity = "critical"
        title = "Workload instability detected"
        root_cause = f"{bad_pods} pods are currently non-running."
        impact = "Possible impact on Spark jobs, Airflow tasks, or platform services."
        suggestion = "Open Namespace Health Matrix and inspect namespaces with bad pods."
    elif memory >= 85:
        severity = "critical"
        title = "Cluster memory pressure detected"
        root_cause = f"Cluster memory utilization is at {memory}%."
        impact = "Scheduling delays, pod evictions, and Spark executor instability may occur."
        suggestion = "Check high-memory namespaces and worker node pressure."
    elif cpu >= 80:
        severity = "warning"
        title = "High CPU utilization detected"
        root_cause = f"Cluster CPU utilization is at {cpu}%."
        impact = "Batch jobs and query workloads may slow down."
        suggestion = "Check Spark, Trino, and Kafka workload activity."

    return {
        "severity": severity,
        "title": title,
        "root_cause": root_cause,
        "impact": impact,
        "suggestion": suggestion,
        "signals": {
            "bad_pods": bad_pods,
            "cpu": cpu,
            "memory": memory,
        },
    }


@app.get("/api/argocd/apps")
async def argocd_apps():
    results = await prom_query_raw("argocd_app_info")
    apps = []

    for item in results:
        metric = item.get("metric", {})
        health = metric.get("health_status", "Unknown")
        sync = metric.get("sync_status", "Unknown")

        severity = "healthy"

        if health not in ["Healthy"]:
            severity = "critical"
        elif sync != "Synced":
            severity = "warning"

        apps.append(
            {
                "name": metric.get("name"),
                "namespace": metric.get("dest_namespace"),
                "project": metric.get("project"),
                "repo": metric.get("repo"),
                "sync_status": sync,
                "health_status": health,
                "autosync": metric.get("autosync_enabled"),
                "severity": severity,
            }
        )

    severity_order = {"critical": 0, "warning": 1, "healthy": 2}
    apps.sort(key=lambda x: (severity_order.get(x["severity"], 99), x["name"]))

    return apps
