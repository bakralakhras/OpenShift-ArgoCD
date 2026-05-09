import os
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


def get_token() -> str:
    with open(PROMETHEUS_TOKEN_FILE, "r") as f:
        return f.read().strip()


async def prom_query(query: str) -> Any:
    token = get_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

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


def value(result: Any, default: float = 0) -> float:
    try:
        return float(result[0]["value"][1])
    except Exception:
        return default


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/overview")
async def overview():
    queries = {
        "nodes_ready": 'sum(kube_node_status_condition{condition="Ready",status="true"})',
        "bad_pods": 'count(kube_pod_status_phase{phase!="Running",phase!="Succeeded"})',
        "targets_up": "sum(up)",
        "cpu": 'avg(1 - rate(node_cpu_seconds_total{mode="idle"}[5m]))',
        "memory": "1 - (sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes))",
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
            - (bad_pods * 0.12)
            - (cpu * 20)
            - (memory * 10),
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
