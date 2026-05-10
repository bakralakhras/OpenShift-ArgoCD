#!/usr/bin/env bash
set -euo pipefail

OUT="docs/validation/platform-validation-$(date +%Y%m%d-%H%M%S).txt"
exec > >(tee "$OUT") 2>&1

echo "SOVEREIGN DATA LAKEHOUSE PLATFORM VALIDATION"
echo "Generated: $(date)"
echo "Repo: $(pwd)"
echo ""

echo "=== CLUSTER ==="
oc version
oc get nodes -o wide

echo ""
echo "=== ARGOCD ==="
oc get applications -n openshift-gitops || true
echo "App count:"
oc get applications -n openshift-gitops --no-headers 2>/dev/null | wc -l || true

echo ""
echo "=== KAFKA ==="
oc get kafka -n kafka || true
oc get kafkatopics -n kafka || true

echo ""
echo "=== AIRFLOW ==="
oc get pods -n airflow -o wide || true
oc exec -n airflow sts/airflow-scheduler -- bash -lc 'ls -lah /opt/airflow/dags || true' || true

echo ""
echo "=== TRINO COUNTS ==="
TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
echo "TRINO_POD=$TRINO_POD"

if [ -n "$TRINO_POD" ]; then
  oc exec -n trino "$TRINO_POD" -- trino --execute "SHOW SCHEMAS FROM iceberg" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) FROM iceberg.warehouse_bronze.transactions" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) FROM iceberg.warehouse_silver.decisions" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) FROM iceberg.warehouse_silver.transactions" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) FROM iceberg.warehouse_gold.daily_fraud_summary" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) FROM iceberg.warehouse_gold.uid_risk_profile" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) FROM iceberg.warehouse_gold.hourly_fraud_pattern" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) FROM iceberg.warehouse_gold.rule_performance" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "DESCRIBE iceberg.warehouse_gold.daily_fraud_summary" || true
  oc exec -n trino "$TRINO_POD" -- trino --execute "DESCRIBE iceberg.warehouse_silver.decisions" || true
fi

echo ""
echo "=== OBSERVABILITY ==="
oc get servicemonitor -A | egrep 'fraud|kafka|trino|airflow|grafana|platform|NAME' || true
oc get prometheusrule -A | egrep 'fraud|platform|NAME' || true

echo ""
echo "=== COMMAND CENTER ==="
oc get pods,deploy,svc,route -n platform-command-ui || true
oc logs -n platform-command-ui deploy/command-api --tail=80 || true

echo ""
echo "=== VAULT ==="
oc get pods,svc,route -n vault || true
oc get vaultconnections,vaultauths,vaultstaticsecrets -A || true

echo ""
echo "=== REPO CLAIM CHECK ==="
find apps -maxdepth 1 -type f \( -name '*.yaml' -o -name '*.yml' \) | sort
find fraud-platform/dags -type f -name '*.py' 2>/dev/null | sort || true
find fraud-platform/schemas -type f 2>/dev/null | sort || true

echo ""
echo "Saved to: $OUT"
