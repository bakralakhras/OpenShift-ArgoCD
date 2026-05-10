#!/usr/bin/env bash
set -euo pipefail

OUT="docs/validation/platform-validation-$(date +%Y%m%d-%H%M%S).txt"

exec > >(tee "$OUT") 2>&1

echo "============================================================"
echo "SOVEREIGN DATA LAKEHOUSE PLATFORM VALIDATION"
echo "Generated: $(date)"
echo "User: $(whoami)"
echo "Repo: $(pwd)"
echo "============================================================"
echo ""

echo "===================="
echo "1. CLUSTER VERSION"
echo "===================="
oc version
echo ""

echo "===================="
echo "2. NODES"
echo "===================="
oc get nodes -o wide
echo ""

echo "Node count:"
oc get nodes --no-headers | wc -l
echo ""

echo "Schedulable masters:"
oc get nodes -l node-role.kubernetes.io/master --no-headers | grep -v SchedulingDisabled || true
echo ""

echo "Workers:"
oc get nodes -l node-role.kubernetes.io/worker --no-headers || true
echo ""

echo "===================="
echo "3. ARGOCD APPLICATIONS"
echo "===================="
oc get applications -n openshift-gitops
echo ""

echo "ArgoCD app count:"
oc get applications -n openshift-gitops --no-headers | wc -l
echo ""

echo "Apps not Synced/Healthy:"
oc get applications -n openshift-gitops --no-headers | awk '$2!="Synced" || $3!="Healthy" {print}' || true
echo ""

echo "===================="
echo "4. KEY NAMESPACES"
echo "===================="
for ns in kafka spark minio lakehouse trino airflow monitoring superset vault schema-registry platform-command-ui postgresql rook-ceph; do
  echo "--- namespace: $ns"
  oc get ns "$ns" 2>/dev/null || true
done
echo ""

echo "===================="
echo "5. POD HEALTH BY PLATFORM NAMESPACE"
echo "===================="
for ns in kafka spark minio lakehouse trino airflow monitoring superset vault schema-registry platform-command-ui postgresql rook-ceph; do
  echo ""
  echo "----- $ns -----"
  oc get pods -n "$ns" -o wide 2>/dev/null || true
done
echo ""

echo "===================="
echo "6. BAD PODS ACROSS CLUSTER"
echo "===================="
oc get pods -A --no-headers | awk '$4!="Running" && $4!="Completed" {print}' || true
echo ""

echo "===================="
echo "7. KAFKA RESOURCES"
echo "===================="
oc get kafka -n kafka 2>/dev/null || true
echo ""
oc get kafkatopics -n kafka 2>/dev/null || true
echo ""

echo "Kafka topic count:"
oc get kafkatopics -n kafka --no-headers 2>/dev/null | wc -l || true
echo ""

echo "===================="
echo "8. SPARK RESOURCES"
echo "===================="
oc get sparkapplications -A 2>/dev/null || true
echo ""
oc get pods -n spark -o wide 2>/dev/null || true
echo ""

echo "===================="
echo "9. AIRFLOW"
echo "===================="
oc get pods -n airflow -o wide 2>/dev/null || true
echo ""
oc get sts,deploy,svc,route -n airflow 2>/dev/null || true
echo ""

echo "Airflow DAG files if accessible:"
oc exec -n airflow sts/airflow-scheduler -- bash -lc 'ls -lah /opt/airflow/dags || true' 2>/dev/null || true
echo ""

echo "===================="
echo "10. TRINO"
echo "===================="
oc get pods,svc,route -n trino 2>/dev/null || true
echo ""

TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
echo "TRINO_POD=$TRINO_POD"
echo ""

if [ -n "$TRINO_POD" ]; then
  echo "Trino catalogs:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SHOW CATALOGS" || true
  echo ""

  echo "Trino schemas in iceberg:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SHOW SCHEMAS FROM iceberg" || true
  echo ""

  echo "Bronze count:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) AS bronze_transactions FROM iceberg.warehouse_bronze.transactions" || true
  echo ""

  echo "Silver decisions count:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) AS silver_decisions FROM iceberg.warehouse_silver.decisions" || true
  echo ""

  echo "Silver transactions count:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) AS silver_transactions FROM iceberg.warehouse_silver.transactions" || true
  echo ""

  echo "Gold daily summary count:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) AS daily_summary_rows FROM iceberg.warehouse_gold.daily_fraud_summary" || true
  echo ""

  echo "Gold UID risk profile count:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) AS uid_risk_rows FROM iceberg.warehouse_gold.uid_risk_profile" || true
  echo ""

  echo "Gold hourly pattern count:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) AS hourly_pattern_rows FROM iceberg.warehouse_gold.hourly_fraud_pattern" || true
  echo ""

  echo "Gold rule performance count:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "SELECT count(*) AS rule_performance_rows FROM iceberg.warehouse_gold.rule_performance" || true
  echo ""

  echo "Fraud summary numbers:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "
    SELECT
      sum(transaction_count) AS total_transactions,
      sum(fraud_count) AS total_fraud,
      round(100.0 * sum(fraud_count) / nullif(sum(transaction_count), 0), 2) AS fraud_rate_pct
    FROM iceberg.warehouse_gold.daily_fraud_summary
  " || true
  echo ""

  echo "Latest daily summaries:"
  oc exec -n trino "$TRINO_POD" -- trino --execute "
    SELECT *
    FROM iceberg.warehouse_gold.daily_fraud_summary
    ORDER BY summary_date DESC
    LIMIT 5
  " || true
fi
echo ""

echo "===================="
echo "11. MINIO / STORAGE"
echo "===================="
oc get pods,svc,route,pvc -n minio 2>/dev/null || true
echo ""
oc get pvc -A | egrep 'minio|airflow|postgres|trino|grafana|rook|ceph|superset|kafka' || true
echo ""
oc get sc 2>/dev/null || true
echo ""

echo "===================="
echo "12. HIVE METASTORE"
echo "===================="
oc get pods,svc,deploy,endpoints -n lakehouse 2>/dev/null || true
echo ""

echo "===================="
echo "13. SCHEMA REGISTRY"
echo "===================="
oc get pods,svc,route -n schema-registry 2>/dev/null || true
echo ""

echo "===================="
echo "14. VAULT / SECRETS OPERATOR"
echo "===================="
oc get pods,svc,route -n vault 2>/dev/null || true
echo ""
oc get vaultconnections,vaultauths,vaultstaticsecrets -A 2>/dev/null || true
echo ""

echo "===================="
echo "15. MONITORING"
echo "===================="
oc get pods,svc,route -n monitoring 2>/dev/null || true
echo ""
oc get servicemonitor -A 2>/dev/null | egrep 'fraud|kafka|trino|airflow|grafana|platform|NAME' || true
echo ""
oc get prometheusrule -A 2>/dev/null | egrep 'fraud|platform|NAME' || true
echo ""

echo "Fraud alert rule file in repo:"
grep -R "FraudRateSpikeDetected\|FraudPipelineDLQNonZero\|KafkaBrokerDown\|SparkExecutorOOM\|HiveMetastoreDown\|TrinoDown" -n platform/monitoring fraud-platform 2>/dev/null || true
echo ""

echo "===================="
echo "16. FRAUD METRICS EXPORTER"
echo "===================="
oc get deploy,pods,svc,servicemonitor -n kafka | egrep 'fraud|NAME' || true
echo ""

echo "Exporter metrics:"
oc exec -n kafka deploy/fraud-metrics-exporter -- curl -s http://127.0.0.1:9090/metrics | egrep 'fraud_total|fraud_avg|fraud_silver|fraud_product|fraud_uid|fraud_rule' | head -80 || true
echo ""

echo "===================="
echo "17. GRAFANA / SUPERSET"
echo "===================="
oc get pods,svc,route -n monitoring 2>/dev/null | egrep 'grafana|NAME' || true
echo ""
oc get pods,svc,route -n superset 2>/dev/null || true
echo ""

echo "===================="
echo "18. PLATFORM COMMAND UI"
echo "===================="
oc get pods,deploy,svc,route,sa,rolebinding,clusterrolebinding -n platform-command-ui 2>/dev/null || true
echo ""

echo "Command API logs tail:"
oc logs -n platform-command-ui deploy/command-api --tail=80 2>/dev/null || true
echo ""

echo "Command UI API health endpoints:"
oc exec -n platform-command-ui deploy/command-api -- python - <<'PY' 2>/dev/null || true
import httpx
for path in ["/health", "/api/overview", "/api/infrastructure", "/api/gitops", "/api/data-platform", "/api/incidents"]:
    try:
        r = httpx.get("http://127.0.0.1:8080" + path, timeout=8)
        print(path, r.status_code, r.text[:300].replace("\n", " "))
    except Exception as e:
        print(path, "ERR", repr(e))
PY
echo ""

echo "===================="
echo "19. REPO FILES CLAIM CHECK"
echo "===================="
echo "apps count:"
find apps -maxdepth 1 -type f \( -name '*.yaml' -o -name '*.yml' \) | sort | wc -l
echo ""

echo "apps files:"
find apps -maxdepth 1 -type f \( -name '*.yaml' -o -name '*.yml' \) | sort
echo ""

echo "fraud schemas:"
find fraud-platform/schemas -type f 2>/dev/null | sort || true
echo ""

echo "spark jobs:"
find fraud-platform/spark-jobs -type f \( -name '*.py' -o -name 'Dockerfile' -o -name '*.yaml' \) 2>/dev/null | sort || true
echo ""

echo "dags:"
find fraud-platform/dags -type f -name '*.py' 2>/dev/null | sort || true
echo ""

echo "platform command ui files:"
find platform/platform-command-ui -maxdepth 4 -type f 2>/dev/null | sort || true
echo ""

echo "docs files:"
find docs -maxdepth 3 -type f | sort || true
echo ""

echo "============================================================"
echo "VALIDATION COMPLETE"
echo "Saved to: $OUT"
echo "============================================================"
