"""
DAG 4 — UID Risk Profile
Builds per-user rolling risk profiles for future scoring enrichment.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "fraud-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag4_uid_risk_profile",
    default_args=default_args,
    description="Build UID risk profiles from silver decisions",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "uid", "risk"],
) as dag:

    build_uid_profiles = BashOperator(
        task_id="build_uid_risk_profiles",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        oc exec -n trino $TRINO_POD -- trino --execute "
        INSERT INTO iceberg.warehouse_gold.uid_risk_profile
        SELECT
          uid,
          COUNT(*)                                        AS total_transactions,
          COUNT(*) FILTER (WHERE decision = 'BLOCK')     AS blocked_count,
          AVG(fraud_score)                                AS avg_fraud_score,
          MAX(fraud_score)                                AS max_fraud_score,
          MAX(CAST(transaction_date AS date))             AS last_seen_date,
          CASE
            WHEN AVG(fraud_score) >= 70 THEN 'HIGH'
            WHEN AVG(fraud_score) >= 35 THEN 'MEDIUM'
            ELSE 'LOW'
          END                                             AS risk_tier,
          CURRENT_TIMESTAMP                               AS updated_at
        FROM iceberg.warehouse_silver.decisions
        WHERE CAST(transaction_date AS date) >= CURRENT_DATE - INTERVAL '30' DAY
        GROUP BY uid
        " 2>/dev/null
        echo "UID risk profile insert done"
        """,
    )

    verify = BashOperator(
        task_id="verify_uid_profiles",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT risk_tier, COUNT(*) as uid_count, AVG(avg_fraud_score) as avg_score
           FROM iceberg.warehouse_gold.uid_risk_profile
           GROUP BY risk_tier ORDER BY avg_score DESC" 2>/dev/null
        """,
    )

    build_uid_profiles >> verify
