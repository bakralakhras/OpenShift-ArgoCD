"""
DAG 5 — Hourly Fraud Pattern
Intraday fraud pattern — IEEE-CIS finding: fraud peaks 0-5am.
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
    dag_id="dag5_hourly_fraud_pattern",
    default_args=default_args,
    description="Build hourly fraud pattern gold table",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "hourly", "pattern"],
) as dag:

    build_hourly = BashOperator(
        task_id="build_hourly_fraud_pattern",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        oc exec -n trino $TRINO_POD -- trino --execute "
        INSERT INTO iceberg.warehouse_gold.hourly_fraud_pattern
        SELECT
          HOUR(transaction_timestamp)                     AS hour_of_day,
          COUNT(*)                                        AS total_transactions,
          COUNT(*) FILTER (WHERE is_fraud = true)         AS fraud_count,
          CAST(
            COUNT(*) FILTER (WHERE is_fraud = true) AS double
          ) / NULLIF(COUNT(*), 0)                         AS fraud_rate,
          AVG(fraud_score)                                AS avg_fraud_score,
          AVG(transaction_amount)                         AS avg_amount,
          CURRENT_DATE                                    AS snapshot_date
        FROM iceberg.warehouse_silver.decisions
        WHERE CAST(transaction_date AS date) = CURRENT_DATE - INTERVAL '1' DAY
        GROUP BY HOUR(transaction_timestamp)
        " 2>/dev/null
        echo "Hourly pattern insert done"
        """,
    )

    verify = BashOperator(
        task_id="verify_hourly_pattern",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT hour_of_day, fraud_rate, avg_fraud_score
           FROM iceberg.warehouse_gold.hourly_fraud_pattern
           ORDER BY fraud_rate DESC LIMIT 10" 2>/dev/null
        """,
    )

    build_hourly >> verify
