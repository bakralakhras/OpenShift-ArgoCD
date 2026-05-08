"""
DAG 2 — Daily Fraud Summary
Reads silver.decisions via Trino and writes aggregated daily metrics to gold.
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
    dag_id="dag2_daily_fraud_summary",
    default_args=default_args,
    description="Build daily fraud summary gold table from silver decisions",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "daily", "trino"],
) as dag:

    check_silver = BashOperator(
        task_id="check_silver_has_data",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        COUNT=$(oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT count(*) FROM iceberg.warehouse_silver.decisions" 2>/dev/null | \
          grep -o '[0-9]*' | tail -1)
        echo "silver.decisions row count: $COUNT"
        if [ "$COUNT" = "0" ] || [ -z "$COUNT" ]; then
          echo "ERROR: silver.decisions is empty. Run dag1_silver_writer first."
          exit 1
        fi
        """,
    )

    build_daily_summary = BashOperator(
        task_id="build_daily_fraud_summary",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        oc exec -n trino $TRINO_POD -- trino --execute "
        INSERT INTO iceberg.warehouse_gold.daily_fraud_summary
        SELECT
          CAST(transaction_date AS date)           AS transaction_date,
          COUNT(*)                                  AS total_transactions,
          SUM(transaction_amount)                   AS total_amount,
          COUNT(*) FILTER (WHERE decision = 'APPROVE') AS approved_count,
          COUNT(*) FILTER (WHERE decision = 'REVIEW')  AS review_count,
          COUNT(*) FILTER (WHERE decision = 'BLOCK')   AS blocked_count,
          COUNT(*) FILTER (WHERE is_fraud = true)      AS fraud_detected,
          CAST(
            COUNT(*) FILTER (WHERE decision = 'BLOCK' AND is_fraud = false) AS double
          ) / NULLIF(COUNT(*) FILTER (WHERE decision = 'BLOCK'), 0)
            AS false_positive_rate,
          CAST(
            COUNT(*) FILTER (WHERE decision = 'BLOCK' AND is_fraud = true) AS double
          ) / NULLIF(COUNT(*) FILTER (WHERE is_fraud = true), 0)
            AS true_positive_rate,
          AVG(fraud_score)                          AS avg_fraud_score,
          AVG(transaction_amount)                   AS avg_transaction_amount,
          CURRENT_TIMESTAMP                         AS created_at
        FROM iceberg.warehouse_silver.decisions
        WHERE CAST(transaction_date AS date) = CURRENT_DATE - INTERVAL '1' DAY
        GROUP BY CAST(transaction_date AS date)
        " 2>/dev/null
        echo "Daily summary insert done"
        """,
    )

    verify_gold = BashOperator(
        task_id="verify_gold_daily_summary",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        echo "gold.daily_fraud_summary row count:"
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT count(*) FROM iceberg.warehouse_gold.daily_fraud_summary" 2>/dev/null
        echo "Latest rows:"
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT transaction_date, total_transactions, blocked_count, fraud_detected
           FROM iceberg.warehouse_gold.daily_fraud_summary
           ORDER BY transaction_date DESC LIMIT 5" 2>/dev/null
        """,
    )

    check_silver >> build_daily_summary >> verify_gold
