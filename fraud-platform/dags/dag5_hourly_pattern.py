from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {"owner": "fraud-platform", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG("dag5_hourly_fraud_pattern", default_args=default_args,
         schedule="0 * * * *", start_date=datetime(2026,1,1), catchup=False,
         tags=["gold","hourly"]) as dag:

    BashOperator(task_id="build_and_verify", bash_command="""
    TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
    oc exec -n trino $TRINO_POD -- trino --execute "
    INSERT INTO iceberg.warehouse_gold.hourly_fraud_pattern
    SELECT
      CURRENT_DATE                                                       AS summary_date,
      transaction_hour,
      NULL                                                               AS transaction_dow,
      COUNT(*)                                                           AS total_transactions,
      COUNT(*) FILTER (WHERE isfraud = 1)                               AS fraud_count,
      CAST(COUNT(*) FILTER (WHERE isfraud = 1) AS double) / COUNT(*)    AS fraud_rate,
      AVG(transactionamt)                                                AS avg_amount,
      CASE
        WHEN transaction_hour BETWEEN 0 AND 5 THEN 'HIGH'
        WHEN transaction_hour BETWEEN 6 AND 9 THEN 'MEDIUM'
        WHEN transaction_hour BETWEEN 22 AND 23 THEN 'MEDIUM'
        ELSE 'LOW'
      END                                                                AS risk_level,
      CURRENT_TIMESTAMP                                                  AS updated_at
    FROM iceberg.warehouse_silver.decisions
    WHERE transaction_hour IS NOT NULL
    GROUP BY transaction_hour
    " 2>/dev/null
    echo "Done:"
    oc exec -n trino $TRINO_POD -- trino --execute \
      "SELECT count(*) FROM iceberg.warehouse_gold.hourly_fraud_pattern" 2>/dev/null
    """)
