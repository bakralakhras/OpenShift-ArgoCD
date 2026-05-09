from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {"owner": "fraud-platform", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG("dag2_daily_fraud_summary", default_args=default_args,
         schedule="0 2 * * *", start_date=datetime(2026,1,1), catchup=False,
         tags=["gold","daily"]) as dag:

    BashOperator(task_id="build_and_verify", bash_command="""
    TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
    oc exec -n trino $TRINO_POD -- trino --execute "
    INSERT INTO iceberg.warehouse_gold.daily_fraud_summary
    SELECT
      CAST(transaction_date AS date)                                    AS summary_date,
      COUNT(*)                                                          AS total_transactions,
      SUM(transactionamt)                                               AS total_amount,
      COUNT(*) FILTER (WHERE isfraud = 1)                              AS fraud_count,
      SUM(transactionamt) FILTER (WHERE isfraud = 1)                   AS fraud_amount,
      CAST(COUNT(*) FILTER (WHERE isfraud = 1) AS double) / COUNT(*)   AS fraud_rate,
      AVG(transactionamt) FILTER (WHERE isfraud = 1)                   AS avg_fraud_amount,
      MAX(transactionamt) FILTER (WHERE isfraud = 1)                   AS max_fraud_amount,
      COUNT(*) FILTER (WHERE decision = 'BLOCK')                       AS blocked_count,
      SUM(transactionamt) FILTER (WHERE decision = 'BLOCK')            AS blocked_amount,
      CURRENT_TIMESTAMP                                                 AS updated_at
    FROM iceberg.warehouse_silver.decisions
    WHERE transaction_date IS NOT NULL
    GROUP BY CAST(transaction_date AS date)
    " 2>/dev/null
    echo "Done. Row count:"
    oc exec -n trino $TRINO_POD -- trino --execute \
      "SELECT count(*) FROM iceberg.warehouse_gold.daily_fraud_summary" 2>/dev/null
    """)
