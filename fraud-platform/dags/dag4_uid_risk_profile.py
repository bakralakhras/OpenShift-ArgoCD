from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {"owner": "fraud-platform", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG("dag4_uid_risk_profile", default_args=default_args,
         schedule="0 3 * * *", start_date=datetime(2026,1,1), catchup=False,
         tags=["gold","uid"]) as dag:

    BashOperator(task_id="build_and_verify", bash_command="""
    TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
    oc exec -n trino $TRINO_POD -- trino --execute "
    INSERT INTO iceberg.warehouse_gold.uid_risk_profile
    SELECT
      uid,
      NULL                                                               AS card1,
      NULL                                                               AS addr1,
      COUNT(*)                                                           AS total_transactions,
      SUM(transactionamt)                                                AS total_amount,
      COUNT(*) FILTER (WHERE isfraud = 1)                               AS fraud_count,
      CAST(COUNT(*) FILTER (WHERE isfraud = 1) AS double) / COUNT(*)    AS fraud_rate,
      AVG(transactionamt)                                                AS avg_amount,
      MAX(transactionamt)                                                AS max_amount,
      COUNT(DISTINCT productcd)                                          AS distinct_merchants,
      COUNT(DISTINCT p_emaildomain)                                      AS distinct_emails,
      MAX(CAST(CURRENT_TIMESTAMP AS timestamp(6) with time zone))        AS last_seen,
      CASE
        WHEN AVG(fraud_score) >= 65 THEN 'HIGH'
        WHEN AVG(fraud_score) >= 35 THEN 'MEDIUM'
        ELSE 'LOW'
      END                                                                AS risk_tier,
      CURRENT_TIMESTAMP                                                  AS updated_at
    FROM iceberg.warehouse_silver.decisions
    WHERE uid IS NOT NULL
    GROUP BY uid
    " 2>/dev/null
    echo "Done:"
    oc exec -n trino $TRINO_POD -- trino --execute \
      "SELECT risk_tier, count(*) FROM iceberg.warehouse_gold.uid_risk_profile GROUP BY risk_tier" 2>/dev/null
    """)
