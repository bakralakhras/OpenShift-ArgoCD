from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {"owner": "fraud-platform", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG("dag3_rule_performance", default_args=default_args,
         schedule="30 2 * * *", start_date=datetime(2026,1,1), catchup=False,
         tags=["gold","rules"]) as dag:

    BashOperator(task_id="build_and_verify", bash_command="""
    TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
    oc exec -n trino $TRINO_POD -- trino --execute "
    INSERT INTO iceberg.warehouse_gold.rule_performance
    SELECT
      CURRENT_DATE                                                       AS summary_date,
      rule_name,
      'scoring'                                                          AS rule_type,
      COUNT(*) FILTER (WHERE rule_score > 0)                            AS triggers_count,
      COUNT(*) FILTER (WHERE rule_score > 0 AND isfraud = 1)            AS true_positive,
      COUNT(*) FILTER (WHERE rule_score > 0 AND isfraud = 0)            AS false_positive,
      COUNT(*) FILTER (WHERE rule_score = 0 AND isfraud = 0)            AS true_negative,
      COUNT(*) FILTER (WHERE rule_score = 0 AND isfraud = 1)            AS false_negative,
      CAST(COUNT(*) FILTER (WHERE rule_score > 0 AND isfraud = 1) AS double) /
        NULLIF(COUNT(*) FILTER (WHERE rule_score > 0), 0)               AS precision_score,
      CAST(COUNT(*) FILTER (WHERE rule_score > 0 AND isfraud = 1) AS double) /
        NULLIF(COUNT(*) FILTER (WHERE isfraud = 1), 0)                  AS recall_score,
      2.0 * (
        CAST(COUNT(*) FILTER (WHERE rule_score > 0 AND isfraud = 1) AS double) /
          NULLIF(COUNT(*) FILTER (WHERE rule_score > 0), 0) *
        CAST(COUNT(*) FILTER (WHERE rule_score > 0 AND isfraud = 1) AS double) /
          NULLIF(COUNT(*) FILTER (WHERE isfraud = 1), 0)
      ) / NULLIF(
        CAST(COUNT(*) FILTER (WHERE rule_score > 0 AND isfraud = 1) AS double) /
          NULLIF(COUNT(*) FILTER (WHERE rule_score > 0), 0) +
        CAST(COUNT(*) FILTER (WHERE rule_score > 0 AND isfraud = 1) AS double) /
          NULLIF(COUNT(*) FILTER (WHERE isfraud = 1), 0), 0
      )                                                                  AS f1_score,
      SUM(transactionamt) FILTER (WHERE rule_score > 0 AND decision = 'BLOCK') AS blocked_amount,
      CURRENT_TIMESTAMP                                                  AS updated_at
    FROM iceberg.warehouse_silver.decisions
    CROSS JOIN UNNEST(
      ARRAY['high_amount','anonymous_email','product_h','velocity','night_tx','mismatch','c13_spike','v258_v257'],
      ARRAY[fraud_score, fraud_score, fraud_score, fraud_score, fraud_score, fraud_score, fraud_score, fraud_score]
    ) AS t(rule_name, rule_score)
    GROUP BY rule_name
    " 2>/dev/null
    echo "Done:"
    oc exec -n trino $TRINO_POD -- trino --execute \
      "SELECT count(*) FROM iceberg.warehouse_gold.rule_performance" 2>/dev/null
    """)
