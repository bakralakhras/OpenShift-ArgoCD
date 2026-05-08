"""
DAG 3 — Rule Performance
Evaluates fraud rule precision/recall against ground truth isFraud labels.
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
    dag_id="dag3_rule_performance",
    default_args=default_args,
    description="Evaluate fraud rule performance against ground truth",
    schedule="30 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "rules", "trino"],
) as dag:

    build_rule_performance = BashOperator(
        task_id="build_rule_performance",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        oc exec -n trino $TRINO_POD -- trino --execute "
        INSERT INTO iceberg.warehouse_gold.rule_performance
        SELECT
          rule_name,
          COUNT(*) FILTER (WHERE rule_score > 0)                        AS fires_count,
          COUNT(*) FILTER (WHERE rule_score > 0 AND is_fraud = true)    AS true_positive_count,
          COUNT(*) FILTER (WHERE rule_score > 0 AND is_fraud = false)   AS false_positive_count,
          CAST(
            COUNT(*) FILTER (WHERE rule_score > 0 AND is_fraud = true) AS double
          ) / NULLIF(COUNT(*) FILTER (WHERE rule_score > 0), 0)        AS precision,
          CURRENT_DATE                                                   AS evaluation_date
        FROM iceberg.warehouse_silver.decisions
        CROSS JOIN UNNEST(
          ARRAY['night_transaction','high_amount','velocity_breach',
                'anonymous_email','high_risk_product','card_mismatch',
                'cent_pattern','c13_spike'],
          ARRAY[night_score, amount_score, velocity_score,
                email_score, product_score, mismatch_score,
                cent_score, c13_score]
        ) AS t(rule_name, rule_score)
        WHERE CAST(transaction_date AS date) = CURRENT_DATE - INTERVAL '1' DAY
        GROUP BY rule_name
        " 2>/dev/null
        echo "Rule performance insert done"
        """,
    )

    verify = BashOperator(
        task_id="verify_rule_performance",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT rule_name, fires_count, precision FROM iceberg.warehouse_gold.rule_performance
           ORDER BY fires_count DESC" 2>/dev/null
        """,
    )

    build_rule_performance >> verify
