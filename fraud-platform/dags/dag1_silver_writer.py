"""
DAG 1 — Silver Writer
Reads fraud-decisions and audit-trail Kafka topics (Avro, Confluent wire format)
and writes to Iceberg silver tables via Spark batch job.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "fraud-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK_IMAGE = "image-registry.openshift-image-registry.svc:5000/spark/fraud-spark-phase3:latest"
MINIO_ENDPOINT = "http://minio.minio.svc.cluster.local:9000"
SCHEMA_REGISTRY = "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"
KAFKA_BOOTSTRAP = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"

SILVER_WRITER_JOB = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: silver-writer-{run_id}
  namespace: spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: {image}
  imagePullPolicy: Always
  mainApplicationFile: local:///opt/spark/jobs/phase4/dag1_silver_writer.py
  sparkVersion: "3.5.0"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "1g"
    serviceAccount: spark
    labels:
      version: "3.5.0"
  executor:
    cores: 1
    coreLimit: "1200m"
    memory: "1g"
    instances: 2
    labels:
      version: "3.5.0"
  sparkConf:
    spark.hadoop.fs.s3a.endpoint: "{minio}"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.hadoop.fs.s3a.access.key: "admin"
    spark.hadoop.fs.s3a.secret.key: "minio1234"
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
    spark.hadoop.fs.s3a.aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version: "2"
    spark.sql.catalog.hadoop_cat: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.hadoop_cat.type: "hadoop"
    spark.sql.catalog.hadoop_cat.warehouse: "s3a://warehouse/iceberg"
    spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.kubernetes.executor.request.cores: "500m"
""".strip()

with DAG(
    dag_id="dag1_silver_writer",
    default_args=default_args,
    description="Backfill silver tables from Kafka fraud-decisions and audit-trail",
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "backfill", "kafka"],
) as dag:

    check_kafka = BashOperator(
        task_id="check_kafka_topics",
        bash_command="""
        oc exec -n kafka lakehouse-kafka-kafka-nodepool-0 -- \
          bin/kafka-log-dirs.sh \
          --bootstrap-server localhost:9092 \
          --topic-list fraud-decisions,audit-trail \
          --describe 2>/dev/null | grep -o '"size":[0-9]*' | head -4
        echo "Kafka check done"
        """,
    )

    check_silver_before = BashOperator(
        task_id="check_silver_row_counts_before",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        echo "silver.decisions before:"
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT count(*) FROM iceberg.warehouse_silver.decisions" 2>/dev/null
        echo "silver.transactions before:"
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT count(*) FROM iceberg.warehouse_silver.transactions" 2>/dev/null
        """,
    )

    submit_silver_writer = BashOperator(
        task_id="submit_silver_writer_spark_job",
        bash_command="""
        RUN_ID=$(date +%s)
        cat <<YAML | oc apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: silver-writer-${RUN_ID}
  namespace: spark
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: image-registry.openshift-image-registry.svc:5000/spark/fraud-spark-phase3:latest
  imagePullPolicy: Always
  mainApplicationFile: local:///opt/spark/jobs/phase4/dag1_silver_writer.py
  sparkVersion: "3.5.0"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "1g"
    serviceAccount: spark
    labels:
      version: "3.5.0"
  executor:
    cores: 1
    coreLimit: "1200m"
    memory: "1g"
    instances: 2
    labels:
      version: "3.5.0"
  sparkConf:
    spark.hadoop.fs.s3a.endpoint: "http://minio.minio.svc.cluster.local:9000"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.hadoop.fs.s3a.access.key: "admin"
    spark.hadoop.fs.s3a.secret.key: "minio1234"
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
    spark.hadoop.fs.s3a.aws.credentials.provider: "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version: "2"
    spark.sql.catalog.hadoop_cat: "org.apache.iceberg.spark.SparkCatalog"
    spark.sql.catalog.hadoop_cat.type: "hadoop"
    spark.sql.catalog.hadoop_cat.warehouse: "s3a://warehouse/iceberg"
    spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    spark.kubernetes.executor.request.cores: "500m"
YAML
        echo "SparkApplication silver-writer-${RUN_ID} submitted"
        echo ${RUN_ID} > /tmp/silver_writer_run_id
        """,
    )

    wait_for_spark = BashOperator(
        task_id="wait_for_spark_completion",
        bash_command="""
        RUN_ID=$(cat /tmp/silver_writer_run_id)
        JOB_NAME="silver-writer-${RUN_ID}"
        echo "Waiting for $JOB_NAME..."
        for i in $(seq 1 60); do
          STATUS=$(oc get sparkapplication $JOB_NAME -n spark \
            -o jsonpath='{.status.applicationState.state}' 2>/dev/null)
          echo "Attempt $i: $STATUS"
          if [ "$STATUS" = "COMPLETED" ]; then
            echo "Job completed successfully"
            exit 0
          elif [ "$STATUS" = "FAILED" ]; then
            echo "Job FAILED"
            oc logs -n spark ${JOB_NAME}-driver 2>/dev/null | tail -30
            exit 1
          fi
          sleep 30
        done
        echo "Timeout waiting for job"
        exit 1
        """,
        execution_timeout=timedelta(minutes=35),
    )

    check_silver_after = BashOperator(
        task_id="check_silver_row_counts_after",
        bash_command="""
        TRINO_POD=$(oc get pod -n trino -o jsonpath='{.items[0].metadata.name}')
        echo "silver.decisions after:"
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT count(*) FROM iceberg.warehouse_silver.decisions" 2>/dev/null
        echo "silver.transactions after:"
        oc exec -n trino $TRINO_POD -- trino --execute \
          "SELECT count(*) FROM iceberg.warehouse_silver.transactions" 2>/dev/null
        """,
    )

    check_kafka >> check_silver_before >> submit_silver_writer >> wait_for_spark >> check_silver_after
