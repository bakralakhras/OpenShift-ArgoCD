from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    coalesce,
    lit,
    to_json,
    struct
)

spark = (
    SparkSession.builder
    .appName("phase2-bronze-to-kafka-replay")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop")
    .config("spark.sql.catalog.hadoop_cat.warehouse", "s3a://warehouse/iceberg")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

source_table = "hadoop_cat.warehouse_bronze.transactions"
target_topic = "transactions-raw"
bootstrap_servers = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"

print(f"Reading Bronze table: {source_table}")

bronze_df = spark.table(source_table)

count = bronze_df.count()
print(f"Bronze source rows: {count}")

if count != 590540:
    raise RuntimeError(f"Unexpected source row count: {count}. Expected 590540.")

replay_df = (
    bronze_df
    .withColumn(
        "uid",
        concat(
            coalesce(col("card1").cast("string"), lit("unknown")),
            lit("_"),
            coalesce(col("addr1").cast("string"), lit("unknown"))
        )
    )
    .orderBy(col("TransactionDT").cast("long").asc())
)

kafka_df = (
    replay_df
    .select(
        col("uid").cast("string").alias("key"),
        to_json(struct(*[col(c) for c in replay_df.columns])).alias("value")
    )
)

print(f"Writing {count} rows to Kafka topic: {target_topic}")

(
    kafka_df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("topic", target_topic)
    .save()
)

print(f"Done. Replayed {count} rows to Kafka topic {target_topic}")

spark.stop()
