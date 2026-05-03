from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, current_date, lit, expr

spark = (
    SparkSession.builder
    .appName("phase2-bronze-bootstrap")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

transaction_path = "s3a://warehouse/landing/ieee-cis/train_transaction.csv"
identity_path = "s3a://warehouse/landing/ieee-cis/train_identity.csv"
target_table = "iceberg.warehouse_bronze.transactions"

print("Reading CSVs...")

transactions_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .csv(transaction_path)
)

identity_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .csv(identity_path)
)

tx_count = transactions_df.count()
id_count = identity_df.count()

print(f"Transactions: {tx_count} rows")
print(f"Identity: {id_count} rows")

transactions_df = (
    transactions_df
    .withColumn("TransactionID", col("TransactionID").cast("long"))
    .withColumn("TransactionDT", col("TransactionDT").cast("long"))
    .withColumn("TransactionAmt", col("TransactionAmt").cast("double"))
    .withColumn("isFraud", col("isFraud").cast("int"))
)

identity_df = identity_df.withColumn("TransactionID", col("TransactionID").cast("long"))

bronze_df = (
    transactions_df.alias("t")
    .join(identity_df.alias("i"), on="TransactionID", how="left")
)

bronze_df = (
    bronze_df
    .withColumn(
        "event_timestamp",
        expr("timestampadd(SECOND, TransactionDT, timestamp('2017-11-30 00:00:00'))")
    )
    .withColumn("ingestion_date", current_date())
    .withColumn("ingested_at", current_timestamp())
    .withColumn("source_file", lit("ieee-cis/train_transaction.csv+train_identity.csv"))
    .withColumn("pipeline_phase", lit("phase2_bronze_bootstrap"))
)

bronze_count = bronze_df.count()
print(f"Bronze rows: {bronze_count}")

if bronze_count != 590540:
    raise RuntimeError(f"Unexpected bronze row count: {bronze_count}. Expected 590540.")

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.warehouse_bronze")

print(f"Writing to Iceberg Hive catalog table: {target_table}")

(
    bronze_df.writeTo(target_table)
    .using("iceberg")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .partitionedBy(col("ingestion_date"))
    .createOrReplace()
)

print(f"Done. Wrote {bronze_count} rows to {target_table}")

spark.stop()
