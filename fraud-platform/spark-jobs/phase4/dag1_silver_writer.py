"""
Phase 4 - DAG 1 Silver Writer
Reads fraud-decisions and audit-trail Kafka topics (Confluent Avro wire format)
and writes to Iceberg silver tables via Hadoop catalog.
"""
import struct
import io
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import *
import fastavro

KAFKA_BOOTSTRAP = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"
SCHEMA_REGISTRY = "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"

def get_schema(schema_id):
    url = f"{SCHEMA_REGISTRY}/schemas/ids/{schema_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return fastavro.parse_schema(resp.json()["schema"] if isinstance(resp.json()["schema"], dict) 
                                  else __import__("json").loads(resp.json()["schema"]))

def decode_avro_message(raw_bytes, schema):
    """Decode Confluent wire format: magic byte + 4 byte schema ID + avro payload"""
    if raw_bytes is None or len(raw_bytes) < 5:
        return None
    # Skip magic byte (0x00) and 4-byte schema ID
    payload = raw_bytes[5:]
    reader = io.BytesIO(payload)
    try:
        return fastavro.schemaless_reader(reader, schema)
    except Exception:
        return None

def main():
    spark = SparkSession.builder \
        .appName("phase4-silver-writer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=== Phase 4 Silver Writer Starting ===")

    # --- Read fraud-decisions from Kafka ---
    print("Reading fraud-decisions topic...")
    decisions_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "fraud-decisions") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "500000") \
        .load()

    print(f"fraud-decisions raw count: {decisions_raw.count()}")

    # Get schema for fraud-decisions (schema ID 5)
    decisions_schema = get_schema(5)

    # Decode Avro messages using UDF
    from pyspark.sql.functions import udf
    from pyspark.sql.types import MapType, StringType

    @udf(returnType=MapType(StringType(), StringType()))
    def decode_decision_udf(raw):
        if raw is None:
            return None
        record = decode_avro_message(bytes(raw), decisions_schema)
        if record is None:
            return None
        return {k: str(v) if v is not None else None for k, v in record.items()}

    decisions_decoded = decisions_raw.select(
        decode_decision_udf(col("value")).alias("data")
    ).filter(col("data").isNotNull())

    # Build silver decisions schema
    silver_decisions = decisions_decoded.select(
        col("data.transaction_id").alias("transaction_id"),
        col("data.uid").alias("uid"),
        col("data.decision").alias("decision"),
        col("data.fraud_score").cast("double").alias("fraud_score"),
        col("data.is_fraud").cast("boolean").alias("is_fraud"),
        col("data.transaction_amount").cast("double").alias("transaction_amount"),
        col("data.product_cd").alias("product_cd"),
        col("data.transaction_timestamp").alias("transaction_timestamp"),
        col("data.transaction_date").alias("transaction_date"),
        col("data.night_score").cast("double").alias("night_score"),
        col("data.amount_score").cast("double").alias("amount_score"),
        col("data.velocity_score").cast("double").alias("velocity_score"),
        col("data.email_score").cast("double").alias("email_score"),
        col("data.product_score").cast("double").alias("product_score"),
        col("data.mismatch_score").cast("double").alias("mismatch_score"),
        col("data.cent_score").cast("double").alias("cent_score"),
        col("data.c13_score").cast("double").alias("c13_score"),
    ).filter(col("transaction_id").isNotNull())

    decisions_count = silver_decisions.count()
    print(f"Decoded decisions count: {decisions_count}")

    if decisions_count > 0:
        silver_decisions.writeTo("hadoop_cat.warehouse_silver.decisions") \
            .option("merge-schema", "true") \
            .append()
        print(f"Written {decisions_count} rows to silver.decisions")
    else:
        print("WARNING: No decision records decoded")

    # --- Read audit-trail from Kafka ---
    print("Reading audit-trail topic...")
    audit_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "audit-trail") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "500000") \
        .load()

    print(f"audit-trail raw count: {audit_raw.count()}")

    # Get schema for audit-trail (schema ID 6)
    audit_schema = get_schema(6)

    @udf(returnType=MapType(StringType(), StringType()))
    def decode_audit_udf(raw):
        if raw is None:
            return None
        record = decode_avro_message(bytes(raw), audit_schema)
        if record is None:
            return None
        return {k: str(v) if v is not None else None for k, v in record.items()}

    audit_decoded = audit_raw.select(
        decode_audit_udf(col("value")).alias("data")
    ).filter(col("data").isNotNull())

    silver_transactions = audit_decoded.select(
        col("data.transaction_id").alias("transaction_id"),
        col("data.uid").alias("uid"),
        col("data.transaction_amount").cast("double").alias("transaction_amount"),
        col("data.product_cd").alias("product_cd"),
        col("data.transaction_timestamp").alias("transaction_timestamp"),
        col("data.transaction_date").alias("transaction_date"),
        col("data.is_fraud").cast("boolean").alias("is_fraud"),
        col("data.email_domain").alias("email_domain"),
        col("data.hour_of_day").cast("integer").alias("hour_of_day"),
        col("data.day_of_week").cast("integer").alias("day_of_week"),
        col("data.cent_amount").cast("double").alias("cent_amount"),
    ).filter(col("transaction_id").isNotNull())

    transactions_count = silver_transactions.count()
    print(f"Decoded transactions count: {transactions_count}")

    if transactions_count > 0:
        silver_transactions.writeTo("hadoop_cat.warehouse_silver.transactions") \
            .option("merge-schema", "true") \
            .append()
        print(f"Written {transactions_count} rows to silver.transactions")
    else:
        print("WARNING: No transaction records decoded")

    print("=== Phase 4 Silver Writer Complete ===")
    spark.stop()

if __name__ == "__main__":
    main()
