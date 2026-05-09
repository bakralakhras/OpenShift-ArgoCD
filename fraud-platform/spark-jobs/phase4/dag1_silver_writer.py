"""
Phase 4 - DAG 1 Silver Writer
Reads fraud-decisions and audit-trail Kafka topics (Confluent Avro wire format)
and writes to Iceberg silver tables via Hadoop catalog.
"""
import io
import json
import requests
import fastavro
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import MapType, StringType

KAFKA_BOOTSTRAP = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"
SCHEMA_REGISTRY = "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"

SCHEMA_CACHE = {}

def fetch_schema(schema_id):
    url = f"{SCHEMA_REGISTRY}/schemas/ids/{schema_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    raw = resp.json()["schema"]
    if isinstance(raw, str):
        raw = json.loads(raw)
    return fastavro.parse_schema(raw)

def decode_confluent_avro(raw_bytes, schema_id):
    """
    Confluent wire format:
    Byte 0:    0x00 magic byte
    Bytes 1-4: schema ID big-endian uint32
    Bytes 5+:  Avro binary payload
    """
    if raw_bytes is None or len(raw_bytes) < 5:
        return None
    magic = raw_bytes[0]
    if magic != 0:
        return None
    payload = raw_bytes[5:]
    try:
        schema = SCHEMA_CACHE.get(schema_id)
        if schema is None:
            schema = fetch_schema(schema_id)
            SCHEMA_CACHE[schema_id] = schema
        reader = io.BytesIO(payload)
        record = fastavro.schemaless_reader(reader, schema)
        return {k: str(v) if v is not None else None for k, v in record.items()}
    except Exception as e:
        return None

def make_decoder_udf(schema_id):
    sid = schema_id
    registry = SCHEMA_REGISTRY

    @udf(returnType=MapType(StringType(), StringType()))
    def decoder(raw):
        import io, json, requests, fastavro
        if raw is None or len(raw) < 5:
            return None
        if raw[0] != 0:
            return None
        payload = bytes(raw[5:])
        try:
            url = f"{registry}/schemas/ids/{sid}"
            resp = requests.get(url, timeout=10)
            raw_schema = resp.json()["schema"]
            if isinstance(raw_schema, str):
                raw_schema = json.loads(raw_schema)
            schema = fastavro.parse_schema(raw_schema)
            reader = io.BytesIO(payload)
            record = fastavro.schemaless_reader(reader, schema)
            return {k: str(v) if v is not None else None for k, v in record.items()}
        except Exception as e:
            return None
    return decoder

def main():
    spark = SparkSession.builder \
        .appName("phase4-silver-writer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("=== Phase 4 Silver Writer Starting ===")

    # --- Read and decode fraud-decisions (schema ID 5) ---
    print("Reading fraud-decisions topic...")
    decisions_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "fraud-decisions") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    raw_count = decisions_raw.count()
    print(f"fraud-decisions raw count: {raw_count}")

    decode_decision = make_decoder_udf(5)

    decisions_decoded = decisions_raw.select(
        decode_decision(col("value")).alias("data")
    ).filter(col("data").isNotNull())

    decisions_count = decisions_decoded.count()
    print(f"Decoded decisions count: {decisions_count}")

    if decisions_count > 0:
        silver_decisions = decisions_decoded.select(
            col("data.TransactionID").cast("integer").alias("transactionid"),
            col("data.uid").alias("uid"),
            col("data.TransactionAmt").cast("double").alias("transactionamt"),
            col("data.ProductCD").alias("productcd"),
            col("data.card4").alias("card4"),
            col("data.card6").alias("card6"),
            col("data.P_emaildomain").alias("p_emaildomain"),
            col("data.transaction_hour").cast("integer").alias("transaction_hour"),
            col("data.fraud_score").cast("double").alias("fraud_score"),
            col("data.decision").alias("decision"),
            col("data.decision_reason").alias("decision_reason"),
            col("data.model_version").alias("model_version"),
            col("data.rules_triggered").alias("rules_triggered"),
            col("data.isFraud").cast("integer").alias("isfraud"),
            col("data.ingestion_id").alias("ingestion_id"),
            col("data.transaction_date").alias("transaction_date"),
        ).filter(col("transactionid").isNotNull())

        decisions_final_count = silver_decisions.count()
        print(f"Silver decisions rows to write: {decisions_final_count}")

        silver_decisions.writeTo("hadoop_cat.warehouse_silver.decisions") \
            .option("merge-schema", "true") \
            .append()
        print(f"Written {decisions_final_count} rows to silver.decisions")
    else:
        print("ERROR: 0 records decoded from fraud-decisions")

    # --- Read and decode audit-trail (schema ID 6) ---
    print("Reading audit-trail topic...")
    audit_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "audit-trail") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    print(f"audit-trail raw count: {audit_raw.count()}")

    decode_audit = make_decoder_udf(6)

    audit_decoded = audit_raw.select(
        decode_audit(col("value")).alias("data")
    ).filter(col("data").isNotNull())

    audit_count = audit_decoded.count()
    print(f"Decoded audit-trail count: {audit_count}")

    if audit_count > 0:
        silver_transactions = audit_decoded.select(
            col("data.TransactionID").cast("integer").alias("transactionid"),
            col("data.isFraud").cast("integer").alias("isfraud"),
            col("data.TransactionAmt").cast("double").alias("transactionamt"),
            col("data.ProductCD").alias("productcd"),
            col("data.uid").alias("uid"),
            col("data.transaction_date").alias("transaction_date"),
            col("data.decided_at").alias("event_timestamp"),
            col("data.transaction_hour").cast("integer").alias("transaction_hour"),
        ).filter(col("transactionid").isNotNull())

        transactions_count = silver_transactions.count()
        print(f"Silver transactions rows to write: {transactions_count}")

        silver_transactions.writeTo("hadoop_cat.warehouse_silver.transactions") \
            .option("merge-schema", "true") \
            .append()
        print(f"Written {transactions_count} rows to silver.transactions")
    else:
        print("ERROR: 0 records decoded from audit-trail")

    print("=== Phase 4 Silver Writer Complete ===")
    spark.stop()

if __name__ == "__main__":
    main()
