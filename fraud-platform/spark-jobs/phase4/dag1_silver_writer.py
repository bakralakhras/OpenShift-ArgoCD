"""
Phase 4 - DAG 1 Silver Writer
Reads fraud-decisions and audit-trail Kafka topics (Confluent Avro wire format)
and writes to Iceberg silver tables via Hadoop catalog.
Schema fetched ONCE in driver, broadcast to all executors.
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

def fetch_schema_json(schema_id):
    url = f"{SCHEMA_REGISTRY}/schemas/ids/{schema_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    raw = resp.json()["schema"]
    if isinstance(raw, str):
        return raw
    return json.dumps(raw)

def make_decoder_udf(schema_json_broadcast):
    @udf(returnType=MapType(StringType(), StringType()))
    def decoder(raw):
        import io, json, fastavro
        if raw is None or len(raw) < 5:
            return None
        if raw[0] != 0:
            return None
        payload = bytes(raw[5:])
        try:
            schema_str = schema_json_broadcast.value
            schema_dict = json.loads(schema_str) if isinstance(schema_str, str) else schema_str
            schema = fastavro.parse_schema(schema_dict)
            reader = io.BytesIO(payload)
            record = fastavro.schemaless_reader(reader, schema)
            result = {}
            for k, v in record.items():
                if v is None:
                    result[k] = None
                elif isinstance(v, list):
                    result[k] = str(v)
                elif isinstance(v, dict):
                    result[k] = str(v.get('value', list(v.values())[0] if v else ''))
                else:
                    result[k] = str(v)
            return result
        except Exception:
            return None
    return decoder

def main():
    spark = SparkSession.builder \
        .appName("phase4-silver-writer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("=== Phase 4 Silver Writer Starting ===")

    # Fetch schemas ONCE in driver and broadcast
    print("Fetching schemas from registry...")
    decisions_schema_json = fetch_schema_json(5)
    audit_schema_json = fetch_schema_json(6)
    print("Schemas fetched successfully")

    decisions_schema_bc = spark.sparkContext.broadcast(decisions_schema_json)
    audit_schema_bc = spark.sparkContext.broadcast(audit_schema_json)

    # Create silver tables in Hadoop catalog if they don't exist
    print("Creating silver tables if not exist...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS hadoop_cat.warehouse_silver.decisions (
            transactionid integer,
            uid string,
            transactionamt double,
            productcd string,
            card4 string,
            card6 string,
            p_emaildomain string,
            transaction_hour integer,
            fraud_score double,
            decision string,
            decision_reason string,
            model_version string,
            rules_triggered string,
            isfraud integer,
            ingestion_id string,
            transaction_date string
        ) USING iceberg

    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS hadoop_cat.warehouse_silver.transactions (
            transactionid integer,
            isfraud integer,
            transactionamt double,
            productcd string,
            uid string,
            transaction_date string,
            event_timestamp string,
            transaction_hour integer,
            ingestion_id string
        ) USING iceberg

    """)
    print("Tables ready")

    # --- fraud-decisions ---
    print("Reading fraud-decisions topic...")
    decisions_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "fraud-decisions") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    print(f"fraud-decisions raw count: {decisions_raw.count()}")

    decode_decision = make_decoder_udf(decisions_schema_bc)

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

        final_count = silver_decisions.count()
        print(f"Writing {final_count} rows to silver.decisions...")
        silver_decisions.writeTo("hadoop_cat.warehouse_silver.decisions") \
            .append()
        print("silver.decisions write complete")
    else:
        print("ERROR: 0 decisions decoded")

    # --- audit-trail ---
    print("Reading audit-trail topic...")
    audit_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "audit-trail") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    print(f"audit-trail raw count: {audit_raw.count()}")

    decode_audit = make_decoder_udf(audit_schema_bc)

    audit_decoded = audit_raw.select(
        decode_audit(col("value")).alias("data")
    ).filter(col("data").isNotNull())

    audit_count = audit_decoded.count()
    print(f"Decoded audit count: {audit_count}")

    if audit_count > 0:
        silver_transactions = audit_decoded.select(
            col("data.TransactionID").cast("integer").alias("transactionid"),
            col("data.isFraud").cast("integer").alias("isfraud"),
            col("data.TransactionAmt").cast("double").alias("transactionamt"),
            col("data.ProductCD").alias("productcd"),
            col("data.uid").alias("uid"),
            col("data.transaction_date").alias("transaction_date"),
            col("data.event_timestamp").alias("event_timestamp"),
            col("data.transaction_hour").cast("integer").alias("transaction_hour"),
            col("data.ingestion_id").alias("ingestion_id"),
        ).filter(col("transactionid").isNotNull())

        final_count = silver_transactions.count()
        print(f"Writing {final_count} rows to silver.transactions...")
        silver_transactions.writeTo("hadoop_cat.warehouse_silver.transactions") \
            .append()
        print("silver.transactions write complete")
    else:
        print("ERROR: 0 audit records decoded")

    print("=== Phase 4 Silver Writer Complete ===")
    spark.stop()

if __name__ == "__main__":
    main()
