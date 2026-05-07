"""
Phase 3 - Job 2: Enrichment
Consumes : transactions-validated  (Confluent Avro, schema ID 7, 101 fields)
Produces : transactions-enriched   (Confluent Avro, schema ID 3, 49 fields)
           transactions-validated-dlq (JSON)

Enrichment adds:
  - PostgreSQL lookups: p_email_risk_score, p_email_is_anonymous,
                        hour_risk_level, hour_risk_score, product_risk_tier
  - Velocity windows per uid: spend_1min, spend_5min, spend_1hr, spend_24hr,
                               txn_count_1hr, txn_count_24hr, velocity_flag

Enriched schema is a SUBSET of validated — only specific C/D/V columns kept.
velocity_flag and has_identity are non-nullable booleans (default: false).
"""
import struct, json, io
from datetime import datetime, timedelta
import requests, fastavro
import psycopg2

KAFKA_BOOTSTRAP    = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"
SCHEMA_REGISTRY    = "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"
INPUT_TOPIC        = "transactions-validated"
OUTPUT_TOPIC       = "transactions-enriched"
DLQ_TOPIC          = "transactions-validated-dlq"
CHECKPOINT         = "s3a://warehouse/checkpoints/job2-enrichment"
ENRICHED_SCHEMA_ID = 3

PG_HOST = "postgresql.postgresql.svc.cluster.local"
PG_PORT = 5432
PG_DB   = "metastore"
PG_USER = "hive"
PG_PASS = "hive123"

# Exact C/D/V columns required by enriched schema (confirmed from avsc)
ENRICHED_C_COLS = {"C1", "C2", "C6", "C11", "C13", "C14"}
ENRICHED_D_COLS = {"D1", "D2", "D3", "D10", "D15"}
ENRICHED_V_COLS = {"V258", "V257", "V201", "V200", "V95", "V96", "V97", "V98", "V99"}


def fetch_parsed(subject):
    r = requests.get(f"{SCHEMA_REGISTRY}/subjects/{subject}/versions/latest", timeout=15)
    r.raise_for_status()
    return fastavro.parse_schema(json.loads(r.json()["schema"]))


def confluent_decode(raw_bytes, schema):
    if raw_bytes is None:
        return None
    buf = bytes(raw_bytes)
    if len(buf) < 5 or buf[0] != 0:
        return None
    try:
        return fastavro.schemaless_reader(io.BytesIO(buf[5:]), schema)
    except Exception:
        return None


def confluent_encode(record, schema, schema_id):
    buf = io.BytesIO()
    buf.write(b'\x00')
    buf.write(struct.pack('>I', schema_id))
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()


def load_ref_tables():
    """Load all 4 reference tables into memory dicts once on driver."""
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    cur.execute("SELECT product_cd, risk_tier, risk_score FROM product_codes")
    product_risk = {r[0]: {"tier": r[1], "score": float(r[2])} for r in cur.fetchall()}

    cur.execute("SELECT domain, is_anonymous, risk_tier, risk_score FROM email_domain_risk")
    email_risk = {r[0]: {"anonymous": bool(r[1]), "tier": r[2], "score": float(r[3])} for r in cur.fetchall()}

    cur.execute("SELECT hour_of_day, risk_level, risk_score FROM hour_risk_level")
    hour_risk = {int(r[0]): {"level": r[1], "score": float(r[2])} for r in cur.fetchall()}

    cur.close()
    conn.close()

    print(f"[J2] Ref tables: products={len(product_risk)} email={len(email_risk)} hour={len(hour_risk)}")
    return product_risk, email_risk, hour_risk


def to_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def build_enriched(rec, product_risk, email_risk, hour_risk, uid_history):
    """Map validated record to enriched schema with all 49 fields."""

    uid   = rec.get("uid")
    hour  = int(rec.get("transaction_hour") or 0)
    amt   = float(rec.get("TransactionAmt") or 0.0)
    prod  = rec.get("ProductCD", "")
    p_email = (rec.get("P_emaildomain") or "").lower().strip()

    # ── PostgreSQL lookups ──
    pr = product_risk.get(prod, {"tier": None, "score": None})
    er = email_risk.get(p_email, {"anonymous": None, "score": None})
    hr = hour_risk.get(hour, {"level": None, "score": None})

    # ── Velocity windows ──
    ts_str = rec.get("event_timestamp", "")
    try:
        ts = datetime.fromisoformat(ts_str)
    except Exception:
        ts = datetime.utcnow()

    past = uid_history.get(uid, []) if uid else []

    def window_sum(minutes):
        cutoff = ts - timedelta(minutes=minutes)
        return round(sum(a for t, a in past if t >= cutoff), 2)

    def window_count(minutes):
        cutoff = ts - timedelta(minutes=minutes)
        return len([1 for t, a in past if t >= cutoff])

    spend_1min  = window_sum(1)
    spend_5min  = window_sum(5)
    spend_1hr   = window_sum(60)
    spend_24hr  = window_sum(1440)
    cnt_1hr     = window_count(60)
    cnt_24hr    = window_count(1440)

    # velocity_flag: spend in last 1hr > 3x average hourly spend over 24hr
    avg_hourly = spend_24hr / 24.0 if spend_24hr > 0 else 0.0
    velocity_flag = bool(spend_1hr > 3.0 * avg_hourly and avg_hourly > 0)

    # Record this event in history
    if uid:
        past.append((ts, amt))
        cutoff_24 = ts - timedelta(hours=24)
        uid_history[uid] = [(t, a) for t, a in past if t >= cutoff_24]

    enriched = {
        # Core passthrough fields
        "ingestion_id":     str(rec.get("ingestion_id") or ""),
        "transaction_date": str(rec.get("transaction_date") or ""),
        "TransactionID":    int(rec["TransactionID"]),
        "uid":              uid,
        "event_timestamp":  str(rec.get("event_timestamp") or ""),
        "transaction_hour": int(rec.get("transaction_hour") or 0),
        "transaction_dow":  int(rec.get("transaction_dow") or 0),
        "TransactionAmt":   float(rec["TransactionAmt"]),
        "cent_amt":         float(rec.get("cent_amt") or 0.0),
        "ProductCD":        str(rec["ProductCD"]),
        "card4":            rec.get("card4"),
        "card6":            rec.get("card6"),
        "P_emaildomain":    rec.get("P_emaildomain"),
        "R_emaildomain":    rec.get("R_emaildomain"),

        # Subset of C columns (only those in enriched schema)
        "C1":  to_float(rec.get("C1")),
        "C2":  to_float(rec.get("C2")),
        "C6":  to_float(rec.get("C6")),
        "C11": to_float(rec.get("C11")),
        "C13": to_float(rec.get("C13")),
        "C14": to_float(rec.get("C14")),

        # Subset of D columns
        "D1":  to_float(rec.get("D1")),
        "D2":  to_float(rec.get("D2")),
        "D3":  to_float(rec.get("D3")),
        "D10": to_float(rec.get("D10")),
        "D15": to_float(rec.get("D15")),

        # Subset of V columns
        "V258": to_float(rec.get("V258")),
        "V257": to_float(rec.get("V257")),
        "V201": to_float(rec.get("V201")),
        "V200": to_float(rec.get("V200")),
        "V95":  to_float(rec.get("V95")),
        "V96":  to_float(rec.get("V96")),
        "V97":  to_float(rec.get("V97")),
        "V98":  to_float(rec.get("V98")),
        "V99":  to_float(rec.get("V99")),

        # Velocity windows
        "spend_1min":     spend_1min  if uid else None,
        "spend_5min":     spend_5min  if uid else None,
        "spend_1hr":      spend_1hr   if uid else None,
        "spend_24hr":     spend_24hr  if uid else None,
        "txn_count_1hr":  cnt_1hr     if uid else None,
        "txn_count_24hr": cnt_24hr    if uid else None,
        "velocity_flag":  velocity_flag,

        # PostgreSQL enrichment
        "p_email_risk_score":   er["score"],
        "p_email_is_anonymous": er["anonymous"],
        "hour_risk_level":      hr["level"],
        "hour_risk_score":      hr["score"],
        "product_risk_tier":    pr["tier"],

        # Identity
        "has_identity": bool(rec.get("has_identity") or False),
        "DeviceType":   rec.get("DeviceType"),
        "isFraud":      rec.get("isFraud"),
    }

    return enriched


def main():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("phase3-job2-enrichment") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[J2] Fetching schemas...")
    val_schema = fetch_parsed("transactions-validated-value")
    enr_schema = fetch_parsed("transactions-enriched-value")
    print(f"[J2] validated={len(val_schema['fields'])} enriched={len(enr_schema['fields'])}")

    print("[J2] Loading reference tables from PostgreSQL...")
    product_risk, email_risk, hour_risk = load_ref_tables()

    # Per-driver velocity state — persists across batches
    uid_history = {}

    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .option("failOnDataLoss", "false") \
        .load()

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            print(f"[J2] Batch {batch_id}: empty")
            return

        rows = batch_df.select("value", "partition", "offset").collect()
        print(f"[J2] Batch {batch_id}: {len(rows)} messages from {INPUT_TOPIC}")

        enriched_out, dlq_out = [], []

        for row in rows:
            rec = confluent_decode(row["value"], val_schema)
            if rec is None:
                dlq_out.append({
                    "key":   b"decode_error",
                    "value": json.dumps({"reason": "decode_failed", "offset": row["offset"]}).encode()
                })
                continue
            try:
                enriched = build_enriched(rec, product_risk, email_risk, hour_risk, uid_history)
                encoded  = confluent_encode(enriched, enr_schema, ENRICHED_SCHEMA_ID)
                key_str  = str(enriched.get("uid") or enriched["TransactionID"])
                enriched_out.append({
                    "key":   key_str.encode("utf-8"),
                    "value": encoded
                })
            except Exception as e:
                dlq_out.append({
                    "key":   b"enrich_error",
                    "value": json.dumps({"reason": str(e), "offset": row["offset"]}).encode()
                })

        if enriched_out:
            spark.createDataFrame(enriched_out).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("topic", OUTPUT_TOPIC).save()
            print(f"[J2] Batch {batch_id}: → {OUTPUT_TOPIC}: {len(enriched_out)}")

        if dlq_out:
            spark.createDataFrame(dlq_out).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("topic", DLQ_TOPIC).save()
            print(f"[J2] Batch {batch_id}: → DLQ: {len(dlq_out)}")

        print(f"[J2] Batch {batch_id}: done enriched={len(enriched_out)} dlq={len(dlq_out)}")

    stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT) \
        .trigger(processingTime="30 seconds") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()
