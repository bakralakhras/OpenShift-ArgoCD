"""
Phase 3 - Job 3: Fraud Scoring
Consumes : transactions-enriched  (Confluent Avro, schema ID 3, 49 fields)
Produces : fraud-scores           (Confluent Avro, schema ID 4, 28 fields)
           transactions-enriched-dlq (JSON)

8 rules — each contributes 0-100 points, weighted into final score 0-100.
No M columns in enriched schema — rule_m_mismatch always scores 0.

Rule weights (sum to 1.0):
  rule_high_amount_new_uid     0.20
  rule_anonymous_email_amount  0.15
  rule_product_h_cnp           0.15
  rule_velocity_breach         0.20
  rule_night_foreign_email     0.10
  rule_m_mismatch              0.05  (always 0 — no M cols in enriched)
  rule_c13_spike               0.10
  rule_v258_v257_threshold     0.05
"""
import struct, json, io
from datetime import datetime
import requests, fastavro

KAFKA_BOOTSTRAP  = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"
SCHEMA_REGISTRY  = "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"
INPUT_TOPIC      = "transactions-enriched"
OUTPUT_TOPIC     = "fraud-scores"
DLQ_TOPIC        = "transactions-enriched-dlq"
CHECKPOINT       = "s3a://warehouse/checkpoints/job3-scoring"
SCORES_SCHEMA_ID = 4
MODEL_VERSION    = "rules-v1.0"

# Rule weights — must sum to 1.0
WEIGHTS = {
    "rule_high_amount_new_uid":    0.20,
    "rule_anonymous_email_amount": 0.15,
    "rule_product_h_cnp":          0.15,
    "rule_velocity_breach":        0.20,
    "rule_night_foreign_email":    0.10,
    "rule_m_mismatch":             0.05,
    "rule_c13_spike":              0.10,
    "rule_v258_v257_threshold":    0.05,
}


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


def score_record(rec):
    """
    Apply 8 fraud rules. Each returns a component score 0-100.
    Returns dict of rule_name -> component_score.
    """
    amt          = float(rec.get("TransactionAmt") or 0.0)
    uid          = rec.get("uid")
    product      = rec.get("ProductCD", "")
    p_email      = (rec.get("P_emaildomain") or "").lower().strip()
    r_email      = (rec.get("R_emaildomain") or "").lower().strip()
    hour         = int(rec.get("transaction_hour") or 0)
    c13          = float(rec.get("C13") or 0.0)
    v258         = float(rec.get("V258") or 0.0)
    v257         = float(rec.get("V257") or 0.0)
    spend_1hr    = float(rec.get("spend_1hr") or 0.0)
    spend_24hr   = float(rec.get("spend_24hr") or 0.0)
    txn_1hr      = int(rec.get("txn_count_1hr") or 0)
    velocity_flag = bool(rec.get("velocity_flag") or False)
    anon         = bool(rec.get("p_email_is_anonymous") or False)
    hour_risk    = (rec.get("hour_risk_level") or "LOW").upper()
    p_risk_score = float(rec.get("p_email_risk_score") or 0.0)

    rules = {}

    # Rule 1: High amount + low transaction history for uid
    # Score 100 if amt > 500 and txn_count_1hr <= 1
    if amt > 500 and txn_1hr <= 1:
        rules["rule_high_amount_new_uid"] = min(100.0, (amt / 500.0) * 60.0)
    elif amt > 200 and txn_1hr <= 2:
        rules["rule_high_amount_new_uid"] = 30.0
    else:
        rules["rule_high_amount_new_uid"] = 0.0

    # Rule 2: Anonymous email domain + high amount
    if anon and amt > 100:
        rules["rule_anonymous_email_amount"] = min(100.0, p_risk_score + (amt / 100.0) * 10.0)
    elif anon:
        rules["rule_anonymous_email_amount"] = p_risk_score
    else:
        rules["rule_anonymous_email_amount"] = 0.0

    # Rule 3: ProductCD=H (home goods — high fraud rate in IEEE-CIS)
    # card-not-present proxy: card6 = 'credit' implied by product H
    if product == "H":
        rules["rule_product_h_cnp"] = 75.0
    elif product == "C":
        rules["rule_product_h_cnp"] = 60.0
    else:
        rules["rule_product_h_cnp"] = 0.0

    # Rule 4: Velocity breach — spend_1hr > 3x average hourly spend
    if velocity_flag:
        avg_hourly = spend_24hr / 24.0 if spend_24hr > 0 else 0.0
        if avg_hourly > 0:
            ratio = spend_1hr / avg_hourly
            rules["rule_velocity_breach"] = min(100.0, ratio * 25.0)
        else:
            rules["rule_velocity_breach"] = 50.0
    else:
        rules["rule_velocity_breach"] = 0.0

    # Rule 5: High-risk hour + foreign/anonymous email domain
    is_foreign = p_email not in {
        "gmail.com", "hotmail.com", "outlook.com",
        "icloud.com", "live.com", "msn.com",
        "comcast.net", "att.net", "verizon.net", "me.com"
    } and p_email != ""
    if hour_risk == "HIGH" and (anon or is_foreign):
        rules["rule_night_foreign_email"] = 80.0
    elif hour_risk == "HIGH":
        rules["rule_night_foreign_email"] = 40.0
    else:
        rules["rule_night_foreign_email"] = 0.0

    # Rule 6: M column mismatch — no M cols in enriched schema, always 0
    rules["rule_m_mismatch"] = 0.0

    # Rule 7: C13 spike — C13 is a count feature, high values = fraud signal
    # IEEE-CIS: C13 > 2500 is a strong fraud indicator
    if c13 > 2500:
        rules["rule_c13_spike"] = 100.0
    elif c13 > 1500:
        rules["rule_c13_spike"] = 60.0
    elif c13 > 500:
        rules["rule_c13_spike"] = 25.0
    else:
        rules["rule_c13_spike"] = 0.0

    # Rule 8: V258/V257 threshold — top Kaggle features
    # V258 > 0 and V257 > 0 together are strong fraud indicators
    if v258 > 0 and v257 > 0:
        rules["rule_v258_v257_threshold"] = min(100.0, (v258 + v257) * 10.0)
    elif v258 > 0 or v257 > 0:
        rules["rule_v258_v257_threshold"] = 30.0
    else:
        rules["rule_v258_v257_threshold"] = 0.0

    return rules


def build_fraud_score(rec, rule_scores):
    """Build the FraudScore Avro record from enriched record + rule scores."""
    # Weighted composite score
    fraud_score = sum(
        rule_scores.get(rule, 0.0) * weight
        for rule, weight in WEIGHTS.items()
    )
    fraud_score = round(min(100.0, max(0.0, fraud_score)), 2)

    # Which rules fired (score > 0)
    rules_triggered = [r for r, s in rule_scores.items() if s > 0.0]

    return {
        "ingestion_id":     str(rec.get("ingestion_id") or ""),
        "scored_at":        datetime.utcnow().isoformat(),
        "transaction_date": str(rec.get("transaction_date") or ""),
        "TransactionID":    int(rec["TransactionID"]),
        "uid":              rec.get("uid"),
        "TransactionAmt":   float(rec["TransactionAmt"]),
        "ProductCD":        str(rec["ProductCD"]),
        "card4":            rec.get("card4"),
        "card6":            rec.get("card6"),
        "P_emaildomain":    rec.get("P_emaildomain"),
        "transaction_hour": int(rec.get("transaction_hour") or 0),
        "has_identity":     bool(rec.get("has_identity") or False),
        "DeviceType":       rec.get("DeviceType"),
        "fraud_score":      fraud_score,
        "model_version":    MODEL_VERSION,
        "rule_high_amount_new_uid":    round(rule_scores["rule_high_amount_new_uid"], 2),
        "rule_anonymous_email_amount": round(rule_scores["rule_anonymous_email_amount"], 2),
        "rule_product_h_cnp":          round(rule_scores["rule_product_h_cnp"], 2),
        "rule_velocity_breach":        round(rule_scores["rule_velocity_breach"], 2),
        "rule_night_foreign_email":    round(rule_scores["rule_night_foreign_email"], 2),
        "rule_m_mismatch":             0.0,
        "rule_c13_spike":              round(rule_scores["rule_c13_spike"], 2),
        "rule_v258_v257_threshold":    round(rule_scores["rule_v258_v257_threshold"], 2),
        "rules_triggered":  rules_triggered,
        "spend_1hr":        rec.get("spend_1hr"),
        "txn_count_1hr":    rec.get("txn_count_1hr"),
        "velocity_flag":    bool(rec.get("velocity_flag") or False),
        "isFraud":          rec.get("isFraud"),
    }


def main():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("phase3-job3-scoring") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[J3] Fetching schemas...")
    enr_schema    = fetch_parsed("transactions-enriched-value")
    scores_schema = fetch_parsed("fraud-scores-value")
    print(f"[J3] enriched={len(enr_schema['fields'])} scores={len(scores_schema['fields'])}")

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
            print(f"[J3] Batch {batch_id}: empty")
            return

        rows = batch_df.select("value", "partition", "offset").collect()
        print(f"[J3] Batch {batch_id}: {len(rows)} messages from {INPUT_TOPIC}")

        scored_out, dlq_out = [], []

        for row in rows:
            rec = confluent_decode(row["value"], enr_schema)
            if rec is None:
                dlq_out.append({
                    "key":   b"decode_error",
                    "value": json.dumps({"reason": "decode_failed", "offset": row["offset"]}).encode()
                })
                continue
            try:
                rule_scores  = score_record(rec)
                fraud_score  = build_fraud_score(rec, rule_scores)
                encoded      = confluent_encode(fraud_score, scores_schema, SCORES_SCHEMA_ID)
                key_str      = str(fraud_score.get("uid") or fraud_score["TransactionID"])
                scored_out.append({
                    "key":   key_str.encode("utf-8"),
                    "value": encoded
                })
            except Exception as e:
                dlq_out.append({
                    "key":   b"score_error",
                    "value": json.dumps({"reason": str(e), "offset": row["offset"]}).encode()
                })

        if scored_out:
            spark.createDataFrame(scored_out).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("topic", OUTPUT_TOPIC).save()
            print(f"[J3] Batch {batch_id}: → {OUTPUT_TOPIC}: {len(scored_out)}")

        if dlq_out:
            spark.createDataFrame(dlq_out).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("topic", DLQ_TOPIC).save()
            print(f"[J3] Batch {batch_id}: → DLQ: {len(dlq_out)}")

        print(f"[J3] Batch {batch_id}: done scored={len(scored_out)} dlq={len(dlq_out)}")

    stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT) \
        .trigger(processingTime="30 seconds") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()
