import struct
import json
import io
import uuid

from datetime import datetime, timezone

import requests
import fastavro

KAFKA_BOOTSTRAP = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"

SCHEMA_REGISTRY = (
    "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"
)

INPUT_TOPIC = "fraud-scores"

DECISIONS_TOPIC = "fraud-decisions"
AUDIT_TOPIC = "audit-trail"

DLQ_TOPIC = "fraud-decisions-dlq"

CHECKPOINT = "s3a://warehouse/checkpoints/job4-decision"

DECISIONS_SCHEMA_ID = 5
AUDIT_SCHEMA_ID = 6

MODEL_VERSION = "rules-v1.0"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def fetch_parsed(subject):
    r = requests.get(
        f"{SCHEMA_REGISTRY}/subjects/{subject}/versions/latest",
        timeout=15
    )

    r.raise_for_status()

    return fastavro.parse_schema(
        json.loads(r.json()["schema"])
    )


def confluent_decode(raw_bytes, schema):
    if raw_bytes is None:
        return None

    buf = bytes(raw_bytes)

    if len(buf) < 5 or buf[0] != 0:
        return None

    try:
        return fastavro.schemaless_reader(
            io.BytesIO(buf[5:]),
            schema
        )

    except Exception:
        return None


def confluent_encode(record, schema, schema_id):
    buf = io.BytesIO()

    buf.write(b"\x00")
    buf.write(struct.pack(">I", schema_id))

    fastavro.schemaless_writer(
        buf,
        schema,
        record
    )

    return buf.getvalue()


def determine_decision(score):
    if score < 35:
        return "APPROVE", "Low fraud score"

    if score < 65:
        return "REVIEW", "Medium fraud risk"

    return "BLOCK", "High fraud risk"


def build_decision_record(rec):
    score = float(rec["fraud_score"])

    decision, reason = determine_decision(score)

    return {
        "ingestion_id": str(rec["ingestion_id"]),

        "decided_at": utc_now(),

        "transaction_date": str(rec["transaction_date"]),

        "TransactionID": int(rec["TransactionID"]),

        "uid": rec.get("uid"),

        "TransactionAmt": float(rec["TransactionAmt"]),

        "ProductCD": str(rec["ProductCD"]),

        "card4": rec.get("card4"),

        "card6": rec.get("card6"),

        "P_emaildomain": rec.get("P_emaildomain"),

        "transaction_hour": int(rec["transaction_hour"]),

        "fraud_score": score,

        "decision": decision,

        "decision_reason": reason,

        "rules_triggered": rec.get("rules_triggered") or [],

        "confidence": round(
            min(1.0, max(0.5, score / 100.0)),
            2
        ),

        "model_version": MODEL_VERSION,

        "score_high_amount_new_uid":
            float(rec.get("rule_high_amount_new_uid") or 0.0),

        "score_anonymous_email_amount":
            float(rec.get("rule_anonymous_email_amount") or 0.0),

        "score_product_h_cnp":
            float(rec.get("rule_product_h_cnp") or 0.0),

        "score_velocity_breach":
            float(rec.get("rule_velocity_breach") or 0.0),

        "score_night_foreign_email":
            float(rec.get("rule_night_foreign_email") or 0.0),

        "score_m_mismatch":
            float(rec.get("rule_m_mismatch") or 0.0),

        "score_c13_spike":
            float(rec.get("rule_c13_spike") or 0.0),

        "score_v258_v257_threshold":
            float(rec.get("rule_v258_v257_threshold") or 0.0),

        "isFraud": rec.get("isFraud"),
    }


def build_audit_record(decision_rec):
    return {
        "audit_id": str(uuid.uuid4()),

        "event_type": "DECIDED",

        "event_timestamp": utc_now(),

        "transaction_date": decision_rec["transaction_date"],

        "TransactionID": decision_rec["TransactionID"],

        "uid": decision_rec.get("uid"),

        "TransactionAmt": decision_rec["TransactionAmt"],

        "ProductCD": decision_rec["ProductCD"],

        "fraud_score": decision_rec["fraud_score"],

        "decision": decision_rec["decision"],

        "rules_triggered": decision_rec["rules_triggered"],

        "model_version": MODEL_VERSION,

        "isFraud": decision_rec.get("isFraud"),

        "pipeline_latency_ms": None,

        "ingestion_id": decision_rec["ingestion_id"],
    }


def main():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("phase3-job4-decision")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("[J4] Fetching schemas...")

    scores_schema = fetch_parsed("fraud-scores-value")
    decisions_schema = fetch_parsed("fraud-decisions-value")
    audit_schema = fetch_parsed("audit-trail-value")

    print("[J4] Schemas loaded")

    stream = (
        spark.readStream
        .format("kafka")

        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP
        )

        .option(
            "subscribe",
            INPUT_TOPIC
        )

        .option(
            "startingOffsets",
            "earliest"
        )

        .option(
            "maxOffsetsPerTrigger",
            5000
        )

        .option(
            "failOnDataLoss",
            "false"
        )

        .load()
    )

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            print(f"[J4] Batch {batch_id}: empty")

            return

        rows = batch_df.select(
            "value",
            "offset"
        ).collect()

        print(f"[J4] Batch {batch_id}: {len(rows)} messages")

        decision_out = []
        audit_out = []
        dlq_out = []

        for row in rows:
            rec = confluent_decode(
                row["value"],
                scores_schema
            )

            if rec is None:
                dlq_out.append({
                    "key": b"decode_error",

                    "value": json.dumps({
                        "reason": "decode_failed",
                        "offset": row["offset"],
                    }).encode("utf-8")
                })

                continue

            try:
                decision_rec = build_decision_record(rec)

                audit_rec = build_audit_record(decision_rec)

                key_str = str(
                    decision_rec.get("uid")
                    or decision_rec["TransactionID"]
                )

                decision_out.append({
                    "key": key_str.encode("utf-8"),

                    "value": confluent_encode(
                        decision_rec,
                        decisions_schema,
                        DECISIONS_SCHEMA_ID
                    ),
                })

                audit_out.append({
                    "key": key_str.encode("utf-8"),

                    "value": confluent_encode(
                        audit_rec,
                        audit_schema,
                        AUDIT_SCHEMA_ID
                    ),
                })

            except Exception as e:
                dlq_out.append({
                    "key": b"decision_error",

                    "value": json.dumps({
                        "reason": str(e),
                        "offset": row["offset"],
                    }).encode("utf-8")
                })

        if decision_out:
            (
                spark.createDataFrame(decision_out)
                .write
                .format("kafka")

                .option(
                    "kafka.bootstrap.servers",
                    KAFKA_BOOTSTRAP
                )

                .option(
                    "topic",
                    DECISIONS_TOPIC
                )

                .save()
            )

            print(
                f"[J4] -> {DECISIONS_TOPIC}: "
                f"{len(decision_out)}"
            )

        if audit_out:
            (
                spark.createDataFrame(audit_out)
                .write
                .format("kafka")

                .option(
                    "kafka.bootstrap.servers",
                    KAFKA_BOOTSTRAP
                )

                .option(
                    "topic",
                    AUDIT_TOPIC
                )

                .save()
            )

            print(
                f"[J4] -> {AUDIT_TOPIC}: "
                f"{len(audit_out)}"
            )

        if dlq_out:
            (
                spark.createDataFrame(dlq_out)
                .write
                .format("kafka")

                .option(
                    "kafka.bootstrap.servers",
                    KAFKA_BOOTSTRAP
                )

                .option(
                    "topic",
                    DLQ_TOPIC
                )

                .save()
            )

            print(
                f"[J4] -> {DLQ_TOPIC}: "
                f"{len(dlq_out)}"
            )

        print(
            f"[J4] Batch {batch_id}: "
            f"done decisions={len(decision_out)} "
            f"audit={len(audit_out)} "
            f"dlq={len(dlq_out)}"
        )

    (
        stream.writeStream
        .foreachBatch(process_batch)

        .option(
            "checkpointLocation",
            CHECKPOINT
        )

        .trigger(
            processingTime="30 seconds"
        )

        .start()

        .awaitTermination()
    )


if __name__ == "__main__":
    main()
