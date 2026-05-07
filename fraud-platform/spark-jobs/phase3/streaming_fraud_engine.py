"""
Phase 3 - Job 1: Validation
Consumes : transactions-raw        (Confluent Avro, schema ID 1, 439 fields)
Produces : transactions-validated  (Confluent Avro, schema ID 7, 101 fields)
           transactions-raw-dlq    (JSON, bad records)
"""
import struct, json, io
from datetime import datetime, timedelta
import requests, fastavro

KAFKA_BOOTSTRAP     = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"
SCHEMA_REGISTRY_URL = "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"
INPUT_TOPIC         = "transactions-raw"
OUTPUT_TOPIC        = "transactions-validated"
DLQ_TOPIC           = "transactions-raw-dlq"
CHECKPOINT_PATH     = "s3a://warehouse/checkpoints/job1-validation"
VALIDATED_SCHEMA_ID = 7
BASE_DATE           = datetime(2017, 11, 30)
VALID_PRODUCTS      = {"W", "H", "C", "S", "R"}

VALIDATED_V_COLS = {
    "V258","V257","V201","V200",
    "V130","V131","V132","V133","V134","V135","V136","V137",
    "V95","V96","V97","V98","V99","V100","V101","V102"
}
VALIDATED_ID_STR_COLS = {"id_30", "id_31", "id_33"}
VALIDATED_ID_DBL_COLS = {"id_01","id_02","id_03","id_04","id_05","id_06"}


def fetch_parsed_schema(subject):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return fastavro.parse_schema(json.loads(resp.json()["schema"]))


def confluent_decode(raw_bytes, parsed_schema):
    if raw_bytes is None:
        return None
    buf = bytes(raw_bytes)
    if len(buf) < 5 or buf[0] != 0:
        return None
    try:
        return fastavro.schemaless_reader(io.BytesIO(buf[5:]), parsed_schema)
    except Exception:
        return None


def confluent_encode(record, parsed_schema, schema_id):
    buf = io.BytesIO()
    buf.write(b'\x00')
    buf.write(struct.pack('>I', schema_id))
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def validate(record):
    if record is None:
        return False, "decode_failed"
    if record.get("TransactionID") is None:
        return False, "null_transaction_id"
    amt = record.get("TransactionAmt")
    if amt is None or not isinstance(amt, (int, float)) or amt <= 0:
        return False, f"invalid_amount:{amt}"
    if record.get("ProductCD") not in VALID_PRODUCTS:
        return False, f"invalid_product:{record.get('ProductCD')}"
    return True, None


def to_bool(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.upper() == "T"
    return None


def to_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def build_validated(raw):
    dt_seconds = int(raw.get("TransactionDT") or 0)
    event_ts   = BASE_DATE + timedelta(seconds=dt_seconds)
    card1      = raw.get("card1")
    addr1      = raw.get("addr1")
    try:
        uid = f"{int(card1)}_{int(addr1)}" if card1 is not None and addr1 is not None else None
    except (ValueError, TypeError):
        uid = None
    amt      = float(raw.get("TransactionAmt") or 0.0)
    cent_amt = round(amt % 1, 4)

    v_vals  = {v: to_float(raw.get(v)) for v in VALIDATED_V_COLS}
    id_vals = {}
    for c in VALIDATED_ID_DBL_COLS:
        id_vals[c] = to_float(raw.get(c))
    for c in VALIDATED_ID_STR_COLS:
        v = raw.get(c)
        id_vals[c] = str(v) if v is not None else None

    has_identity = any(raw.get(f"id_{str(i).zfill(2)}") is not None for i in range(1, 7))

    validated = {
        "ingestion_id":     str(raw.get("ingestion_id") or ""),
        "ingested_at":      str(raw.get("ingested_at") or datetime.utcnow().isoformat()),
        "transaction_date": event_ts.strftime("%Y-%m-%d"),
        "TransactionID":    int(raw["TransactionID"]),
        "isFraud":          raw.get("isFraud"),
        "TransactionDT":    int(raw.get("TransactionDT") or 0),
        "event_timestamp":  event_ts.isoformat(),
        "transaction_hour": event_ts.hour,
        "transaction_dow":  event_ts.weekday(),
        "TransactionAmt":   float(raw["TransactionAmt"]),
        "cent_amt":         cent_amt,
        "ProductCD":        str(raw["ProductCD"]),
        "card1": to_float(raw.get("card1")),
        "card2": to_float(raw.get("card2")),
        "card3": to_float(raw.get("card3")),
        "card4": str(raw["card4"]) if raw.get("card4") is not None else None,
        "card5": to_float(raw.get("card5")),
        "card6": str(raw["card6"]) if raw.get("card6") is not None else None,
        "uid":   uid,
        "addr1": to_float(raw.get("addr1")),
        "addr2": to_float(raw.get("addr2")),
        "dist1": to_float(raw.get("dist1")),
        "dist2": to_float(raw.get("dist2")),
        "P_emaildomain": raw.get("P_emaildomain"),
        "R_emaildomain": raw.get("R_emaildomain"),
        "C1":  to_float(raw.get("C1")),  "C2":  to_float(raw.get("C2")),
        "C3":  to_float(raw.get("C3")),  "C4":  to_float(raw.get("C4")),
        "C5":  to_float(raw.get("C5")),  "C6":  to_float(raw.get("C6")),
        "C7":  to_float(raw.get("C7")),  "C8":  to_float(raw.get("C8")),
        "C9":  to_float(raw.get("C9")),  "C10": to_float(raw.get("C10")),
        "C11": to_float(raw.get("C11")), "C12": to_float(raw.get("C12")),
        "C13": to_float(raw.get("C13")), "C14": to_float(raw.get("C14")),
        "D1":  to_float(raw.get("D1")),  "D2":  to_float(raw.get("D2")),
        "D3":  to_float(raw.get("D3")),  "D4":  to_float(raw.get("D4")),
        "D5":  to_float(raw.get("D5")),  "D6":  to_float(raw.get("D6")),
        "D7":  to_float(raw.get("D7")),  "D8":  to_float(raw.get("D8")),
        "D9":  to_float(raw.get("D9")),  "D10": to_float(raw.get("D10")),
        "D11": to_float(raw.get("D11")), "D12": to_float(raw.get("D12")),
        "D13": to_float(raw.get("D13")), "D14": to_float(raw.get("D14")),
        "D15": to_float(raw.get("D15")),
        "M1": to_bool(raw.get("M1")), "M2": to_bool(raw.get("M2")),
        "M3": to_bool(raw.get("M3")),
        "M4": str(raw["M4"]) if raw.get("M4") is not None else None,
        "M5": to_bool(raw.get("M5")), "M6": to_bool(raw.get("M6")),
        "M7": to_bool(raw.get("M7")), "M8": to_bool(raw.get("M8")),
        "M9": to_bool(raw.get("M9")),
        "has_identity": has_identity,
        **v_vals,
        **id_vals,
    }
    return validated


def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("phase3-job1-validation") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[J1] Fetching schemas...")
    raw_schema = fetch_parsed_schema("transactions-raw-value")
    val_schema = fetch_parsed_schema("transactions-validated-value")
    print(f"[J1] raw={len(raw_schema['fields'])} fields  validated={len(val_schema['fields'])} fields")

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
            print(f"[J1] Batch {batch_id}: empty")
            return
        rows = batch_df.select("value", "partition", "offset").collect()
        print(f"[J1] Batch {batch_id}: {len(rows)} messages")

        valid_out, dlq_out = [], []

        for row in rows:
            record = confluent_decode(row["value"], raw_schema)
            is_valid, reason = validate(record)
            if is_valid:
                try:
                    validated = build_validated(record)
                    encoded   = confluent_encode(validated, val_schema, VALIDATED_SCHEMA_ID)
                    key_str   = str(validated.get("uid") or validated["TransactionID"])
                    valid_out.append({"key": key_str.encode("utf-8"), "value": encoded})
                except Exception as e:
                    dlq_out.append({
                        "key":   b"build_error",
                        "value": json.dumps({"reason": f"build_error:{e}", "offset": row["offset"]}).encode()
                    })
            else:
                dlq_out.append({
                    "key":   (reason or "unknown").encode("utf-8"),
                    "value": json.dumps({"reason": reason, "offset": row["offset"]}).encode()
                })

        if valid_out:
            spark.createDataFrame(valid_out).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("topic", OUTPUT_TOPIC).save()
            print(f"[J1] Batch {batch_id}: → {OUTPUT_TOPIC}: {len(valid_out)}")

        if dlq_out:
            spark.createDataFrame(dlq_out).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
                .option("topic", DLQ_TOPIC).save()
            print(f"[J1] Batch {batch_id}: → {DLQ_TOPIC}: {len(dlq_out)}")

        print(f"[J1] Batch {batch_id}: done valid={len(valid_out)} dlq={len(dlq_out)}")

    stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="30 seconds") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()
