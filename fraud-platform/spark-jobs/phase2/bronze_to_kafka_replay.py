import io
import json
import struct
import uuid
import requests
import fastavro
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, coalesce, lit
from pyspark.sql.types import StructType, StructField, BinaryType

SCHEMA_REGISTRY_URL = "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"
SUBJECT = "transactions-raw-value"
BOOTSTRAP_SERVERS = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"
SOURCE_TABLE = "hadoop_cat.warehouse_bronze.transactions"
TARGET_TOPIC = "transactions-raw"

resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions/latest")
resp.raise_for_status()
SCHEMA_ID = resp.json()["id"]
PARSED_SCHEMA = fastavro.parse_schema(json.loads(resp.json()["schema"]))

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

print(f"Reading Bronze table: {SOURCE_TABLE}")
bronze_df = spark.table(SOURCE_TABLE)

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

schema_id_val = SCHEMA_ID
registry_url = SCHEMA_REGISTRY_URL
subject_val = SUBJECT


def normalize_null(value):
    if value is None:
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == "" or cleaned.lower() in {"null", "none", "nan"}:
            return None
        return cleaned

    return value


def avro_base_type(field_type):
    if isinstance(field_type, list):
        non_null_types = [t for t in field_type if t != "null"]
        if non_null_types:
            return non_null_types[0]
        return "null"

    return field_type


def cast_for_avro(value, field_type):
    value = normalize_null(value)
    base_type = avro_base_type(field_type)

    if value is None:
        return None

    try:
        if base_type == "string":
            return str(value)

        if base_type == "int":
            return int(float(value))

        if base_type == "long":
            return int(float(value))

        if base_type == "double":
            return float(value)

        if base_type == "float":
            return float(value)

        if base_type == "boolean":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in {"true", "1", "yes", "y"}
            return bool(value)

        return value

    except Exception:
        return None


def default_for_field(field):
    field_type = field.get("type")

    if isinstance(field_type, list) and "null" in field_type:
        return None

    if "default" in field:
        return field["default"]

    base_type = avro_base_type(field_type)

    if base_type == "string":
        return ""

    if base_type in {"int", "long"}:
        return 0

    if base_type in {"double", "float"}:
        return 0.0

    if base_type == "boolean":
        return False

    return None


def serialize_partition(rows):
    import io
    import json
    import struct
    import uuid
    import requests
    import fastavro
    from datetime import datetime

    resp = requests.get(f"{registry_url}/subjects/{subject_val}/versions/latest")
    resp.raise_for_status()

    schema_id = resp.json()["id"]
    parsed_schema = fastavro.parse_schema(json.loads(resp.json()["schema"]))

    for row in rows:
        row_dict = row.asDict()
        uid = row_dict.get("uid", "unknown") or "unknown"

        avro_record = {}

        for field in parsed_schema["fields"]:
            name = field["name"]
            field_type = field["type"]

            if name == "ingestion_id":
                avro_record[name] = str(uuid.uuid4())

            elif name == "kafka_offset":
                avro_record[name] = 0

            elif name == "kafka_partition":
                avro_record[name] = 0

            elif name == "ingested_at":
                avro_record[name] = datetime.utcnow().isoformat() + "Z"

            elif name == "ingestion_date":
                value = row_dict.get("ingestion_date")
                avro_record[name] = str(value) if value is not None else datetime.utcnow().strftime("%Y-%m-%d")

            elif name == "uid":
                avro_record[name] = str(uid)

            elif name == "event_timestamp":
                value = row_dict.get("event_timestamp")
                avro_record[name] = str(value) if value is not None else None

            else:
                value = row_dict.get(name)
                casted_value = cast_for_avro(value, field_type)

                if casted_value is None:
                    avro_record[name] = default_for_field(field)
                else:
                    avro_record[name] = casted_value

        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(struct.pack(">I", schema_id))
        fastavro.schemaless_writer(buf, parsed_schema, avro_record)

        yield (str(uid).encode("utf-8"), buf.getvalue())


rdd = replay_df.rdd.mapPartitions(serialize_partition)

out_schema = StructType([
    StructField("key", BinaryType(), True),
    StructField("value", BinaryType(), True)
])

kafka_df = spark.createDataFrame(rdd, out_schema)

print(f"Writing {count} rows as Avro to Kafka topic: {TARGET_TOPIC}")

(
    kafka_df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", TARGET_TOPIC)
    .save()
)

print(f"Done. Replayed {count} rows as Avro to {TARGET_TOPIC}")

spark.stop()
