import io
import json
import struct
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, coalesce, lit, udf
from pyspark.sql.types import BinaryType
import fastavro

SCHEMA_REGISTRY_URL = "http://apicurio-registry.schema-registry.svc:8080/apis/ccompat/v7"
SUBJECT = "transactions-raw-value"
BOOTSTRAP_SERVERS = "lakehouse-kafka-kafka-bootstrap.kafka.svc:9092"
SOURCE_TABLE = "hadoop_cat.warehouse_bronze.transactions"
TARGET_TOPIC = "transactions-raw"

def get_schema_id():
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions/latest")
    resp.raise_for_status()
    return resp.json()["id"]

def get_schema():
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions/latest")
    resp.raise_for_status()
    return json.loads(resp.json()["schema"])

schema_id = get_schema_id()
parsed_schema = fastavro.parse_schema(get_schema())

def serialize_avro(row_dict):
    """Serialize a row dict to Confluent wire-format Avro bytes."""
    buf = io.BytesIO()
    buf.write(b'\x00')
    buf.write(struct.pack('>I', schema_id))
    fastavro.schemaless_writer(buf, parsed_schema, row_dict)
    return buf.getvalue()

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

all_cols = replay_df.columns

def row_to_avro(row):
    d = row.asDict()
    # Schema expects specific types — cast to string where needed
    clean = {}
    for k, v in d.items():
        if v is None:
            clean[k] = None
        else:
            clean[k] = str(v) if not isinstance(v, (int, float, bool)) else v
    return serialize_avro(clean)

avro_udf = udf(row_to_avro, BinaryType())

kafka_df = (
    replay_df
    .withColumn("value", avro_udf(col("uid")))  # placeholder — see below
    .select(
        col("uid").cast("string").alias("key"),
        avro_udf(lit(None)).alias("value")
    )
)

# Better: use mapPartitions for full row serialization
def serialize_partition(rows):
    for row in rows:
        d = row.asDict()
        clean = {k: (str(v) if v is not None and not isinstance(v, (int, float, bool)) else v) for k, v in d.items()}
        key = clean.get("uid", "unknown").encode("utf-8")
        value = serialize_avro(clean)
        yield (key, value)

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

rdd = replay_df.rdd.mapPartitions(serialize_partition)
schema = StructType([
    StructField("key", BinaryType(), True),
    StructField("value", BinaryType(), True)
])
kafka_df = spark.createDataFrame(rdd, schema)

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
