# Databricks notebook source
# ── Get topic passed from parent ──────────────────────────────────────────────
topic = dbutils.widgets.get("topic")
batch_id = dbutils.widgets.get("batch_id")
target_table = dbutils.widgets.get("target_table")
print(f"Processing table: {target_table} from {topic}")

kafka_broker = "lkc-vzjkjp.westeurope.azure.private.confluent.cloud:9092"
kafka_security_protocol = "SASL_SSL"
kafka_sasl_mechanism = "PLAIN"


mysql_kafka_topic_prefix = "ACCORD_UAT_CONNECTOR_TOPIC"
mysql_kafka_api_key = "2GAQFDCSG7YEEADD"
mysql_kafka_api_secret = "cfltTlpo2hLbBtb32C8jj8grT/NZid2cjqshhwhbEbC4UqgS46flC7LtQ5mXTsPQ"


postgres_kafka_topic_prefix = "PSQL_ACCORD_UAT_CONNECTOR_TOPIC"
postgres_kafka_api_key = "5UPXIB2K5PTPJM3Y"
postgres_kafka_api_secret = "cflt3sRpDH/cvVPGSDEVYNWVPgSz8eMljpQHfSM6B0uB+bn/GpuQFMJ+SsorjBuA"

graviton_kafka_topic_prefix = "ACCORD_UAT_GX_CONNECTOR_TOPIC"
graviton_kafka_api_key = "2C4W3WFYQ37NQPWY"
graviton_kafka_api_secret = "cfltfSThsZax/robPrF9OKi+O5PTfta+arb+t2O42oyi36isIYNAdiH41VVdI26A"

if topic.startswith(mysql_kafka_topic_prefix):
    kafka_api_key=mysql_kafka_api_key
    kafka_api_secret=mysql_kafka_api_secret
elif topic.startswith(postgres_kafka_topic_prefix):
    kafka_api_key=postgres_kafka_api_key
    kafka_api_secret=postgres_kafka_api_secret
elif topic.startswith(graviton_kafka_topic_prefix):
    kafka_api_key=graviton_kafka_api_key
    kafka_api_secret=graviton_kafka_api_secret

jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_api_key}' password='{kafka_api_secret}';"

print(jaas_config)

# COMMAND ----------

# ── Stream ────────────────────────────────────────────────────────────────────
import pyspark.sql.functions as F
import time
from pyspark.sql.types import LongType
kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .load()
)

kafka_stream_df = kafka_stream_df.withColumn("batch_id", F.lit(batch_id).cast(LongType()))

parsed_df = (
    kafka_stream_df
    .select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
        F.col("batch_id")
    )
)

query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/mnt/checkpoints/kafka_streams/{topic}")  # ← clean per-topic checkpoint
    .trigger(availableNow=True)
    .toTable("etl_accord_uat.bronze.multiplex_bronze")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC Load silver Tables

# COMMAND ----------

target_catalog = "etl_accord_uat"
target_schema  = "silver"                             # your silver schema

# ── Derive source_schema and source_table from topic ─────────────────────────
# topic format: <PREFIX>.<source_schema>.<source_table>
topic_parts   = topic.split(".")
source_schema = topic_parts[1]                        # e.g. entity-management-system
source_table  = topic_parts[2]                        # e.g. entity

key = f"{source_schema}.{source_table}"

primary_column = "id"

print(f"Topic:          {topic}")
print(f"Source schema:  {source_schema}")
print(f"Source table:   {source_table}")
print(f"Target table:   {target_catalog}.{target_schema}.{target_table}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    TimestampType, DateType, BooleanType, StringType, DecimalType
)
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import base64
import decimal
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

# ── UDFs (same as DLT) ────────────────────────────────────────────────────────
def base64_decode_description(data):
    if not data:
        return None
    try:
        return base64.b64decode(data).decode("utf-8")
    except Exception:
        return data

spark.udf.register("base64_decode_description", base64_decode_description, StringType())

def create_decimal_udf(scale):
    def decode_decimal_base64(s):
        if s is None:
            return None
        try:
            b = base64.b64decode(s)
            unscaled = int.from_bytes(b, byteorder="big", signed=True)
            value = decimal.Decimal(unscaled) / (decimal.Decimal(10) ** scale)
            return str(value)
        except Exception:
            return None
    return udf(decode_decimal_base64, StringType())

# ── Schema helpers (same as DLT) ──────────────────────────────────────────────
def handle_fields(schema):
    new_fields = []
    for field in schema.fields:
        if isinstance(field.dataType, (TimestampType, DateType, DecimalType, BooleanType)):
            new_fields.append(StructField(field.name, StringType(), field.nullable))
        else:
            new_fields.append(field)
    return StructType(new_fields)

def convert_fields(df, schema):
    for field in schema.fields:
        col_name = field.name
        dt = field.dataType
        cleaned = F.regexp_replace(F.col(col_name), '^"|"$', '')
        cleaned = F.regexp_replace(cleaned, "^'|'$", "")

        if isinstance(dt, TimestampType):
            df = df.withColumn(col_name,
                F.when(cleaned.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_timestamp(cleaned))
                 .when(cleaned.rlike(r"^\d{15,17}$"), (cleaned.cast("decimal(20,0)") / F.lit(1_000_000)).cast("timestamp"))
                 .when(cleaned.rlike(r"^\d{12,13}$"), (cleaned.cast("bigint") / 1000).cast("timestamp"))
                 .when(cleaned.rlike(r"^\d{10}$"), cleaned.cast("bigint").cast("timestamp"))
                 .otherwise(None)
            )
        elif isinstance(dt, DateType):
            df = df.withColumn(col_name,
                F.when(cleaned.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(cleaned))
                 .when(cleaned.rlike(r"^\d{15,17}$"), F.to_date((cleaned.cast("decimal(20,0)") / F.lit(1_000_000)).cast("timestamp")))
                 .when(cleaned.rlike(r"^\d{12,13}$"), F.to_date(F.from_unixtime(cleaned.cast("bigint") / 1000)))
                 .when(cleaned.rlike(r"^\d{10}$"), F.to_date(F.from_unixtime(cleaned.cast("bigint"))))
                 .when(cleaned.rlike(r"^\d{4,5}$"), F.to_date(F.from_unixtime(cleaned.cast("bigint") * 86400)))
                 .otherwise(None)
            )
        elif isinstance(dt, DecimalType):
            decimal_udf = create_decimal_udf(dt.scale)
            df = df.withColumn(col_name, decimal_udf(cleaned).cast(DecimalType(dt.precision, dt.scale)))

        elif isinstance(dt, BooleanType):
            df = df.withColumn(col_name,
                F.when(cleaned.isin("true", "TRUE", "1"), True)
                 .when(cleaned.isin("false", "FALSE", "0"), False)
                 .otherwise(None)
            )

    if source_table == "general_details" and "description" in df.columns:
        df = df.withColumn("description", F.expr("base64_decode_description(description)"))

    return df

# COMMAND ----------

from pyspark.sql.window import Window

def deduplicate_df(df, primary_column: str, sequence_col: str = "kafka_ingestion_timestamp"):
    """
    Keep only the latest record per primary key before merging.
    """
    window = Window.partitionBy(primary_column).orderBy(F.col(sequence_col).desc())
    
    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

# COMMAND ----------

# ── 1. Read schema from historical table ──────────────────────────────────────
original_json_schema = spark.read.table(f"{target_catalog}.{target_schema}.{target_table}").schema
modified_json_schema = handle_fields(original_json_schema)

# ── 2. Read raw data from multiplex_bronze for this topic + batch ─────────────
bronze_df = (
    spark.read
    .table(f"{target_catalog}.bronze.multiplex_bronze")
    .filter(
        (F.col("topic") == topic) &
        (F.col("batch_id") == F.lit(batch_id).cast(LongType()))
    )
)

print(f"Bronze rows for this batch: {bronze_df.count()}")

# COMMAND ----------

# ── 3. Extract CDC payload ────────────────────────────────────────────────────
extracted_df = bronze_df.select(
    F.col("timestamp").alias("kafka_ingestion_timestamp"),
    F.when(
        F.get_json_object(F.col("value"), "$.op") == "d",
        F.get_json_object(F.col("value"), "$.before")
    ).otherwise(
        F.get_json_object(F.col("value"), "$.after")
    ).alias("payload"),
    F.get_json_object(F.col("value"), "$.op").alias("operation")
).withColumn(
    "json_data",
    F.from_json(F.col("payload"), modified_json_schema)
)

converted_df = convert_fields(
    extracted_df.select(
        "kafka_ingestion_timestamp",
        "operation",
        "json_data.*"
    ),
    original_json_schema
)

converted_df = converted_df.filter(F.col("operation").isNotNull())
print(f"CDC rows after parsing: {converted_df.count()}")

# ── 4. Separate and deduplicate BEFORE dropping columns ───────────────────────
deletes_df = converted_df.filter(F.col("operation") == "d")
upserts_df = converted_df.filter(F.col("operation") != "d")

# ── Deduplicate while kafka_ingestion_timestamp is still present ──────────────
upserts_df = deduplicate_df(upserts_df, primary_column, "kafka_ingestion_timestamp")
deletes_df = deduplicate_df(deletes_df, primary_column, "kafka_ingestion_timestamp")

print(f"Upserts after dedup: {upserts_df.count()}")
print(f"Deletes after dedup: {deletes_df.count()}")

# ── Drop CDC metadata columns AFTER dedup ─────────────────────────────────────
upserts_df = upserts_df.drop("operation", "kafka_ingestion_timestamp")
deletes_df = deletes_df.drop("kafka_ingestion_timestamp")   # keep operation for delete merge condition

# ── Create target table if it doesn't exist ───────────────────────────────────
full_target_table = f"{target_catalog}.{target_schema}.{target_table}"

if not spark.catalog.tableExists(full_target_table):
    print(f"Creating target table: {full_target_table}")
    upserts_df.limit(0).write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .saveAsTable(full_target_table)

# ── Perform MERGE ─────────────────────────────────────────────────────────────
delta_target = DeltaTable.forName(spark, full_target_table)
update_cols  = {c: f"source.{c}" for c in upserts_df.columns if c != primary_column}

if not upserts_df.isEmpty():
    (
        delta_target.alias("target")
        .merge(
            upserts_df.alias("source"),
            f"target.{primary_column} = source.{primary_column}"
        )
        .whenMatchedUpdate(set=update_cols)
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ Upserted into {full_target_table}")

if not deletes_df.isEmpty():
    (
        delta_target.alias("target")
        .merge(
            deletes_df.alias("source"),
            f"target.{primary_column} = source.{primary_column}"
        )
        .whenMatchedDelete()
        .execute()
    )
    print(f"🗑️ Deleted from {full_target_table}")

# COMMAND ----------

# # ── 3. Extract CDC payload (same logic as DLT view) ───────────────────────────
# extracted_df = bronze_df.select(
#     F.col("timestamp").alias("kafka_ingestion_timestamp"),
#     F.when(
#         F.get_json_object(F.col("value"), "$.op") == "d",
#         F.get_json_object(F.col("value"), "$.before")
#     ).otherwise(
#         F.get_json_object(F.col("value"), "$.after")
#     ).alias("payload"),
#     F.get_json_object(F.col("value"), "$.op").alias("operation")
# ).withColumn(
#     "json_data",
#     F.from_json(F.col("payload"), modified_json_schema)
# )

# converted_df = convert_fields(
#     extracted_df.select(
#         "kafka_ingestion_timestamp",
#         "operation",
#         "json_data.*"
#     ),
#     original_json_schema
# )

# converted_df = converted_df.filter(F.col("operation").isNotNull())

# print(f"CDC rows after parsing: {converted_df.count()}")

# # ── 4. Apply SCD Type 1 MERGE (replaces dlt.apply_changes) ───────────────────
# full_target_table = f"{target_catalog}.{target_schema}.{target_table}"

# # Separate deletes and upserts
# deletes_df = converted_df.filter(F.col("operation") == "d")
# upserts_df = converted_df.filter(F.col("operation") != "d") \
#                           .drop("operation", "kafka_ingestion_timestamp")

# # ── Deduplicate before merge ──────────────────────────────────────────────────
# upserts_df = deduplicate_df(upserts_df, primary_column, "kafka_ingestion_timestamp")
# deletes_df = deduplicate_df(deletes_df, primary_column, "kafka_ingestion_timestamp")

# print(f"Upserts after dedup: {upserts_df.count()}")
# print(f"Deletes after dedup: {deletes_df.count()}")

# # ── Create target table if it doesn't exist ───────────────────────────────────
# if not spark.catalog.tableExists(full_target_table):
#     print(f"Creating target table: {full_target_table}")
#     upserts_df.limit(0).write \
#         .format("delta") \
#         .option("delta.enableChangeDataFeed", "true") \
#         .saveAsTable(full_target_table)

# # ── Perform MERGE ─────────────────────────────────────────────────────────────
# delta_target = DeltaTable.forName(spark, full_target_table)

# # Build dynamic update map (all columns except primary key)
# update_cols = {c: f"source.{c}" for c in upserts_df.columns if c != primary_column}

# if upserts_df.count() > 0:
#     (
#         delta_target.alias("target")
#         .merge(
#             upserts_df.alias("source"),
#             f"target.{primary_column} = source.{primary_column}"
#         )
#         .whenMatchedUpdate(set=update_cols)
#         .whenNotMatchedInsertAll()
#         .execute()
#     )
#     print(f"✅ Upserted {upserts_df.count()} rows into {full_target_table}")

# # ── Apply deletes separately ──────────────────────────────────────────────────
# if deletes_df.count() > 0:
#     (
#         delta_target.alias("target")
#         .merge(
#             deletes_df.alias("source"),
#             f"target.{primary_column} = source.{primary_column}"
#         )
#         .whenMatchedDelete()
#         .execute()
#     )
#     print(f"🗑️ Deleted {deletes_df.count()} rows from {full_target_table}")

# COMMAND ----------

dbutils.notebook.exit("success")