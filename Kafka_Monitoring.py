# Databricks notebook source
!pip install confluent-kafka
%restart_python

# COMMAND ----------

from confluent_kafka import Consumer, TopicPartition
from datetime import datetime
import json
from typing import List, Union

kafka_monitoring_table="etl_accord_uat.bronze.kafka_ingestion_frequency"

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

mysql_kafka_config = {
    "bootstrap.servers": kafka_broker,
    "security.protocol": kafka_security_protocol,
    "sasl.mechanism": kafka_sasl_mechanism,
    "sasl.username": mysql_kafka_api_key,
    "sasl.password": mysql_kafka_api_secret,
    "group.id": "kafka-monitoring-group",
    "auto.offset.reset": "latest"
}

postgres_kafka_config = {
    "bootstrap.servers": kafka_broker,
    "security.protocol": kafka_security_protocol,
    "sasl.mechanism": kafka_sasl_mechanism,
    "sasl.username": postgres_kafka_api_key,
    "sasl.password": postgres_kafka_api_secret,
    "group.id": "kafka-monitoring-group",
    "auto.offset.reset": "latest"
}

graviton_kafka_config = {
    "bootstrap.servers": kafka_broker,
    "security.protocol": kafka_security_protocol,
    "sasl.mechanism": kafka_sasl_mechanism,
    "sasl.username": graviton_kafka_api_key,
    "sasl.password": graviton_kafka_api_secret,
    "group.id": "kafka-monitoring-group",
    "auto.offset.reset": "latest"
}

# COMMAND ----------

def extract_topics_from_configs(
    module: str,
    configs: List[Union[str, dict]],
    topic_prefix: str,
    deduplicate: bool = True
) -> List[str]:
    """
    Extract Kafka topics from multiple JSON configs.

    Args:
        configs: List of file paths OR dict objects
        topic_prefix: Kafka_topic prefix
        deduplicate: Remove duplicate topics

    Returns:
        List of topic strings
    """

    topics = []

    for config in configs:
        # Load JSON if file path
        if isinstance(config, str):
            with open(config, "r") as f:
                config_data = json.load(f)
        else:
            config_data = config

        for table in config_data.get("tables", []):
            schema = table.get("schema_name")
            table_name = table.get("table_name")

            if not schema or not table_name:
                raise Exception("Improper json!!")

            if module=="postgres" and schema=="entity-management-system":
                topic = f"{topic_prefix}.{schema}.{table_name}"
                topics.append(topic)
            elif module=="graviton" and schema=="graviton":
                topic = f"{topic_prefix}.{schema}.{table_name}"
                topics.append(topic)
            elif module=="mysql" and schema!="entity-management-system" and schema!="graviton":
                topic = f"{topic_prefix}.{schema}.{table_name}"
                topics.append(topic)

    if deduplicate:
        topics = list(set(topics))

    return topics
    
def build_prefix_mapping(config_paths: list[str]) -> dict[str, str]:
    def extract_mapping(path: str) -> dict:
        with open(path, "r") as f:
            config = json.load(f)
        return {
            f"{entry['schema_name']}.{entry['table_name']}": entry["target_table_prefix"]
            for entry in config["tables"]
        }

    mapping = {}
    for m in map(extract_mapping, config_paths):
        mapping.update(m)

    print(f"Total mappings loaded: {len(mapping)}")
    return mapping

# ── Build topic → target_table directly from topic name ──────────────────────
def build_topic_to_target_table(all_topics: list[str], prefix_mapping: dict[str, str]) -> dict[str, str]:
    """
    topic format:  "KAFKA_PREFIX.schema_name.table_name"
    prefix_mapping: "schema_name.table_name" → "target_prefix"
    returns:        "KAFKA_PREFIX.schema_name.table_name" → "target_prefix_table_name"
    """
    topic_to_table = {}
    for topic in all_topics:
        parts = topic.split(".", 1)          # split only on first dot → ["KAFKA_PREFIX", "schema.table"]
        if len(parts) < 2:
            print(f"⚠️ Skipping malformed topic: {topic}")
            continue

        schema_table = parts[1]              # "schema_name.table_name"
        table_name   = schema_table.split(".")[-1]
        target_prefix = prefix_mapping.get(schema_table)

        if target_prefix:
            topic_to_table[topic] = f"{target_prefix}_{table_name}"
        else:
            print(f"⚠️ No prefix mapping found for: {schema_table}")

    return topic_to_table

configs = [
    "/Workspace/Shared/etl_databricks/generator/config/batch_1.json",
    "/Workspace/Shared/etl_databricks/generator/config/batch_2.json"
]

postgres_topics = extract_topics_from_configs("postgres",configs, postgres_kafka_topic_prefix)
graviton_topics = extract_topics_from_configs("graviton",configs, graviton_kafka_topic_prefix)
mysql_topics = extract_topics_from_configs("mysql",configs, mysql_kafka_topic_prefix)

# ── Combine all topics into one list ──────────────────────────────────────────
all_topics = postgres_topics + graviton_topics + mysql_topics

prefix_mapping       = build_prefix_mapping(configs)
topic_to_target_table = build_topic_to_target_table(all_topics, prefix_mapping)
print(f"Total topic→table mappings: {len(topic_to_target_table)}")


# COMMAND ----------

def get_offsets(topics, kafka_config):
    consumer = Consumer(kafka_config)
    rows = []

    for topic in topics:
        # Get metadata for topic
        metadata = consumer.list_topics(topic, timeout=10)

        if topic not in metadata.topics:
            print(f"⚠️ Topic not found: {topic}")
            continue

        partitions = metadata.topics[topic].partitions

        for p in partitions:
            tp = TopicPartition(topic, p)

            # Get latest offset (high watermark)
            low, high = consumer.get_watermark_offsets(tp)

            rows.append((topic, p, high, datetime.now()))

    consumer.close()
    return rows

# COMMAND ----------

# ── Get offsets and enrich with target_table ──────────────────────────────────
current_offsets = get_offsets(postgres_topics, postgres_kafka_config)
current_offsets.extend(get_offsets(graviton_topics, graviton_kafka_config))
current_offsets.extend(get_offsets(mysql_topics, mysql_kafka_config))

# Enrich rows with target_table: (topic, partition, curr_offset, recorded_at) → +target_table
enriched_offsets = [
    (topic_to_target_table.get(topic, "unknown"),topic, partition, curr_offset, recorded_at)
    for topic, partition, curr_offset, recorded_at in current_offsets
]

curr_df = spark.createDataFrame(
    enriched_offsets,
    ["target_table","topic", "partition", "curr_offset", "recorded_at"]
)


# COMMAND ----------

import pyspark.sql.functions as F
table_df=spark.table(f"{kafka_monitoring_table}")
table_df=table_df.withColumnRenamed("recorded_at","recorded_at_tgt")

# COMMAND ----------

join_df=curr_df.join(table_df, ["target_table","topic", "partition"], "left")

# COMMAND ----------

transform_df = join_df.withColumn(
    "last_data_arrival_time",
    F.when(F.col("last_data_arrival_time").isNull(), F.col("recorded_at"))  # Case 1
     .when(F.col("curr_offset") == F.col("latest_offset"), F.col("last_data_arrival_time"))  # Case 2
     .when(F.col("curr_offset") > F.col("latest_offset"), F.col("recorded_at"))  # Case 3
     .otherwise(F.col("last_data_arrival_time"))  # safety fallback
).withColumn(
    "idle_time",
    F.when(F.col("idle_time").isNull(), F.lit(-1))  # Case 1
     .when(F.col("curr_offset") == F.col("latest_offset"), F.unix_timestamp(F.col("recorded_at")) - 
    F.unix_timestamp(F.col("last_data_arrival_time")))  # Case 2
     .when(F.col("curr_offset") > F.col("latest_offset"), F.lit(0))  # Case 3
     .otherwise(F.col("idle_time"))  # safety fallback
).withColumn(
    "date_key",
    F.date_format("recorded_at", "yyyy-MM-dd")
).withColumn(
    "idle_increment",
    F.when(F.col("recorded_at_tgt").isNull(), F.lit(0))  # Case 1
     .when(
        F.col("curr_offset") == F.col("latest_offset"),
        F.unix_timestamp("recorded_at") - 
        F.unix_timestamp("recorded_at_tgt")
     )  # Case 2
     .when(F.col("curr_offset") > F.col("latest_offset"), F.lit(0))  # Case 3
     .otherwise(F.lit(0))
)

# COMMAND ----------

transform_df=transform_df.withColumn(
    "latest_offset",
    F.when(F.col("latest_offset").isNull(), F.col("curr_offset"))  # Case 1
     .when(F.col("curr_offset") == F.col("latest_offset"), F.col("latest_offset"))  # Case 2
     .when(F.col("curr_offset") > F.col("latest_offset"), F.col("curr_offset"))  # Case 3
     .otherwise(F.col("latest_offset"))  # safety fallback
)

# COMMAND ----------

transform_df = transform_df.withColumn(
    "idle_time_map",
    F.map_concat(
        # Remove current date key from existing map
        F.map_filter(
            F.coalesce(
                F.col("idle_time_map"),
                F.map_from_arrays(F.array(), F.array())
            ),
            lambda k, v: k != F.col("date_key")
        ),

        # Add updated struct for current date
        F.map_from_arrays(
            F.array(F.col("date_key")),
            F.array(
                F.struct(
                    # ✅ max_idle = max(previous, current)
                    F.greatest(
                        F.coalesce(
                            F.col("idle_time_map")[F.col("date_key")]["max_idle"],
                            F.lit(0)
                        ),
                        F.col("idle_time")
                    ).alias("max_idle"),

                    # ✅ total_idle = previous + increment
                    (
                        F.coalesce(
                            F.col("idle_time_map")[F.col("date_key")]["total_idle"],
                            F.lit(0)
                        ) + F.col("idle_increment")
                    ).alias("total_idle")
                )
            )
        )
    )
)

# COMMAND ----------

transform_df = transform_df.withColumn(
    "idle_time_map",
    F.map_filter(
        F.coalesce(
            F.col("idle_time_map"),
            F.map_from_arrays(F.array(), F.array())
        ),
        lambda k, v: F.to_date(k) >= F.date_sub(F.current_date(), 7)
    )
).withColumn(
    "priority",
    F.when(F.col("priority").isNull(), F.lit(3))
     .otherwise(F.col("priority"))
)

# COMMAND ----------

final_df=transform_df.select("target_table","topic","partition","latest_offset","recorded_at","last_data_arrival_time","idle_time","idle_time_map","priority")
final_df.write.mode("overwrite").insertInto(f"{kafka_monitoring_table}")

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE etl_accord_uat.bronze.kafka_ingestion_frequency(
#     target_table STRING,
#     topic STRING,
#     partition INT,
#     latest_offset LONG,
#     recorded_at TIMESTAMP,
#     last_data_arrival_time TIMESTAMP,
#     idle_time INT,
#     idle_time_map MAP<STRING, STRUCT<max_idle: BIGINT, total_idle: BIGINT>>,
#     priority INT
# )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE etl_accord_uat.bronze.multiplex_bronze(
#      key STRING,
#      value STRING,
#      topic STRING,
#      partition INT,
#      offset LONG,
#      timestamp TIMESTAMP,
#      batch_id LONG
# )
# PARTITIONED BY (batch_id, topic)

# COMMAND ----------

# from collections import Counter

# counts = Counter(all_topics)
# duplicates = [item for item, count in counts.items() if count > 1]

# print("Duplicate elements:", duplicates)
# # Output: [2, 4]
