# Databricks notebook source
mysql_SCHEMA = "vc_user_management"
data = """vc_product_tools
vc_tool
"""

csv_string = ",".join(
    line.strip().lower()
    for line in data.splitlines()
    if line.strip()
)
print(csv_string)

# COMMAND ----------

MYSQL_HOST = "accord-usprod2-mysql-db.mysql.database.azure.com"
jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:3306/information_schema"
jdbc_properties = {
    "user": "confluent_user",
    "password": "Ribv92rtAv*9tbvAVna",
    "driver": "com.mysql.cj.jdbc.Driver"
}

TARGET_CATALOG = "etl_accord_us2"
TARGET_SCHEMA = "_historical_mysql_etl"
PARTITION_COLUMN = "id"


# COMMAND ----------

from pyspark.sql.functions import col, lower
pk_df = (
    spark.read.jdbc(
        url=jdbc_url,
        table="""
        (
            SELECT
                k.TABLE_SCHEMA,
                k.TABLE_NAME,
                k.COLUMN_NAME AS primary_column
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
            WHERE k.CONSTRAINT_NAME = 'PRIMARY'
        ) t
        """,
        properties=jdbc_properties
    )
    .withColumn("TABLE_NAME", lower(col("TABLE_NAME")))
)


# COMMAND ----------

tables = [t.strip().lower() for t in csv_string.split(",") if t.strip()]
print(len(tables))
result = []

for full_table_name in tables:
    module = full_table_name.split("_")[0]  # credit
    source_table = full_table_name.replace("credit_73_", "", 1)

    pk_row = (
        pk_df
        .filter(col("TABLE_SCHEMA") == mysql_SCHEMA)
        .filter(col("TABLE_NAME") == source_table)
        .limit(1)
        .collect()
    )

    if not pk_row:
        print(f"⚠️ Table not found or no PK: {source_table}")
        continue

    source_schema = pk_row[0]["TABLE_SCHEMA"]
    primary_column = pk_row[0]["primary_column"]

    result.append({
        "module": module,
        "source_schema": source_schema,
        "source_table": source_table,
        "primary_column": primary_column,
        "partition_column": PARTITION_COLUMN,
        "target_catalog": TARGET_CATALOG,
        "target_schema": TARGET_SCHEMA,
        "target_table": f"_historical_{full_table_name}"
    })


# COMMAND ----------

import json
with open("mysql_tables.json", "w") as f:
        json.dump(result, f, indent=2)