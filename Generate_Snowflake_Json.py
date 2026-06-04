# Databricks notebook source
SOURCE_CATALOG="etl_accord_us2"
SOURCE_SCHEMA="equity_mysql"
TABLE_LIST="""
MYSQL_EQUITY_73_PORTFOLIO_SUMMARY_DATA
"""
tables_list_csv = ",".join(
    line.strip().lower()
    for line in TABLE_LIST.splitlines()
    if line.strip()
)

print(tables_list_csv)

# COMMAND ----------

for table in tables_list_csv.split(","):
    table = table.strip()
    try:
        col_list = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table}").columns
    except Exception as e:
        msg = str(e)
        if "TABLE_OR_VIEW_NOT_FOUND" in msg:
            print(f"Table {table} not found")
            continue
        else:
            raise


# COMMAND ----------

import json

result_rows = []

for table in tables_list_csv.split(","):
    table = table.strip()

    col_list = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table}").columns

    if "org_id" in col_list:
        org_column = "org_id"
    elif "ORG_ID" in col_list:
        org_column = "ORG_ID"
    elif "tenant_org_id" in col_list:
        org_column = "tenant_org_id"
    elif "TENANT_ORG_ID" in col_list:
        org_column = "TENANT_ORG_ID"
    else:
        org_column = "None"

    result_rows.append({
        "source_schema": SOURCE_SCHEMA,
        "tablename": table,
        "orgcolumnname": org_column,
        "primaryKey": "id",
        "mappingDetails": ""
    })


result_df = spark.createDataFrame(result_rows)
display(result_df.select("source_schema","tablename", "orgcolumnname","primaryKey"))


# COMMAND ----------

with open("snowflake.json", "w") as f:
        json.dump(result_rows, f, indent=2)

# COMMAND ----------

result_csv = ",\n".join(
    f"\"{table.strip()}\""
    for table in tables_list_csv.split(",")
    if table.strip()
)

print(result_csv)

# COMMAND ----------

count_rows = []

for table in tables_list_csv.split(","):
    table = table.strip()

    col_list = spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table}").columns

    if "org_id" in col_list:
        org_column = "org_id"
    elif "ORG_ID" in col_list:
        org_column = "ORG_ID"
    elif "tenant_org_id" in col_list:
        org_column = "tenant_org_id"
    elif "TENANT_ORG_ID" in col_list:
        org_column = "TENANT_ORG_ID"
    else:
        org_column = "None"

    df=spark.sql(f"select * from {SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table} where {org_column}='b426c437-e8fc-471d-a932-b7df3e6822a3'")
    count=df.count()
    count_rows.append({
        "source_schema": SOURCE_SCHEMA,
        "tablename": table,
        "orgcolumnname": org_column,
        "count": count
    })


count_df = spark.createDataFrame(count_rows)
display(count_df.select("source_schema","tablename", "orgcolumnname","count"))