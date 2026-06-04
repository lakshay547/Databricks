# Databricks notebook source
# MAGIC %run ./Common_Utils

# COMMAND ----------

def suggest_mysql_partition_column(
    source_schema: str,
    source_table: str,
    jdbc_url: str
) -> str | None:
    """
    Suggests a safe numeric partition column for Spark JDBC reads.
    Returns column name or None if no safe candidate is found.
    """

    numeric_types = {
        "int", "integer", "bigint", "smallint", "mediumint", "tinyint"
    }

    # ------------------------------
    # Column metadata
    # ------------------------------
    column_query = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = '{source_schema}'
          AND TABLE_NAME   = '{source_table}'
    """

    col_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({column_query}) t")
        .options(**jdbc_properties)
        .load()
    )

    columns = {
        r["COLUMN_NAME"]: {
            "type": r["DATA_TYPE"].lower(),
            "nullable": r["IS_NULLABLE"] == "YES"
        }
        for r in col_df.collect()
    }

    # ------------------------------
    # 1️⃣ Primary key (single, numeric)
    # ------------------------------
    pk_query = f"""
        SELECT COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = '{source_schema}'
          AND TABLE_NAME   = '{source_table}'
          AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
    """

    pk_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({pk_query}) t")
        .options(**jdbc_properties)
        .load()
    )

    pk_cols = [r["COLUMN_NAME"] for r in pk_df.collect()]

    if len(pk_cols) == 1:
        col = pk_cols[0]
        meta = columns.get(col)
        if meta and meta["type"] in numeric_types:
            return col

    # ------------------------------
    # 2️⃣ Unique indexed numeric column
    # ------------------------------
    unique_query = f"""
        SELECT DISTINCT k.COLUMN_NAME
        FROM information_schema.STATISTICS s
        JOIN information_schema.KEY_COLUMN_USAGE k
          ON s.TABLE_SCHEMA = k.TABLE_SCHEMA
         AND s.TABLE_NAME = k.TABLE_NAME
         AND s.INDEX_NAME = k.CONSTRAINT_NAME
        WHERE s.TABLE_SCHEMA = '{source_schema}'
          AND s.TABLE_NAME   = '{source_table}'
          AND s.NON_UNIQUE = 0
          AND k.CONSTRAINT_NAME <> 'PRIMARY'
    """

    uniq_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({unique_query}) t")
        .options(**jdbc_properties)
        .load()
    )

    for r in uniq_df.collect():
        col = r["COLUMN_NAME"]
        meta = columns.get(col)
        if meta and meta["type"] in numeric_types and not meta["nullable"]:
            return col

    # ------------------------------
    # 3️⃣ Any indexed numeric column
    # ------------------------------
    index_query = f"""
        SELECT DISTINCT COLUMN_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = '{source_schema}'
          AND TABLE_NAME   = '{source_table}'
    """

    idx_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({index_query}) t")
        .options(**jdbc_properties)
        .load()
    )

    for r in idx_df.collect():
        col = r["COLUMN_NAME"]
        meta = columns.get(col)
        if meta and meta["type"] in numeric_types and not meta["nullable"]:
            return col

    return None


# COMMAND ----------

def apply_partition_column_suggestions(
    tables_cfg: list[dict],
    results: dict
) -> list[dict]:
    """
    Updates partition_column in table configs based on suggestions.

    Args:
        tables_cfg: List of table config dictionaries
        results: Dict with key 'schema.table' -> suggested partition column

    Returns:
        Updated list of table configs
    """

    for table in tables_cfg:
        key = f"{table['source_schema']}.{table['source_table']}"

        if key in results:
            table["partition_column"] = results[key]

    return tables_cfg

# COMMAND ----------

load_type="ALL"

# COMMAND ----------

try:
    config_path = "/Workspace/Users/lakshay.goel@73strings.com/Code/mysql_tables.json"
    tables = load_and_validate_table_config(config_path)

    filtered_tables = [
        t for t in tables
        if should_load_table(t, load_type)
    ]

    if not filtered_tables:
        raise ValueError(f"Incorrect load_type: {load_type}")

    total_tables = len(filtered_tables)
    log(f"Total Tables: {total_tables}")

    results = []

    for t in tables:
        source_schema = t["source_schema"]
        source_table  = t["source_table"]

        jdbc_url = (
            f"jdbc:mysql://{source_mysql_server}/{source_schema}"
            "?zeroDateTimeBehavior=convertToNull"
            "&useSSL=true&requireSSL=true"
            "&verifyServerCertificate=false"
        )

        try:
            partition_column = suggest_mysql_partition_column(
                source_schema,
                source_table,
                jdbc_url
            )
        except Exception:
            partition_column = None

        results.append({
            "table": f"{source_schema}.{source_table}",
            "partition_column": partition_column
        })
except Exception as e:
    log(f"Error: {e}")
    raise e
finally:
    print("=" * 60)
    log("PARTITION COLUMN SUGGESTIONS")
    print("=" * 60)

    for r in results:
        table = r["table"]
        col   = r["partition_column"] or "NONE"
        print(f"{table:45} → {col}")

    print("=" * 60)



# COMMAND ----------

updated_tables = apply_partition_column_suggestions(
    filtered_tables,
    results
)

# Write back to JSON
with open(config_path, "w") as f:
    json.dump(updated_tables, f, indent=2)

# COMMAND ----------

print(filtered_tables)

# COMMAND ----------

print(updated_tables)