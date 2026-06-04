# Databricks notebook source
# MAGIC %run ./Common_Utils

# COMMAND ----------

load_type="""
ALL
"""

# COMMAND ----------

def get_mysql_table_stats(
    source_schema: str,
    source_table: str,
    jdbc_url: str
) -> dict:
    """
    Returns MySQL table size and row count.

    Strategy:
    1. INFORMATION_SCHEMA (data_length + index_length)
    2. Fallback:
       - COUNT(*)
       - Assume 10 KB per row

    Output:
    {
        table_size_mb: float | None,
        row_count: int | None,
        source: "metadata" | "count_fallback" | "unknown"
    }
    """

    # -----------------------------------------
    # 1️⃣ INFORMATION_SCHEMA (preferred)
    # -----------------------------------------
    metadata_query = f"""
        SELECT
            data_length,
            index_length,
            table_rows
        FROM information_schema.tables
        WHERE table_schema = '{source_schema}'
          AND table_name   = '{source_table}'
    """

    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"({metadata_query}) t")
            .options(**jdbc_properties)
            .load()
        )

        rows = df.collect()
        if rows:
            r = rows[0].asDict()
            data_length = r.get("DATA_LENGTH")
            index_length = r.get("INDEX_LENGTH")
            table_rows = r.get("TABLE_ROWS")

            if data_length is not None and index_length is not None:
                total_bytes = int(data_length) + int(index_length)
                return {
                    "table_size_mb": round(total_bytes / (1024 * 1024), 2),
                    "row_count": int(table_rows) if table_rows is not None else None,
                    "source": "metadata"
                }
    except Exception:
        pass  # move to fallback

    # -----------------------------------------
    # 2️⃣ Fallback: COUNT(*) × 10 KB
    # -----------------------------------------
    count_query = f"""
        SELECT COUNT(*) AS row_count
        FROM {source_schema}.{source_table}
    """

    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"({count_query}) t")
            .options(**jdbc_properties)
            .load()
        )

        row_count = int(df.collect()[0]["row_count"])
        print(row_count)

        # Assume 10 KB per row
        avg_row_bytes = 10 * 1024
        total_bytes = row_count * avg_row_bytes

        return {
            "table_size_mb": round(total_bytes / (1024 * 1024), 2),
            "row_count": row_count,
            "source": "count_fallback"
        }

    except Exception:
        pass

    # -----------------------------------------
    # 3️⃣ Unknown
    # -----------------------------------------
    return {
        "table_size_mb": None,
        "row_count": None,
        "source": "unknown"
    }


# COMMAND ----------

def get_lower_upper_bounds(
    source_schema,
    source_table,
    partition_column,
    jdbc_url
):
    bounds_query = f"""
        SELECT
            MIN({partition_column}) AS min_id,
            MAX({partition_column}) AS max_id
        FROM {source_schema}.{source_table}
    """
    
    bounds_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({bounds_query}) t")
        .options(**jdbc_properties)
        .load()
    )

    bounds = bounds_df.collect()[0]
    return bounds

# COMMAND ----------

import math
def load_table_with_strategy(
    strategy: str,
    table_size_mb: float,
    source_schema: str,
    source_table: str,
    primary_column: str,
    partition_column: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    jdbc_url: str
):
    """
    Unified load function handling DIRECT, PARTITIONED and ROW_CHUNKED.
    """

    FULL_TARGET = f"{target_catalog}.{target_schema}.{target_table}"

    # -------------------------------------------------
    # Partition calculation (soft rule)
    # -------------------------------------------------
    partitions = max(16, int(table_size_mb // 200))
    partitions = ((partitions + 15) // 16) * 16  # multiple of 16

    # -------------------------------------------------
    # DIRECT LOAD
    # -------------------------------------------------
    if strategy == "DIRECT":
        log(f"Loading table directly without partitioning")
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"{source_schema}.{source_table}")
            .options(**jdbc_properties)
            .load()
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(FULL_TARGET)
        )
        return

    # -------------------------------------------------
    # PARTITIONED LOAD
    # -------------------------------------------------
    if strategy == "PARTITIONED":
        bounds = get_lower_upper_bounds(
            source_schema,
            source_table,
            partition_column,
            jdbc_url
        )
        log(f"Loading table using {partitions} partitions")

        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"{source_schema}.{source_table}")
            .option("partitionColumn", partition_column)
            .option("lowerBound", bounds["min_id"])
            .option("upperBound", bounds["max_id"])
            .option("numPartitions", partitions)
            .options(**jdbc_properties)
            .load()
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(FULL_TARGET)
        )
        return

    # -------------------------------------------------
    # ROW CHUNKED LOAD
    # -------------------------------------------------
    if strategy == "ROW_CHUNKED":

        CHUNK_MB = 50 * 1024  # 50 GB
        num_chunks = math.ceil(table_size_mb / CHUNK_MB)
        partitions = max(16, int(CHUNK_MB // 200))
        partitions = ((partitions + 15) // 16) * 16  # multiple of 16
        log(f"Loading table using chunking in {num_chunks} chunks using {partitions} partitions")
        bounds = get_lower_upper_bounds(
            source_schema,
            source_table,
            primary_column,
            jdbc_url
        )

        min_id = bounds["min_id"]
        max_id = bounds["max_id"]

        total_rows = max_id - min_id + 1
        rows_per_chunk = math.ceil(total_rows / num_chunks)
        log(f"The code will load {rows_per_chunk} rows per chunk")

        for i in range(num_chunks):
            chunk_min = min_id + (i * rows_per_chunk)
            chunk_max = min(chunk_min + rows_per_chunk - 1, max_id)

            log(f"Loading chunk {i+1}/{num_chunks}: starting from {chunk_min} to {chunk_max}")

            chunk_query = f"""
                (SELECT *
                 FROM {source_schema}.{source_table}
                 WHERE {primary_column} BETWEEN {chunk_min} AND {chunk_max}) t
            """
            
            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", chunk_query)
                .option("partitionColumn", partition_column)
                .option("lowerBound", chunk_min)
                .option("upperBound", chunk_max)
                .option("numPartitions", partitions)
                .options(**jdbc_properties)
                .load()
            )

            write_mode = "overwrite" if i == 0 else "append"
            log(f"Using write mode: {write_mode}")
            (
                df.write
                .format("delta")
                .mode(write_mode)
                .saveAsTable(FULL_TARGET)
            )

            log(f"Completed Loading chunk {i+1}/{num_chunks} from {chunk_min} to {chunk_max}")

        return

    raise ValueError(f"Unsupported strategy: {strategy}")


# COMMAND ----------

def assess_table_risk(
    source_schema: str,
    source_table: str,
    partition_column: str,
    jdbc_url: str
) -> dict:
    """
    Decide load strategy based only on table size.
    """

    # ------------------------------
    # Thresholds (MB)
    # ------------------------------
    DIRECT_MAX_MB      = 500
    PARTITIONED_MAX_MB = 102_400  # 100 GB

    # ------------------------------
    # Fetch table stats
    # ------------------------------
    stats = get_mysql_table_stats(
        source_schema=source_schema,
        source_table=source_table,
        jdbc_url=jdbc_url
    )

    log(f"Table size estimated to be {stats['table_size_mb']} MB from {stats['source']}")
    log(f"No. of rows in table estimated to be {stats['row_count']} from {stats['source']}")
    table_size_mb = stats["table_size_mb"]
    row_count     = stats["row_count"]

    # ------------------------------
    # Decide strategy
    # ------------------------------
    if table_size_mb < DIRECT_MAX_MB:
        strategy = "DIRECT"
        reasons = {
            f"Table less than 500 MB: {table_size_mb} MB, Row count: {row_count}"
        }

    elif table_size_mb < PARTITIONED_MAX_MB:
        strategy = "PARTITIONED"
        reasons = {
            f"Table between 500 MB and 100 GB: {table_size_mb} MB, Row count: {row_count}"
        }

    else:
        strategy = "ROW_CHUNKED"
        reasons = {
            f"Table greater than 100 GB: {table_size_mb} MB, Row count: {row_count}"
        }
    
    if partition_column is None:
        strategy = "DIRECT"
        reasons = {"No partition column specified!"}

    return {
        "base_strategy": strategy,
        "table_size_mb": table_size_mb,
        "reasons": reasons
    }


# COMMAND ----------

from datetime import datetime

# -------------------------------------------------
# Load & validate config (once)
# -------------------------------------------------
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
    log(f"Total Tables to load: {total_tables}")

    successful_tables = []
    failed_tables = []

    # -------------------------------------------------
    # Main loop
    # -------------------------------------------------
    for count, t in enumerate(filtered_tables, start=1):
        table_metadata = {}

        source_schema = t["source_schema"]
        source_table = t["source_table"]
        primary_column = t["primary_column"]
        partition_column = t["partition_column"]

        target_catalog = t["target_catalog"]
        target_schema = t["target_schema"]
        target_table = t["target_table"]

        table_id = f"{source_schema}.{source_table}"

        log(
            f"Processing table {count}/{total_tables}: "
            f"{table_id} → {target_catalog}.{target_schema}.{target_table}"
        )

        jdbc_url = (
            f"jdbc:mysql://{source_mysql_server}/{source_schema}"
            "?zeroDateTimeBehavior=convertToNull"
            "&useSSL=true&requireSSL=true"
            "&verifyServerCertificate=false"
            "&connectTimeout=10000"
            "&socketTimeout=600000"
            "&useCursorFetch=true"
            "&defaultFetchSize=500"
        )

        try:
            # ---------------- Step 1: Assess risk ----------------
            risk_result = assess_table_risk(
                source_schema=source_schema,
                source_table=source_table,
                partition_column=partition_column,
                jdbc_url=jdbc_url
            )

            strategy = risk_result["base_strategy"]
            table_size_mb = risk_result["table_size_mb"]

            log(f"Chosen strategy : {strategy}")
            log(f"Reason          : {risk_result['reasons']}")

            # ---------------- Step 2: Load ----------------
            load_table_with_strategy(
                strategy=strategy,
                table_size_mb=table_size_mb,
                source_schema=source_schema,
                source_table=source_table,
                primary_column=primary_column,
                partition_column=partition_column,
                target_catalog=target_catalog,
                target_schema=target_schema,
                target_table=target_table,
                jdbc_url=jdbc_url
            )

            successful_tables.append(table_id)
            log(f"Completed loading {table_id}\n")

        except Exception as e:
            failed_tables.append({
                "table": table_id,
                "error": str(e)
            })
            log(f"FAILED loading {table_id}: {e}\n")

finally:
    # -------------------------------------------------
    # Final Summary
    # -------------------------------------------------
    log("=" * 59)
    log("LOAD SUMMARY")
    log("=" * 59)

    log(f"Total tables     : {total_tables}")
    log(f"Successful loads : {len(successful_tables)}")
    log(f"Failed loads     : {len(failed_tables)}")

    if successful_tables:
        log("\nTables loaded successfully:")
        for t in successful_tables:
            log(f"  ✔ {t}")

    if failed_tables:
        log("\nTables failed:")
        for f in failed_tables:
            log(f"  ✖ {f['table']} → {f['error']}")

    log("=" * 59)
