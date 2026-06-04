# Databricks notebook source
# Databricks notebook source
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col

# ── Widgets ───────────────────────────────────────────────────────────────────
dbutils.widgets.dropdown("mode", "availableNow", ["availableNow", "continuous"], "Streaming Mode")

mode        = dbutils.widgets.get("mode")
max_workers = 10

# ── batch_id only meaningful for availableNow ─────────────────────────────────
batch_id = int(time.time())
print(f"Mode:      {mode}")
print(f"Batch ID:  {batch_id}")
print(f"Workers:   {max_workers}")

kafka_monitoring_table = "etl_accord_uat.bronze.kafka_ingestion_frequency"

# COMMAND ----------

# ── Topic list ────────────────────────────────────────────────────────────────
if mode == "availableNow":
    print("Fetching Priorty 2 topics")
    rows = (
        spark.read.format("delta")
        .table(kafka_monitoring_table)
        .filter((col("idle_time") == 0) & (col("priority") == 2))
        .select("topic", "target_table")
        .distinct()
        .collect()
    )

    topic_list            = [row["topic"] for row in rows]
    topic_to_target_table = {row["topic"]: row["target_table"] for row in rows}

elif mode == "continuous":
    print("Fetching Priorty 1 topics")
    rows = (
        spark.read.format("delta")
        .table(kafka_monitoring_table)
        .filter((col("priority") == 1))
        .select("topic", "target_table")
        .distinct()
        .collect()
    )
    topic_list            = [row["topic"] for row in rows]
    topic_to_target_table = {row["topic"]: row["target_table"] for row in rows}

print(f"Total topics: {len(topic_list)}")

# COMMAND ----------

# ── Notebook path and timeout per mode ───────────────────────────────────────
NOTEBOOK_CONFIG = {
    "availableNow": {
        "path": "/Workspace/Shared/etl_databricks/generator/Structured_Streaming",
        "timeout_seconds": 3600,          # 1 hour — stream stops after processing
    },
    "continuous": {
        "path": "/Workspace/Shared/etl_databricks/generator/Structured_Streaming_Continuous",
        "timeout_seconds": 86400,         # 24 hours — stream runs all day
    }
}

notebook_path    = NOTEBOOK_CONFIG[mode]["path"]
timeout_seconds  = NOTEBOOK_CONFIG[mode]["timeout_seconds"]

print(f"Notebook: {notebook_path}")
print(f"Timeout:  {timeout_seconds}s")

# COMMAND ----------

# ── Run child notebook ────────────────────────────────────────────────────────
def run_notebook(topic: str) -> dict:
    arguments = {
        "topic":        str(topic),
        "target_table": str(topic_to_target_table[topic]),
        "mode":         mode,
    }

    if mode == "availableNow":
        arguments["batch_id"] = str(batch_id)

    try:
        result = dbutils.notebook.run(
            notebook_path,
            timeout_seconds=timeout_seconds,
            arguments=arguments
        )
        return {"topic": topic, "target_table": str(topic_to_target_table[topic]), "status": "success", "result": result}
    except Exception as e:
        return {"topic": topic, "target_table": str(topic_to_target_table[topic]), "status": "failed", "error": str(e)}


# ── Run all topics in parallel ────────────────────────────────────────────────
results = []
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(run_notebook, topic): topic for topic in topic_list}

    for future in as_completed(futures):
        try:
            result = future.result()
        except Exception as e:
            topic  = futures[future]
            result = {"topic": topic, "target_table": topic_to_target_table.get(topic, "unknown"), "status": "failed", "error": str(e)}

        results.append(result)
        print(f"{'✅' if result['status'] == 'success' else '❌'} {result['topic']} → {result['target_table']}: {result['status']}")

# ── Summary ───────────────────────────────────────────────────────────────────
failed  = [r for r in results if r["status"] == "failed"]
success = [r for r in results if r["status"] == "success"]

print(f"\nMode:      {mode}")
print(f"Completed: {len(success)}/{len(results)} topics")
if failed:
    print("Failed topics:")
    for f in failed:
        print(f"  ❌ {f['topic']} → {f['target_table']}: {f['error']}")