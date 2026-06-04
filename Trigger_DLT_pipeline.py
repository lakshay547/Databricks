# Databricks notebook source
# MAGIC %run /Workspace/Shared/etl_databricks/generator/load_utils_clone

# COMMAND ----------

# MAGIC %run /Workspace/Users/lakshay.goel@73strings.com/Code/DLT_Pipeline_Utils

# COMMAND ----------

live_batch_1="337b271b-73a6-4758-92bf-e9c192c057ce"
live_batch_2="365da543-31cd-4c48-a955-ed25c54d41cc"
pipeline_to_be_modified="live_batch_1"
module="equity"
tables=[
  "pwerm_widget"
]
cluster="Standard_D8ads_v6"

# COMMAND ----------

if pipeline_to_be_modified=="live_batch_1":
    stop_dlt_pipeline(live_batch_1)
    wait_until_stopped(live_batch_1)
elif pipeline_to_be_modified=="live_batch_2":
    stop_dlt_pipeline(live_batch_2)
    wait_until_stopped(live_batch_2)
else:
    print("No Pipeline Mentioned!!")

# COMMAND ----------

generate_cdc_historical_notebooks(cluster, pipeline_to_be_modified, module,tables)

# COMMAND ----------

if pipeline_to_be_modified=="live_batch_1":
    start_dlt_pipeline(live_batch_1)
    wait_until_stopped(live_batch_1)
elif pipeline_to_be_modified=="live_batch_2":
    start_dlt_pipeline(live_batch_2)
    wait_until_stopped(live_batch_2)
else:
    print("No Pipeline Mentioned!!")

# COMMAND ----------

add_kafka_topics(module)

# COMMAND ----------

batch=pipeline_to_be_modified.replace("live_", "")

# COMMAND ----------

generate_cdc_notebooks(cluster,batch,tables)

# COMMAND ----------

from databricks.sdk.service.pipelines import PipelineState
pipeline_state=get_pipeline_state(pipeline_to_be_modified)
if pipeline_state.get("state") == PipelineState.RUNNING:
    print("Pipeline is Running")