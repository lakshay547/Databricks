# Databricks notebook source
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

def start_dlt_pipeline(pipeline_id: str):
    w.pipelines.start_update(pipeline_id=pipeline_id)


# COMMAND ----------

def stop_dlt_pipeline(pipeline_id: str):
    w.pipelines.stop(pipeline_id=pipeline_id)

# COMMAND ----------

def get_pipeline_state(pipeline_id: str):
    p = w.pipelines.get(pipeline_id)
    return {
        "state": p.state,
        "health": p.health
    }


# COMMAND ----------

from databricks.sdk.service.pipelines import PipelineState
import time

def wait_until_stopped(pipeline_id, poll_seconds=20):
    while True:
        p = w.pipelines.get(pipeline_id)
        state = p.state

        print("State:", state)

        if state in (
            PipelineState.IDLE,
            PipelineState.FAILED
        ):
            print("Pipeline is stopped / idle")
            break

        time.sleep(poll_seconds)

