# Databricks notebook source
# DBTITLE 1,Pipeline Logging Utils — Title & Description
# MAGIC %md
# MAGIC # Pipeline Logging Utils
# MAGIC
# MAGIC This is a **shared utility notebook** that provides reusable logging functions for the entire medallion pipeline. Every pipeline notebook (`SAMPLE DATA GENERATOR`, `STEP 1`–`STEP 4`) calls `%run ./pipeline_logging_utils` at the top to import these functions.
# MAGIC
# MAGIC ## What It Does
# MAGIC * Creates the `pipeline_logs` Delta table automatically (IF NOT EXISTS) on first use
# MAGIC * Provides functions to log each pipeline step's start, success, failure, and metrics
# MAGIC * Captures: run ID, step name, status, timestamps, duration, record count, watermark, errors, and traceback
# MAGIC * Each pipeline run generates a unique `run_id` (UUID) that ties all step logs together
# MAGIC
# MAGIC ## Log Table Schema
# MAGIC | Column | Type | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `log_id` | STRING | UUID per log entry |
# MAGIC | `run_id` | STRING | UUID shared across all steps in one run |
# MAGIC | `step_name` | STRING | Which step (generate_data, create_schema, bronze, silver, gold) |
# MAGIC | `step_status` | STRING | STARTED / SUCCESS / FAILED / SKIPPED |
# MAGIC | `trigger_time` | TIMESTAMP | When the run was triggered |
# MAGIC | `step_start_time` | TIMESTAMP | When this step started |
# MAGIC | `step_end_time` | TIMESTAMP | When this step ended |
# MAGIC | `duration_seconds` | DOUBLE | How long the step took |
# MAGIC | `trigger_reason` | STRING | manual / scheduled / job |
# MAGIC | `record_count` | BIGINT | Records processed in this step |
# MAGIC | `watermark_value` | STRING | Incremental watermark used |
# MAGIC | `error_message` | STRING | Error summary if failed |
# MAGIC | `error_traceback` | STRING | Full Python traceback if failed |
# MAGIC | `additional_info` | STRING | Extra context as JSON string |

# COMMAND ----------

# DBTITLE 1,Pipeline Logging Utility Functions
# ─────────────────────────────────────────────────────────────────────
# PIPELINE LOGGING UTILITIES
# ─────────────────────────────────────────────────────────────────────
# This module provides reusable logging functions for the medallion
# pipeline. Import it with:  %run ./pipeline_logging_utils
#
# Functions:
#   init_pipeline_run(step_name)        → returns run_id, creates log table
#   log_step_start(run_id, step_name)   → logs STARTED status
#   log_step_success(run_id, step_name, record_count, watermark, extra)
#                                       → logs SUCCESS with metrics
#   log_step_skipped(run_id, step_name, reason)
#                                       → logs SKIPPED (e.g. no new data)
#   log_step_failure(run_id, step_name, error_msg, traceback_str)
#                                       → logs FAILED with error details
#   get_last_run_id()                   → returns the latest run_id
#   get_run_summary(run_id)             → returns a summary DataFrame for a run
# ─────────────────────────────────────────────────────────────────────
import uuid
import json
import traceback as _tb
from datetime import datetime

# Global variable to track the current run's trigger time
_PIPELINE_TRIGGER_TIME = None


def _get_database_name():
    """Retrieve the database_name from widget or return default."""
    try:
        return dbutils.widgets.get("database_name")
    except Exception:
        return "table_for_all"


def _ensure_log_table_exists(database_name):
    """Create the pipeline_logs Delta table if it doesn't already exist.
    Uses IF NOT EXISTS so it's idempotent — safe to call on every run.
    This ensures the log table is always available even if Step 1 hasn't run.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS medallian_catalog.{database_name}.pipeline_logs (
            log_id            STRING,
            run_id            STRING,
            step_name         STRING,
            step_status       STRING,
            trigger_time      TIMESTAMP,
            step_start_time   TIMESTAMP,
            step_end_time     TIMESTAMP,
            duration_seconds  DOUBLE,
            trigger_reason    STRING,
            record_count      BIGINT,
            watermark_value   STRING,
            error_message     STRING,
            error_traceback   STRING,
            additional_info   STRING
        ) USING DELTA
        COMMENT 'Pipeline execution logs — one row per step per run'
    """)


def _determine_trigger_reason():
    """Determine why the pipeline was triggered.
    Checks if running inside a Databricks Job vs ad-hoc notebook execution.
    """
    try:
        # In a job run, dbutils.jobs.workspaceClient has a job ID
        job_id = dbutils.jobs.workspaceClient.get("jobId", None)
        if job_id:
            return "job"
        return "manual"
    except Exception:
        return "manual"


def init_pipeline_run(step_name="pipeline"):
    """Initialize a new pipeline run or join an existing one.
    
    Generates a unique run_id for this pipeline execution. If a run_id
    widget parameter exists (passed from a job), it uses that instead
    so all tasks in a job share the same run_id.
    
    Parameters:
        step_name (str): Name of the step calling this function.
                        Used to determine if this is the first step.
    
    Returns:
        str: A UUID run_id string
    """
    global _PIPELINE_TRIGGER_TIME
    
    database_name = _get_database_name()
    _ensure_log_table_exists(database_name)
    
    # Check if run_id was passed as a widget (for job orchestration)
    try:
        run_id = dbutils.widgets.get("run_id")
        if not run_id or run_id == "":
            run_id = str(uuid.uuid4())
    except Exception:
        run_id = str(uuid.uuid4())
    
    # Set trigger time if this is a new run
    if _PIPELINE_TRIGGER_TIME is None:
        _PIPELINE_TRIGGER_TIME = datetime.now()
    
    return run_id


def _write_log_entry(run_id, step_name, step_status, trigger_time,
                     step_start_time, step_end_time, duration_seconds,
                     trigger_reason, record_count=None, watermark_value=None,
                     error_message=None, error_traceback=None,
                     additional_info=None):
    """Write a single log entry to the pipeline_logs Delta table.
    Internal function — called by the public logging functions.
    """
    database_name = _get_database_name()
    log_id = str(uuid.uuid4())
    
    log_data = [(log_id, run_id, step_name, step_status, trigger_time,
                 step_start_time, step_end_time, duration_seconds,
                 trigger_reason, record_count, watermark_value,
                 error_message, error_traceback, additional_info)]
    
    log_df = spark.createDataFrame(log_data, schema="""
        log_id STRING, run_id STRING, step_name STRING, step_status STRING,
        trigger_time TIMESTAMP, step_start_time TIMESTAMP, step_end_time TIMESTAMP,
        duration_seconds DOUBLE, trigger_reason STRING, record_count BIGINT,
        watermark_value STRING, error_message STRING, error_traceback STRING,
        additional_info STRING
    """)
    
    (log_df.write
        .format("DELTA")
        .mode("append")
        .saveAsTable(f"medallian_catalog.{database_name}.pipeline_logs"))


def log_step_start(run_id, step_name):
    """Log the START of a pipeline step.
    
    Parameters:
        run_id (str): The pipeline run ID (from init_pipeline_run)
        step_name (str): Name of the step (e.g. 'bronze_layer')
    
    Returns:
        datetime: The step start timestamp (pass to log_step_success/failure)
    """
    global _PIPELINE_TRIGGER_TIME
    if _PIPELINE_TRIGGER_TIME is None:
        _PIPELINE_TRIGGER_TIME = datetime.now()
    
    step_start = datetime.now()
    trigger_reason = _determine_trigger_reason()
    
    _write_log_entry(
        run_id=run_id,
        step_name=step_name,
        step_status="STARTED",
        trigger_time=_PIPELINE_TRIGGER_TIME,
        step_start_time=step_start,
        step_end_time=None,
        duration_seconds=None,
        trigger_reason=trigger_reason,
        record_count=None,
        watermark_value=None
    )
    
    print(f"📋 Log: Step '{step_name}' STARTED at {step_start}")
    return step_start


def log_step_success(run_id, step_name, step_start_time,
                      record_count=None, watermark_value=None,
                      additional_info=None):
    """Log the SUCCESSFUL completion of a pipeline step.
    
    Parameters:
        run_id (str): The pipeline run ID
        step_name (str): Name of the step
        step_start_time (datetime): Start time from log_step_start()
        record_count (int, optional): Number of records processed
        watermark_value (str, optional): Incremental watermark used
        additional_info (str, optional): Extra context as JSON string
    """
    global _PIPELINE_TRIGGER_TIME
    step_end = datetime.now()
    duration = (step_end - step_start_time).total_seconds()
    trigger_reason = _determine_trigger_reason()
    
    _write_log_entry(
        run_id=run_id,
        step_name=step_name,
        step_status="SUCCESS",
        trigger_time=_PIPELINE_TRIGGER_TIME,
        step_start_time=step_start_time,
        step_end_time=step_end,
        duration_seconds=duration,
        trigger_reason=trigger_reason,
        record_count=record_count,
        watermark_value=watermark_value,
        additional_info=additional_info
    )
    
    print(f"✅ Log: Step '{step_name}' SUCCEEDED in {duration:.1f}s (records: {record_count})")


def log_step_skipped(run_id, step_name, step_start_time, reason="No new data"):
    """Log that a step was SKIPPED (e.g. no new records to process).
    
    Parameters:
        run_id (str): The pipeline run ID
        step_name (str): Name of the step
        step_start_time (datetime): Start time from log_step_start()
        reason (str): Why the step was skipped
    """
    global _PIPELINE_TRIGGER_TIME
    step_end = datetime.now()
    duration = (step_end - step_start_time).total_seconds()
    trigger_reason = _determine_trigger_reason()
    
    _write_log_entry(
        run_id=run_id,
        step_name=step_name,
        step_status="SKIPPED",
        trigger_time=_PIPELINE_TRIGGER_TIME,
        step_start_time=step_start_time,
        step_end_time=step_end,
        duration_seconds=duration,
        trigger_reason=trigger_reason,
        additional_info=json.dumps({"skip_reason": reason})
    )
    
    print(f"⏭️  Log: Step '{step_name}' SKIPPED — {reason}")


def log_step_failure(run_id, step_name, step_start_time,
                     error_message, error_traceback_str=None):
    """Log the FAILURE of a pipeline step with full error details.
    
    Parameters:
        run_id (str): The pipeline run ID
        step_name (str): Name of the step
        step_start_time (datetime): Start time from log_step_start()
        error_message (str): Short error description
        error_traceback_str (str, optional): Full Python traceback string
    """
    global _PIPELINE_TRIGGER_TIME
    step_end = datetime.now()
    duration = (step_end - step_start_time).total_seconds()
    trigger_reason = _determine_trigger_reason()
    
    # Capture traceback if not provided
    if error_traceback_str is None:
        error_traceback_str = _tb.format_exc()
    
    _write_log_entry(
        run_id=run_id,
        step_name=step_name,
        step_status="FAILED",
        trigger_time=_PIPELINE_TRIGGER_TIME,
        step_start_time=step_start_time,
        step_end_time=step_end,
        duration_seconds=duration,
        trigger_reason=trigger_reason,
        error_message=error_message,
        error_traceback=error_traceback_str
    )
    
    print(f"❌ Log: Step '{step_name}' FAILED after {duration:.1f}s — {error_message}")
    print(f"   Traceback: {error_traceback_str[:500]}")


def get_last_run_id():
    """Get the most recent run_id from the pipeline_logs table.
    Useful for querying the status of the last pipeline run.
    """
    database_name = _get_database_name()
    try:
        result = spark.sql(f"""
            SELECT run_id, MAX(trigger_time) as latest_trigger
            FROM medallian_catalog.{database_name}.pipeline_logs
            GROUP BY run_id
            ORDER BY latest_trigger DESC
            LIMIT 1
        """).collect()
        if result:
            return result[0]['run_id']
        return None
    except Exception:
        return None


def get_run_summary(run_id=None):
    """Get a summary DataFrame of all steps in a pipeline run.
    
    Parameters:
        run_id (str, optional): Specific run_id to query.
                               If None, uses the latest run_id.
    
    Returns:
        DataFrame: Spark DataFrame with step statuses for the run
    """
    database_name = _get_database_name()
    if run_id is None:
        run_id = get_last_run_id()
    
    if run_id is None:
        print("⚠️ No pipeline runs found in logs")
        return None
    
    return spark.sql(f"""
        SELECT step_name, step_status, step_start_time, step_end_time,
               duration_seconds, record_count, watermark_value,
               error_message, additional_info
        FROM medallian_catalog.{database_name}.pipeline_logs
        WHERE run_id = '{run_id}'
        ORDER BY step_start_time
    """)


print("📋 Pipeline logging utilities loaded. "
      f"Logging to: medallian_catalog.{_get_database_name()}.pipeline_logs")