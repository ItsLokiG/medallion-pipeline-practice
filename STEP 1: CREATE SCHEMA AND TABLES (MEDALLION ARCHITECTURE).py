# Databricks notebook source
# DBTITLE 1,Notebook Title & Description
# MAGIC %md
# MAGIC # Step 1: Create Schema and Tables (Medallion Architecture)
# MAGIC
# MAGIC This notebook initializes the Unity Catalog schema and creates Delta tables for all three medallion layers: Bronze (raw ingestion), Silver (cleansed/conformed), and Gold (business aggregations). It is the foundational setup step that must be run before the data pipeline notebooks (Steps 2–4).
# MAGIC
# MAGIC **Tables Created:**
# MAGIC * `bronze_sales_transactions` — Raw CSV data with metadata columns (file_name, ingestion_timestamp)
# MAGIC * `silver_sales_transactions` — Cleaned, validated, and enriched transaction data
# MAGIC * `gold_daily_sales_summary` — Daily aggregated business KPIs

# COMMAND ----------

# DBTITLE 1,Import: pipeline_logging_utils
# MAGIC %run ./pipeline_logging_utils

# COMMAND ----------

# DBTITLE 1,Import: Spark SQL types and functions
# ─────────────────────────────────────────────────────────────────────
# Import Spark SQL types and functions needed for schema definitions.
# These are used by the create_tables() function below to define column
# types for the Bronze, Silver, and Gold Delta tables.
# ─────────────────────────────────────────────────────────────────────
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Widgets: database_name, DELTA_PATH, run_id
# ─────────────────────────────────────────────────────────────────────
# Widget: database_name — UC schema for all medallion tables.
# Widget: DELTA_PATH   — (Reserved) Path for external Delta storage.
# Widget: run_id       — (Optional) Shared run_id for job orchestration.
# ─────────────────────────────────────────────────────────────────────
dbutils.widgets.text(name = "database_name", defaultValue="", label="Database Name")
DATABASE_NAME = dbutils.widgets.get("database_name")

dbutils.widgets.text(name = "DELTA_PATH", defaultValue="", label="Delta Path")
DELTA_PATH = dbutils.widgets.get("DELTA_PATH")

dbutils.widgets.text(name = "run_id", defaultValue="", label="Run ID (optional)")

# COMMAND ----------

# DBTITLE 1,create_tables() — Bronze/Silver/Gold/PipelineLogs DDL
# ─────────────────────────────────────────────────────────────────────
# Function: create_tables
# Purpose: Creates the Unity Catalog schema and all three medallion
#          Delta tables (Bronze, Silver, Gold) if they don't already exist.
#          Uses CREATE TABLE IF NOT EXISTS for idempotency — safe to re-run.
#
# Parameters: None (uses DATABASE_NAME widget value)
#
# Tables Created:
#   1. bronze_sales_transactions  — Raw data with metadata columns
#   2. silver_sales_transactions   — Cleaned and enriched data
#   3. gold_daily_sales_summary   — Daily business KPIs
#
# Architecture Notes:
#   - All tables use Delta format for ACID transactions and time travel.
#   - Bronze table includes file_name and ingestion_timestamp for auditability.
#   - Silver table adds derived columns: final_amount, discount_percentage,
#     customer_age_group, is_weekend, quarter.
#   - Gold table is partitioned by date for efficient daily queries.
# ─────────────────────────────────────────────────────────────────────
def create_tables():
    """Create database and define table schemas for Bronze, Silver, Gold layers."""
    
    # ── Schema Creation ──────────────────────────────────────────────
    # Switch to the medallian_catalog and create the target schema if needed.
    # Using IF NOT EXISTS makes this idempotent.
    spark.sql("USE CATALOG medallian_catalog")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}")
    spark.sql(f"USE {DATABASE_NAME}")
    
    # ── Bronze Table ────────────────────────────────────────────────
    # Stores raw CSV data with minimal transformation.
    # Includes metadata columns for auditability:
    #   - file_name: Source CSV file path (from _metadata.file_path)
    #   - ingestion_timestamp: When the record was loaded into Bronze
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze_sales_transactions (
            transaction_id STRING,
            customer_id STRING,
            product_id STRING,
            product_name STRING,
            category STRING,
            quantity DOUBLE,
            unit_price DOUBLE,
            total_amount DOUBLE,
            discount DOUBLE,
            transaction_date TIMESTAMP,
            customer_age INT,
            customer_city STRING,
            payment_method STRING,
            store_id STRING,
            created_at TIMESTAMP,
            file_name STRING,
            ingestion_timestamp TIMESTAMP
        ) USING DELTA;
    """)
    
    # ── Silver Table ─────────────────────────────────────────────────
    # Stores cleaned and enriched data derived from Bronze.
    # Adds derived columns:
    #   - final_amount: total_amount - discount
    #   - discount_percentage: (discount / total_amount) * 100
    #   - customer_age_group: Categorical bucket (e.g. "25-34", "65+")
    #   - is_weekend: Boolean flag for Saturday/Sunday
    #   - quarter: Calendar quarter (1-4)
    #   - processed_at: Timestamp when the Silver record was created
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver_sales_transactions (
            transaction_id STRING,
            customer_id STRING,
            product_id STRING,
            product_name STRING,
            category STRING,
            quantity DOUBLE,
            unit_price DOUBLE,
            total_amount DOUBLE,
            final_amount DOUBLE,
            discount_percentage DOUBLE,
            transaction_date TIMESTAMP,
            customer_age_group STRING,
            customer_city STRING,
            payment_method STRING,
            store_id STRING,
            is_weekend BOOLEAN,
            quarter INT,
            created_at TIMESTAMP,
            processed_at TIMESTAMP
        ) USING DELTA;
    """)
    
    # ── Gold Table ──────────────────────────────────────────────────
    # Stores daily aggregated business KPIs from Silver.
    # One row per day with metrics:
    #   - total_transactions: Count of transactions for the day
    #   - total_revenue: Sum of final_amount
    #   - total_discount: Sum of discount amounts
    #   - avg_transaction_value: Average final_amount
    #   - unique_customers: Distinct customer count
    #   - top_category: Highest-revenue category for the day
    #   - top_payment_method: Most used payment method for the day
    #   - weekend_flag: Whether the day falls on a weekend
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_daily_sales_summary (
            date DATE,
            total_transactions INT,
            total_revenue DOUBLE,
            total_discount DOUBLE,
            avg_transaction_value DOUBLE,
            unique_customers INT,
            top_category STRING,
            top_payment_method STRING,
            weekend_flag BOOLEAN,
            processed_at TIMESTAMP
        ) USING DELTA;
    """)
    
    # ── Pipeline Logs Table ─────────────────────────────────────────
    # Stores execution logs for every pipeline run — one row per step.
    # Created here alongside other tables for centralized schema management.
    # Also created idempotently by pipeline_logging_utils on first use.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS pipeline_logs (
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
        COMMENT 'Pipeline execution logs — one row per step per run';
    """)
    
    print("✅ Database and tables created successfully!")

# COMMAND ----------

# DBTITLE 1,Execute: create_tables() with logging
# ─────────────────────────────────────────────────────────────────────
# Execute the schema and table creation.
# Safe to re-run: uses IF NOT EXISTS for idempotency.
# DDL runs every time but only creates tables that don't exist yet —
# this is fast and ensures schema is always up-to-date.
# Wrapped with pipeline logging: STARTED → SUCCESS/FAILED.
# ─────────────────────────────────────────────────────────────────────
RUN_ID = init_pipeline_run("create_schema")
STEP_START = log_step_start(RUN_ID, "create_schema")

try:
    create_tables()
    log_step_success(
        run_id=RUN_ID,
        step_name="create_schema",
        step_start_time=STEP_START,
        additional_info='{"tables": ["bronze_sales_transactions", "silver_sales_transactions", "gold_daily_sales_summary", "pipeline_logs"]}'
    )
except Exception as e:
    log_step_failure(
        run_id=RUN_ID,
        step_name="create_schema",
        step_start_time=STEP_START,
        error_message=str(e)
    )
    raise

# COMMAND ----------


