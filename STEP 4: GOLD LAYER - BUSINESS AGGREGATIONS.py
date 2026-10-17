# Databricks notebook source
# DBTITLE 1,Notebook Title & Description
# MAGIC %md
# MAGIC # Step 4: Gold Layer — Business Aggregations
# MAGIC
# MAGIC This notebook reads cleansed data from the Silver Delta table, computes daily business KPIs, and loads aggregated results into the Gold Delta table. It processes one day at a time (the latest unprocessed date) to support incremental daily reporting.
# MAGIC
# MAGIC **Key Metrics Computed:**
# MAGIC * `total_transactions`: Count of transactions for the day
# MAGIC * `total_revenue`: Sum of final_amount (after discount)
# MAGIC * `total_discount`: Sum of discount dollar amounts
# MAGIC * `avg_transaction_value`: Average final_amount
# MAGIC * `unique_customers`: Distinct customer count
# MAGIC * `top_category`: Highest-revenue product category
# MAGIC * `top_payment_method`: Most used payment method
# MAGIC * `weekend_flag`: Whether the date falls on a weekend
# MAGIC
# MAGIC **Input:** `medallian_catalog.{database_name}.silver_sales_transactions`
# MAGIC **Output:** `medallian_catalog.{database_name}.gold_daily_sales_summary` (Delta, append mode)

# COMMAND ----------

# DBTITLE 1,Import: pipeline_logging_utils
# MAGIC %run ./pipeline_logging_utils

# COMMAND ----------

# DBTITLE 1,Import: PySpark types and functions
# ─────────────────────────────────────────────────────────────────────
# Import PySpark types and functions needed for Gold layer processing.
# Uses wildcard import for convenience — provides col, current_timestamp,
# and other functions used in the aggregation SQL and DataFrame operations.
# ─────────────────────────────────────────────────────────────────────
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Widgets: database_name, run_id
# ─────────────────────────────────────────────────────────────────────
# Widget: database_name — UC schema where Gold table lives.
# Widget: run_id       — (Optional) Shared run_id for job orchestration.
# ─────────────────────────────────────────────────────────────────────
dbutils.widgets.text(name = "database_name", defaultValue="", label="Database Name")
DATABASE_NAME = dbutils.widgets.get("database_name")
dbutils.widgets.text("run_id", defaultValue="", label="Run ID (optional)")


# COMMAND ----------

# DBTITLE 1,get_last_processed_timestamp() — Gold reference
# ─────────────────────────────────────────────────────────────────────
# Function: get_last_processed_timestamp
# Purpose: Queries the Gold Delta table for the maximum created_at
#          value (unused in Gold processing but available for reference).
#          Gold processing uses date-based idempotency instead.
#
# Parameters:
#   table_name (str): Name of the Delta table to check.
#
# Returns:
#   str: Timestamp string or "1900-01-01 00:00:00" if empty/not found.
# ─────────────────────────────────────────────────────────────────────
def get_last_processed_timestamp(table_name):
    """Get the last processed timestamp for incremental loading."""
    try:
        last_timestamp = spark.sql(f"""
            SELECT MAX(created_at) as max_timestamp 
            FROM medallian_catalog.{DATABASE_NAME}.{table_name}
        """).collect()[0]['max_timestamp']
        
        if last_timestamp is None:
            return "1900-01-01 00:00:00"
        return last_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # Table doesn't exist yet — return earliest timestamp for first run
        return "1900-01-01 00:00:00"

# COMMAND ----------

# DBTITLE 1,gold_layer_processing() — Silver to Gold
# ─────────────────────────────────────────────────────────────────────
# Function: gold_layer_processing
# Purpose: Creates daily business aggregations from the Silver table
#          and loads them into the Gold table. Processes one day at a
#          time (the latest date in Silver not yet in Gold).
#
# Parameters: None (uses widget: database_name)
#
# Processing Steps:
#   1. Find the latest date in the Silver table
#   2. Check if that date is already in Gold (idempotency check)
#   3. If not processed, compute daily aggregations using SQL CTEs:
#      a. daily_stats: Core metrics (transactions, revenue, discount, etc.)
#      b. top_category: Highest-revenue category (using ROW_NUMBER)
#      c. top_payment: Most used payment method (using ROW_NUMBER)
#   4. Cast columns to match Gold table schema
#   5. Append to Gold Delta table
#
# Optimization Notes:
#   - Uses SQL CTEs for complex aggregations — Spark SQL optimizer
#     handles this efficiently in a single pass.
#   - ROW_NUMBER() window function identifies top category/payment
#     without needing separate DataFrames or joins.
#   - mergeSchema option removed — schema should be fixed for
#     production stability. The Gold table schema is defined in Step 1.
#   - Date-based idempotency: if the date is already in Gold, the
#     function skips processing (prevents duplicate daily summaries).
# ─────────────────────────────────────────────────────────────────────
def gold_layer_processing():
    """Create business aggregations and store in Gold layer."""
    
    print("🔄 Starting Gold layer processing...")
    
    # Step 1: Get the latest transaction date from the Silver table
    latest_date_result = spark.sql(f"""
        SELECT MAX(date(transaction_date)) as max_date 
        FROM medallian_catalog.{DATABASE_NAME}.silver_sales_transactions
    """).collect()
    
    # Handle empty Silver table gracefully
    if not latest_date_result or latest_date_result[0]['max_date'] is None:
        print("⚠️ No data available in Silver layer")
        return {"record_count": 0, "latest_date": None}
    
    latest_date = latest_date_result[0]['max_date']
    
    # Step 2: Idempotency check — skip if this date is already in Gold
    existing_record = spark.sql(f"""
        SELECT COUNT(*) as count 
        FROM medallian_catalog.{DATABASE_NAME}.gold_daily_sales_summary 
        WHERE date = '{latest_date}'
    """).collect()[0]['count']
    
    if existing_record > 0:
        print(f"✅ Date {latest_date} already processed in Gold layer")
        return {"record_count": 0, "latest_date": str(latest_date), "skipped": True}
    
    # Step 3: Create daily aggregations using SQL CTEs
    #   - daily_stats: Core daily metrics (transactions, revenue, discount, customers)
    #   - top_category: Category with highest revenue (ROW_NUMBER = 1)
    #   - top_payment: Payment method with highest count (ROW_NUMBER = 1)
    # All CTEs filter for the latest_date only for efficient single-day processing.
    df_newgold = spark.sql(f"""
        WITH daily_stats AS (
            SELECT 
                date(transaction_date) as date,
                COUNT(*) as total_transactions,
                SUM(final_amount) as total_revenue,
                SUM(COALESCE(discount_percentage * total_amount / 100, 0)) as total_discount,
                AVG(final_amount) as avg_transaction_value,
                COUNT(DISTINCT customer_id) as unique_customers,
                is_weekend as weekend_flag
            FROM medallian_catalog.{DATABASE_NAME}.silver_sales_transactions
            WHERE date(transaction_date) = '{latest_date}'
            GROUP BY date(transaction_date), is_weekend
        ),
        top_category AS (
            SELECT 
                date(transaction_date) as date,
                category,
                SUM(final_amount) as category_revenue,
                ROW_NUMBER() OVER (PARTITION BY date(transaction_date) ORDER BY SUM(final_amount) DESC) as rn
            FROM medallian_catalog.{DATABASE_NAME}.silver_sales_transactions
            WHERE date(transaction_date) = '{latest_date}'
            GROUP BY date(transaction_date), category
        ),
        top_payment AS (
            SELECT 
                date(transaction_date) as date,
                payment_method,
                COUNT(*) as payment_count,
                ROW_NUMBER() OVER (PARTITION BY date(transaction_date) ORDER BY COUNT(*) DESC) as rn
            FROM medallian_catalog.{DATABASE_NAME}.silver_sales_transactions
            WHERE date(transaction_date) = '{latest_date}'
            GROUP BY date(transaction_date), payment_method
        )
        SELECT 
            ds.date,
            ds.total_transactions,
            ds.total_revenue,
            ds.total_discount,
            ds.avg_transaction_value,
            ds.unique_customers,
            tc.category as top_category,
            tp.payment_method as top_payment_method,
            ds.weekend_flag,
            current_timestamp() as processed_at
        FROM daily_stats ds
        LEFT JOIN top_category tc ON ds.date = tc.date AND tc.rn = 1
        LEFT JOIN top_payment tp ON ds.date = tp.date AND tp.rn = 1
    """)

    # Step 4: Cast columns to match the Gold table schema exactly.
    # This ensures type compatibility when appending to the Delta table.
    df_gold = df_newgold \
        .withColumn("total_transactions", col("total_transactions").cast("int")) \
        .withColumn("total_revenue", col("total_revenue").cast("double")) \
        .withColumn("total_discount", col("total_discount").cast("double")) \
        .withColumn("avg_transaction_value", col("avg_transaction_value").cast("double")) \
        .withColumn("unique_customers", col("unique_customers").cast("int"))
    
    # Step 5: Write to Gold Delta table in append mode.
    # mergeSchema removed — schema is fixed and defined in Step 1.
    df_gold.write \
        .format("DELTA") \
        .mode("append") \
        .saveAsTable(f"medallian_catalog.{DATABASE_NAME}.gold_daily_sales_summary")
    
    print(f"✅ Gold layer processing completed for date: {latest_date}")
    
    return {"record_count": 1, "latest_date": str(latest_date), "skipped": False}

# COMMAND ----------

# DBTITLE 1,Execute: gold_layer_processing() with logging
# ─────────────────────────────────────────────────────────────────────
# Execute the Gold layer processing.
# Wrapped with pipeline logging: STARTED → SUCCESS/SKIPPED/FAILED.
# ─────────────────────────────────────────────────────────────────────
RUN_ID = init_pipeline_run("gold_layer")
STEP_START = log_step_start(RUN_ID, "gold_layer")

try:
    result = gold_layer_processing()
    if result["record_count"] == 0:
        skip_reason = "No data in Silver" if result.get("latest_date") is None else f"Date {result['latest_date']} already processed"
        log_step_skipped(RUN_ID, "gold_layer", STEP_START, skip_reason)
    else:
        log_step_success(
            run_id=RUN_ID,
            step_name="gold_layer",
            step_start_time=STEP_START,
            record_count=result["record_count"],
            additional_info='{"latest_date": "' + result.get("latest_date", "") + '"}'
        )
except Exception as e:
    log_step_failure(
        run_id=RUN_ID,
        step_name="gold_layer",
        step_start_time=STEP_START,
        error_message=str(e)
    )
    raise