# Databricks notebook source
# DBTITLE 1,Notebook Title & Description
# MAGIC %md
# MAGIC # Step 2: Bronze Layer — Read from Source and Load to Bronze Table
# MAGIC
# MAGIC This notebook reads raw CSV files from the source volume, applies schema enforcement, adds metadata columns (file name, ingestion timestamp), performs type casting, and incrementally loads the data into the Bronze Delta table.
# MAGIC
# MAGIC **Key Features:**
# MAGIC * Schema-on-read with explicit type definitions (all source columns read as STRING, then cast)
# MAGIC * Incremental loading via `MAX(created_at)` watermark from the Bronze table
# MAGIC * Metadata capture: source file path and ingestion timestamp for auditability
# MAGIC * Safe type casting using `try_cast` to handle malformed values
# MAGIC
# MAGIC **Input:** CSV files at `{source_path}/*/*`
# MAGIC **Output:** `medallian_catalog.{database_name}.bronze_sales_transactions` (Delta, append mode)

# COMMAND ----------

# DBTITLE 1,Import: pipeline_logging_utils
# MAGIC %run ./pipeline_logging_utils

# COMMAND ----------

# DBTITLE 1,Import: PySpark types and functions
# ─────────────────────────────────────────────────────────────────────
# Import PySpark functions needed for Bronze layer processing.
#   - try_to_timestamp / to_timestamp: Safe timestamp parsing from strings
#   - col: Column reference for DataFrame operations
#   - current_timestamp: Capture ingestion time for auditability
#   - lit: Create literal columns (used for watermark comparison)
#   - expr: Execute SQL expressions within DataFrame API
# ─────────────────────────────────────────────────────────────────────
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import try_to_timestamp, to_timestamp, col, current_timestamp, lit, expr

# COMMAND ----------

# DBTITLE 1,Widgets: database_name, source_path, run_id
# ─────────────────────────────────────────────────────────────────────
# Widget: database_name — UC schema where Bronze table lives.
# Widget: source_path  — Volume path containing source CSV files.
# Widget: run_id       — (Optional) Shared run_id for job orchestration.
# ─────────────────────────────────────────────────────────────────────
dbutils.widgets.text(name = "database_name", defaultValue="", label="Database Name")
DATABASE_NAME = dbutils.widgets.get("database_name")
dbutils.widgets.text("source_path", defaultValue="", label="Source Path")
SOURCE_PATH = dbutils.widgets.get("source_path")
dbutils.widgets.text("run_id", defaultValue="", label="Run ID (optional)")

# COMMAND ----------

# DBTITLE 1,Debug: Print source path
# Debug: Print the configured source path to verify the widget value.
# This is useful when debugging parameter passing from orchestrator notebooks.
print(SOURCE_PATH)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, expr, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ─────────────────────────────────────────────────────────────────────
# Function: get_last_processed_timestamp
# Purpose: Queries the Bronze Delta table for the maximum created_at
#          value to establish an incremental loading watermark.
#          Only records newer than this timestamp will be loaded.
#
# Parameters:
#   table_name (str): Name of the Delta table to check (e.g. "bronze_sales_transactions")
#
# Returns:
#   str: Timestamp string in "YYYY-MM-DD HH:MM:SS" format.
#        Returns "1900-01-01 00:00:00" if the table is empty or doesn't exist
#        (ensures all records are loaded on the first run).
#
# Optimization Notes:
#   - Uses collect()[0] to retrieve a single scalar value (no data movement).
#   - Catches all exceptions (table not found, empty table) and returns a
#     default watermark to ensure first-run completeness.
# ─────────────────────────────────────────────────────────────────────
def get_last_processed_timestamp(table_name):
    """Get the last processed timestamp for incremental loading"""
    try:
        last_timestamp = spark.sql(f"""
            SELECT MAX(created_at) as max_timestamp 
            FROM medallian_catalog.{DATABASE_NAME}.{table_name}
        """).collect()[0].max_timestamp
        
        if last_timestamp is None:
            return "1900-01-01 00:00:00"
        return last_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        # Table doesn't exist yet or query failed — return earliest timestamp
        # to ensure all source records are loaded on the first run.
        return "1900-01-01 00:00:00"


def bronze_layer_processing():
    """Read CSV files and load to Bronze layer with incremental logic."""
    
    print("🔄 Starting Bronze layer processing...")
    
    # Step 1: Get the incremental watermark from the existing Bronze table
    last_processed = get_last_processed_timestamp("bronze_sales_transactions")
    print(f"📅 Last processed timestamp: {last_processed}")
    
    # Step 2: Define explicit CSV schema — all columns read as STRING
    #         to allow safe type casting downstream. This prevents schema
    #         inference issues with mixed-type CSV columns.
    csv_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("customer_age", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("created_at", StringType(), True)
    ])
    
    # Step 3: Read all CSV files from the source path recursively.
    # The glob pattern {SOURCE_PATH}*/* matches all date-stamped subfolders.
    df_raw = spark.read \
        .option("header", "true") \
        .schema(csv_schema) \
        .csv(f"{SOURCE_PATH}*/*")


    
   

    
    # Step 4 & 5: Transform — add metadata columns and cast types.
    #   - file_name: Captured from Spark's built-in _metadata.file_path for auditability
    #   - ingestion_timestamp: Current timestamp for audit trail
    #   - try_cast: Safely converts strings to numeric; returns NULL for malformed values
    #   - try_to_timestamp: Safe timestamp parsing with explicit format
    df_bronze = (df_raw
        .selectExpr("*", "_metadata.file_path as file_name")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("quantity", expr("try_cast(quantity AS DOUBLE)"))
        .withColumn("unit_price", expr("try_cast(unit_price AS DOUBLE)"))
        .withColumn("total_amount", expr("try_cast(total_amount AS DOUBLE)"))
        .withColumn("discount", expr("try_cast(discount AS DOUBLE)"))
        .withColumn("customer_age", expr("try_cast(customer_age AS INT)"))
        .withColumn("transaction_date", expr("try_to_timestamp(transaction_date, 'yyyy-MM-dd HH:mm:ss')"))
        .withColumn("created_at", expr("try_to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss')"))
    )

    # Step 6: Incremental filter — only keep records newer than the watermark.
    # Cast the watermark string to timestamp for proper comparison.
    df_incremental = df_bronze.filter(
        col("created_at") > lit(last_processed).cast("timestamp")
    )
    
    # Count incremental records once and reuse (avoids duplicate Spark jobs)
    record_count = df_incremental.count()
    if record_count == 0:
        print("✅ No new records to process")
        return {"record_count": 0, "watermark": last_processed}
    
    print(f"📊 Processing {record_count} new records")
    
    # Step 7: Write to Bronze Delta table in append mode.
    # Append preserves existing records and adds new ones for incremental loading.
    (df_incremental.write
        .format("Delta")
        .mode("append")
        .saveAsTable(f"medallian_catalog.{DATABASE_NAME}.bronze_sales_transactions"))
    
    print("✅ Bronze layer processing completed!")
    
    return {"record_count": record_count, "watermark": last_processed}


# COMMAND ----------

# DBTITLE 1,Execute: bronze_layer_processing() with logging
# ─────────────────────────────────────────────────────────────────────
# Execute the Bronze layer processing function.
# Wrapped with pipeline logging: STARTED → SUCCESS/SKIPPED/FAILED.
# ─────────────────────────────────────────────────────────────────────
RUN_ID = init_pipeline_run("bronze_layer")
STEP_START = log_step_start(RUN_ID, "bronze_layer")

try:
    result = bronze_layer_processing()
    if result["record_count"] == 0:
        log_step_skipped(RUN_ID, "bronze_layer", STEP_START, "No new records to process")
    else:
        log_step_success(
            run_id=RUN_ID,
            step_name="bronze_layer",
            step_start_time=STEP_START,
            record_count=result["record_count"],
            watermark_value=result["watermark"]
        )
except Exception as e:
    log_step_failure(
        run_id=RUN_ID,
        step_name="bronze_layer",
        step_start_time=STEP_START,
        error_message=str(e)
    )
    raise

# COMMAND ----------

# DBTITLE 1,Verify: Bronze table record count
# MAGIC %sql
# MAGIC -- ────────────────────────────────────────────────────────────────────
# MAGIC -- Verification Query: Count total records in the Bronze table.
# MAGIC -- This confirms that the incremental load worked correctly.
# MAGIC -- Replace 'table_for_all' with the actual database_name parameter value.
# MAGIC -- ────────────────────────────────────────────────────────────────────
# MAGIC select count(*) from medallian_catalog.table_for_all.bronze_sales_transactions

# COMMAND ----------

# DBTITLE 1,Debug: Check Bronze watermark
# Debug: Check the current watermark value for the Bronze table.
# This returns the latest created_at timestamp in the Bronze table.
get_last_processed_timestamp("bronze_sales_transactions")