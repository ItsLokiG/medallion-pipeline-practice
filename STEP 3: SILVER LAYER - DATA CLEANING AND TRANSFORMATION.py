# Databricks notebook source
# DBTITLE 1,Notebook Title & Description
# MAGIC %md
# MAGIC # Step 3: Silver Layer — Data Cleaning and Transformation
# MAGIC
# MAGIC This notebook reads incremental data from the Bronze Delta table, applies data quality filters, derives new business columns, and loads the cleansed data into the Silver Delta table.
# MAGIC
# MAGIC **Key Transformations:**
# MAGIC * Filter out null transaction IDs and non-positive quantities/prices
# MAGIC * Derive `final_amount` (total_amount - discount)
# MAGIC * Derive `discount_percentage` (discount / total_amount * 100)
# MAGIC * Bucket `customer_age` into age groups (18-24, 25-34, etc.)
# MAGIC * Add `is_weekend` flag and `quarter` from transaction_date
# MAGIC * Deduplicate by transaction_id to prevent duplicate records
# MAGIC
# MAGIC **Input:** `medallian_catalog.{database_name}.bronze_sales_transactions`
# MAGIC **Output:** `medallian_catalog.{database_name}.silver_sales_transactions` (Delta, append mode)

# COMMAND ----------

# DBTITLE 1,Import: pipeline_logging_utils
# MAGIC %run ./pipeline_logging_utils

# COMMAND ----------

# DBTITLE 1,Import: PySpark types and functions
# ─────────────────────────────────────────────────────────────────────
# Import PySpark types and functions needed for Silver layer processing.
# Uses wildcard import for convenience — provides col, lit, when, coalesce,
# dayofweek, quarter, current_timestamp, and other transformation functions.
# ─────────────────────────────────────────────────────────────────────
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Widgets: database_name, run_id
# ─────────────────────────────────────────────────────────────────────
# Widget: database_name — UC schema where Silver table lives.
# Widget: run_id       — (Optional) Shared run_id for job orchestration.
# ─────────────────────────────────────────────────────────────────────
dbutils.widgets.text(name = "database_name", defaultValue="", label="Database Name")
DATABASE_NAME = dbutils.widgets.get("database_name")
dbutils.widgets.text("run_id", defaultValue="", label="Run ID (optional)")

# COMMAND ----------

# DBTITLE 1,get_last_processed_timestamp() — Silver watermark
# ─────────────────────────────────────────────────────────────────────
# Function: get_last_processed_timestamp
# Purpose: Queries the Silver Delta table for the maximum created_at
#          value to establish an incremental loading watermark.
#          Only Bronze records newer than this timestamp will be processed.
#
# Parameters:
#   table_name (str): Name of the Delta table to check (e.g. "silver_sales_transactions")
#
# Returns:
#   str: Timestamp string in "YYYY-MM-DD HH:MM:SS" format.
#        Returns "1900-01-01 00:00:00" if table is empty or doesn't exist.
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

# DBTITLE 1,silver_layer_processing() — Bronze to Silver
# ─────────────────────────────────────────────────────────────────────
# Function: silver_layer_processing
# Purpose: Reads incremental data from Bronze, applies data quality
#          filters, derives business columns, deduplicates, and loads
#          the cleansed data into the Silver Delta table.
#
# Parameters: None (uses widget: database_name)
#
# Processing Steps:
#   1. Get incremental watermark from Silver table
#   2. Read incremental Bronze records (created_at > watermark)
#   3. Apply data quality filters (non-null IDs, positive values)
#   4. Derive new columns: final_amount, discount_percentage,
#      customer_age_group, is_weekend, quarter
#   5. Deduplicate by transaction_id (keep first occurrence)
#   6. Write to Silver Delta table (append mode)
#
# Optimization Notes:
#   - count() is called once and stored in a variable to avoid
#     triggering duplicate Spark jobs (original code called it twice).
#   - Incremental filter uses lit().cast("timestamp") for proper
#     type comparison (string vs timestamp).
#   - Deduplication via dropDuplicates prevents duplicate records
#     if Bronze is re-processed.
# ─────────────────────────────────────────────────────────────────────
def silver_layer_processing():
    """Clean and transform data from Bronze to Silver layer."""
    
    print("🔄 Starting Silver layer processing...")
    
    # Step 1: Get incremental watermark from the Silver table
    last_processed = get_last_processed_timestamp("silver_sales_transactions")
    
    # Step 2: Read incremental Bronze records — only those newer than
    # the Silver watermark. Cast the string watermark to timestamp
    # for proper type comparison.
    df_bronze = spark.table(f"medallian_catalog.{DATABASE_NAME}.bronze_sales_transactions") \
        .filter(col("created_at") > lit(last_processed).cast("timestamp"))
    
    # Count once and reuse — avoids triggering a second Spark job
    record_count = df_bronze.count()
    if record_count == 0:
        print("✅ No new records to process in Silver layer")
        return {"record_count": 0, "watermark": last_processed}
    
    print(f"📊 Processing {record_count} records for Silver layer")
    
    # Steps 3 & 4: Data cleaning and transformation
    # - Data quality filters: remove null IDs and non-positive values
    # - Derive final_amount, discount_percentage, age groups, etc.
    df_silver = (
        df_bronze
        # Data quality filters: remove invalid records
        .filter(col("transaction_id").isNotNull())
        .filter(col("quantity") > 0)
        .filter(col("unit_price") > 0)
        .filter(col("total_amount") > 0)
        # Derive final_amount: total minus discount (handle NULL discount)
        .withColumn("final_amount", col("total_amount") - coalesce(col("discount"), lit(0)))
        # Derive discount_percentage: safe division with NULL handling
        .withColumn("discount_percentage", 
                   when(col("total_amount") > 0, (coalesce(col("discount"), lit(0)) / col("total_amount")) * 100)
                   .otherwise(0))
        # Bucket customer ages into standard groups for analytics
        .withColumn("customer_age_group",
                   when(col("customer_age") < 25, "18-24")
                   .when(col("customer_age") < 35, "25-34")
                   .when(col("customer_age") < 45, "35-44")
                   .when(col("customer_age") < 55, "45-54")
                   .when(col("customer_age") < 65, "55-64")
                   .otherwise("65+"))
        # is_weekend: dayofweek returns 1=Sunday, 7=Saturday in Spark
        .withColumn("is_weekend", dayofweek(col("transaction_date")).isin([1, 7]))
        # quarter: Extract calendar quarter (1-4) from transaction_date
        .withColumn("quarter", quarter(col("transaction_date")))
        # processed_at: Timestamp when the Silver record was created
        .withColumn("processed_at", current_timestamp())
        # Deduplicate by transaction_id to prevent duplicate Silver records
        .dropDuplicates(["transaction_id"])
        # Select columns in the Silver table schema order
        .select(
            "transaction_id", "customer_id", "product_id", "product_name", "category",
            "quantity", "unit_price", "total_amount", "final_amount", "discount_percentage",
            "transaction_date", "customer_age_group", "customer_city", "payment_method",
            "store_id", "is_weekend", "quarter", "created_at", "processed_at"
        )
    )
    
    # Step 6: Write to Silver Delta table in append mode
    df_silver.write \
        .format("DELTA") \
        .mode("append") \
        .saveAsTable(f"medallian_catalog.{DATABASE_NAME}.silver_sales_transactions")
    
    print("✅ Silver layer processing completed!")
    
    return {"record_count": record_count, "watermark": last_processed}

# COMMAND ----------

# DBTITLE 1,Execute: silver_layer_processing() with logging
# ─────────────────────────────────────────────────────────────────────
# Execute the Silver layer processing.
# Wrapped with pipeline logging: STARTED → SUCCESS/SKIPPED/FAILED.
# ─────────────────────────────────────────────────────────────────────
RUN_ID = init_pipeline_run("silver_layer")
STEP_START = log_step_start(RUN_ID, "silver_layer")

try:
    result = silver_layer_processing()
    if result["record_count"] == 0:
        log_step_skipped(RUN_ID, "silver_layer", STEP_START, "No new records to process")
    else:
        log_step_success(
            run_id=RUN_ID,
            step_name="silver_layer",
            step_start_time=STEP_START,
            record_count=result["record_count"],
            watermark_value=result["watermark"]
        )
except Exception as e:
    log_step_failure(
        run_id=RUN_ID,
        step_name="silver_layer",
        step_start_time=STEP_START,
        error_message=str(e)
    )
    raise