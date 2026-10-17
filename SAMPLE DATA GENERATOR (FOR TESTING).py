# Databricks notebook source
# DBTITLE 1,Notebook Title & Description
# MAGIC %md
# MAGIC # Sample Data Generator (For Testing)
# MAGIC
# MAGIC This notebook generates synthetic sales transaction data in CSV format and writes it to a Unity Catalog volume. It is used to simulate source data for testing the medallion architecture pipeline (Bronze → Silver → Gold). The generated data mimics real-world e-commerce transactions with realistic fields such as customer IDs, product categories, payment methods, and store locations.

# COMMAND ----------

# DBTITLE 1,Import: pipeline_logging_utils
# MAGIC %run ./pipeline_logging_utils

# COMMAND ----------

# DBTITLE 1,Widgets: source_path, database_name, run_id
# ─────────────────────────────────────────────────────────────────────
# Widget: source_path   — Volume path where generated CSVs will be saved.
# Widget: database_name — UC schema for the pipeline_logs table.
# Widget: run_id        — (Optional) Shared run_id for job orchestration.
#                         If empty, a new UUID is generated.
# ─────────────────────────────────────────────────────────────────────
dbutils.widgets.text("source_path", defaultValue="", label="Source Path")
SOURCE_PATH = dbutils.widgets.get("source_path")
dbutils.widgets.text("database_name", defaultValue="", label="Database Name")
DATABASE_NAME = dbutils.widgets.get("database_name")
dbutils.widgets.text("run_id", defaultValue="", label="Run ID (optional)")

# COMMAND ----------

# DBTITLE 1,generate_sample_data() — Synthetic CSV Generator
# ─────────────────────────────────────────────────────────────────────
# Function: generate_sample_data
# Purpose: Generates synthetic sales transaction records and saves them
#          as CSV files to the source_path volume. Each run creates a
#          date-stamped folder (e.g. sample_sales_2025_10_31) to simulate
#          daily batch file drops from an upstream source system.
#
# Parameters:
#   num_records (int): Number of synthetic transaction records to generate.
#                      Default is 1000; use a smaller number (e.g. 20)
#                      for quick testing.
#
# Output:
#   CSV file at {SOURCE_PATH}/sample_sales_{YYYY_MM_DD}/ containing
#   num_records rows with 15 columns matching the Bronze layer schema.
#
# Optimization Notes:
#   - coalesce(1) consolidates output to a single CSV part file for
#     easier downstream reading (avoids multiple small files).
#   - Imports are placed at function scope to keep this utility self-contained.
# ─────────────────────────────────────────────────────────────────────
def generate_sample_data(num_records=1000):
    """Generate sample CSV data for testing the pipeline.

    Creates synthetic sales transaction records with realistic distributions
    and writes them as a single CSV file to the configured source volume.
    """
    
    # Import random and datetime utilities at function scope for modularity
    from random import randint, choice, uniform
    from datetime import datetime, timedelta
    
    # Reference data for generating realistic synthetic values
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"]
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Cash", "Apple Pay"]
    stores = ["ST001", "ST002", "ST003", "ST004", "ST005"]
    
    sample_data = []
    # Use yesterday's date as the base to simulate a daily batch file drop
    base_date = datetime.now() - timedelta(days=1)  # Yesterday's data
    
    # Generate each transaction record with randomized but realistic values
    for i in range(num_records):
        # Random timestamp within yesterday (spreads across 24 hours)
        transaction_date = base_date + timedelta(
            hours=randint(0, 23), 
            minutes=randint(0, 59), 
            seconds=randint(0, 59)
        )
        
        # Calculate monetary fields with realistic ranges
        unit_price = round(uniform(10, 500), 2)
        quantity = randint(1, 5)
        total_amount = round(unit_price * quantity, 2)
        discount = round(uniform(0, total_amount * 0.2), 2)  # Max 20% discount
        
        # Build the record dictionary matching the Bronze layer schema exactly
        record = {
            "transaction_id": f"TXN_{i+1:06d}",           # Unique zero-padded ID
            "customer_id": f"CUST_{randint(1000, 9999)}",  # Random customer reference
            "product_id": f"PROD_{randint(100, 999)}",     # Random product reference
            "product_name": f"Product {randint(1, 100)}",  # Generic product name
            "category": choice(categories),                # Random product category
            "quantity": quantity,                           # 1-5 items per transaction
            "unit_price": unit_price,                        # $10 - $500 per unit
            "total_amount": total_amount,                   # unit_price * quantity
            "discount": discount,                           # Up to 20% of total_amount
            "transaction_date": transaction_date.strftime("%Y-%m-%d %H:%M:%S"),
            "customer_age": randint(18, 75),                # Adult age range
            "customer_city": choice(cities),               # Random US city
            "payment_method": choice(payment_methods),     # Random payment type
            "store_id": choice(stores),                    # Random store code
            "created_at": transaction_date.strftime("%Y-%m-%d %H:%M:%S")  # Same as transaction_date
        }
        sample_data.append(record)
    
    # Define the column order to match the Bronze layer table schema exactly
    desired_order = [
        "transaction_id", "customer_id", "product_id", "product_name", "category",
        "quantity", "unit_price", "total_amount", "discount", "transaction_date",
        "customer_age", "customer_city", "payment_method", "store_id", "created_at"
    ]

    # Convert the Python list of dicts to a Spark DataFrame for efficient CSV writing
    df_sample = spark.createDataFrame(sample_data).select(desired_order)
    
    # Write as a single CSV file (coalesce(1)) with header row.
    # The folder name is date-stamped to simulate daily batch file drops.
    df_sample.coalesce(1).write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(f"{SOURCE_PATH}/sample_sales_{base_date.strftime('%Y_%m_%d')}")
    
    print(f"✅ Generated {num_records} sample records")
    print(f"📁 Data saved to {SOURCE_PATH}")

# COMMAND ----------

# DBTITLE 1,Execute: generate 20 sample records with logging
# ─────────────────────────────────────────────────────────────────────
# Execute the data generator with a small batch (20 records) for
# quick testing. For production-like load testing, increase to 1000+.
# Wrapped with pipeline logging: STARTED → SUCCESS/FAILED.
# ─────────────────────────────────────────────────────────────────────
RUN_ID = init_pipeline_run("generate_sample_data")
STEP_START = log_step_start(RUN_ID, "generate_sample_data")

try:
    generate_sample_data(20)
    log_step_success(
        run_id=RUN_ID,
        step_name="generate_sample_data",
        step_start_time=STEP_START,
        record_count=20,
        additional_info='{"source_path": "' + SOURCE_PATH + '"}'
    )
except Exception as e:
    log_step_failure(
        run_id=RUN_ID,
        step_name="generate_sample_data",
        step_start_time=STEP_START,
        error_message=str(e)
    )
    raise

# COMMAND ----------

