# Databricks notebook source
# DBTITLE 1,Bronze Layer - SFTP Customers Pipeline
# MAGIC %md
# MAGIC # Bronze Layer: SFTP Customers Data Pipeline
# MAGIC
# MAGIC This pipeline ingests customer data from SFTP source with data quality checks.
# MAGIC
# MAGIC **Source**: `/Volumes/enterprise_dwh_dev/bronze/raw_ecom_files/sftp/olist_customers_dataset.csv`
# MAGIC
# MAGIC **Target**: `enterprise_dwh_dev.bronze.customers_bronze`
# MAGIC
# MAGIC **Data Quality Checks**:
# MAGIC - Customer ID must not be null
# MAGIC - Zip code must be valid (positive integer)
# MAGIC - State code must be 2 characters
# MAGIC - City must not be null

# COMMAND ----------

# DBTITLE 1,Import Libraries
import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name

# COMMAND ----------

# DBTITLE 1,Bronze: Raw Customers (Autoloader Streaming)
@dlt.table(
    name="customers_bronze",
    comment="Bronze layer: Raw customer data from SFTP source",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "customer_id"
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_unique_id", "customer_unique_id IS NOT NULL")
@dlt.expect("valid_zip_code", "customer_zip_code_prefix > 0")
@dlt.expect("valid_state_code", "LENGTH(customer_state) = 2")
@dlt.expect_or_drop("valid_city", "customer_city IS NOT NULL")
def customers_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/enterprise_dwh_dev/bronze/raw_ecom_files/schema/sftp_customers")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/enterprise_dwh_dev/bronze/raw_ecom_files/sftp/")
        .select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix").cast("int"),
            col("customer_city"),
            col("customer_state"),
            current_timestamp().alias("ingestion_timestamp"),
            input_file_name().alias("source_file")
        )
    )

# COMMAND ----------


