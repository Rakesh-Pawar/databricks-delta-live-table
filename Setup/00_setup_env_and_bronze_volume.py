# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %md
# MAGIC # 00 - Setup environment and bronze volume
# MAGIC This notebook creates the catalog, schemas, bronze volume, folder structure, session configuration values, and a reusable key-value config table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

# DBTITLE 1,Define Data Lake Paths and Environment Configurations
catalog_name = "enterprise_dwh_dev"
schema_bronze = "bronze"
schema_silver = "silver"
schema_gold = "gold"
volume_name = "raw_ecom_files"
config_table_name = "env_config"

base_volume_path = f"/Volumes/{catalog_name}/{schema_bronze}/{volume_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create catalog and schemas

# COMMAND ----------

# DBTITLE 1,Initialize Catalog and Create Bronze Silver Gold Schema ...
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_bronze}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_gold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create bronze volume directly inside bronze schema

# COMMAND ----------

# DBTITLE 1,Create Volume in Data Lake Bronze Schema
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_bronze}.{volume_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create folder structure inside the bronze volume
# MAGIC Upload your files one by one into the relevant folder after running this notebook.

# COMMAND ----------

# DBTITLE 1,Create Folders in Data Lake Volume Structure
folders = [
    "bulk_upload",
    "csv",
    "excel",
    "json",
    "rest_api",
    "sftp",
    "archive",
    "checkpoint",
    "schema"
]

for folder in folders:
    if folder not in dbutils.fs.ls(base_volume_path):
        dbutils.fs.mkdirs(f"{base_volume_path}/{folder}")
    else:
        print("Folder already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build path configuration map

# COMMAND ----------

path_config = {
    "CATALOG_NAME": catalog_name,
    "BRONZE_SCHEMA": f"{catalog_name}.{schema_bronze}",
    "SILVER_SCHEMA": f"{catalog_name}.{schema_silver}",
    "GOLD_SCHEMA": f"{catalog_name}.{schema_gold}",
    "RAW_VOLUME_NAME": volume_name,
    "RAW_BASE_PATH": base_volume_path,
    "RAW_BULK_UPLOAD_PATH": f"{base_volume_path}/bulk_upload",
    "RAW_CSV_PATH": f"{base_volume_path}/csv",
    "RAW_EXCEL_PATH": f"{base_volume_path}/excel",
    "RAW_JSON_PATH": f"{base_volume_path}/json",
    "RAW_REST_API_PATH": f"{base_volume_path}/rest_api",
    "RAW_SFTP_PATH": f"{base_volume_path}/sftp",
    "RAW_ARCHIVE_PATH": f"{base_volume_path}/archive",
    "RAW_CHECKPOINT_PATH": f"{base_volume_path}/checkpoint",
    "RAW_SCHEMA_PATH": f"{base_volume_path}/schema"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Store all paths in the current session using python 
# MAGIC These values can be fetched dynamically in later notebooks.

# COMMAND ----------

# DBTITLE 1,Store Configuration in Environment Variables
import os

for k, v in path_config.items():
    os.environ[f"PROJECT_{k}"] = v

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create config table for reusable key-value storage

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_bronze}.{config_table_name} (
    config_key STRING,
    config_value STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Load path configuration into the config table

# COMMAND ----------

config_rows = [(k, v) for k, v in path_config.items()]
config_df = spark.createDataFrame(config_rows, ["config_key", "config_value"])
config_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_bronze}.{config_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verify created folders and config values

# COMMAND ----------

print("Base volume path:", base_volume_path)
print("\nConfigured folders:")
for folder in folders:
    print(f"- {base_volume_path}/{folder}")

print("\nConfig table preview:")
display(spark.table(f"{catalog_name}.{schema_bronze}.{config_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Suggested upload mapping
# MAGIC Upload your files one by one into these folders:
# MAGIC - bulk_upload -> general/manual files
# MAGIC - csv -> CSV source files
# MAGIC - excel -> XLSX/Excel files
# MAGIC - json -> JSON files
# MAGIC - rest_api -> API-derived files
# MAGIC - sftp -> SFTP-derived files
# MAGIC
# MAGIC Example usage in later notebooks:
# MAGIC ```python
# MAGIC raw_csv_path = spark.conf.get("project.raw_csv_path")
# MAGIC env_map = {r['config_key']: r['config_value'] for r in spark.table("enterprise_dwh_dev.bronze.env_config").collect()}
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Pipeline Summary - Created Lakeflow Pipelines
# MAGIC %md
# MAGIC ## ✓ Lakeflow Spark Declarative Pipelines Created
# MAGIC
# MAGIC All pipeline notebooks have been created in the **Pipelines** folder with comprehensive data quality checks.
# MAGIC
# MAGIC ### Master Pipeline (Recommended)
# MAGIC * [00_master_bronze_pipeline_dlt](#notebook-3178718354831059) - All 6 sources combined
# MAGIC
# MAGIC ### Individual Source Pipelines
# MAGIC * [01_bronze_sftp_customers_dlt](#notebook-3178718354831053) - 99,441 customers
# MAGIC * [02_bronze_rest_api_categories_dlt](#notebook-3178718354831054) - 71 product categories
# MAGIC * [03_bronze_json_users_dlt](#notebook-3178718354831055) - 7 users
# MAGIC * [04_bronze_excel_reviews_dlt](#notebook-3178718354831056) - 104,162 reviews
# MAGIC * [05_bronze_csv_payments_dlt](#notebook-3178718354831057) - 103,886 payments
# MAGIC * [06_bronze_bulk_geolocation_dlt](#notebook-3178718354831058) - 1,000,163 locations
# MAGIC
# MAGIC ### Documentation
# MAGIC * [README_Pipeline_Setup](#notebook-3178718354831060) - Complete setup guide
# MAGIC
# MAGIC ### Next Steps
# MAGIC 1. Navigate to **Workflows > Lakeflow Pipelines**
# MAGIC 2. Click **Create Pipeline**
# MAGIC 3. Select the master pipeline notebook or individual pipelines
# MAGIC 4. Configure target as `enterprise_dwh_dev.bronze`
# MAGIC 5. Start the pipeline to begin ingestion with auto quality checks
