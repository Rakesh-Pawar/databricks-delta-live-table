# -- This file defines a sample transformation.
# -- Edit the sample below or add new transformations
# -- using "+ Add" in the file browser.

# -- CREATE MATERIALIZED VIEW sample_trips_ecom_bronze_ingestion_pipeline_bundle AS
# -- SELECT
# --     pickup_zip,
# --     fare_amount,
# --     trip_distance
# -- FROM samples.nyctaxi.trips

@dlt.table
def order_payment_bronze():
    return (
        spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.schemaLocation", "/Volumes/enterprise_dwh_dev/bronze/raw_ecom_files/schema/csv/_schemas") \
        .option("cloudFiles.ignoreCorruptFiles", "true") \
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
        .option("cloudFiles.maxFilesPerTrigger", "1") \
        .option("cloudFiles.maxBytesPer
    )