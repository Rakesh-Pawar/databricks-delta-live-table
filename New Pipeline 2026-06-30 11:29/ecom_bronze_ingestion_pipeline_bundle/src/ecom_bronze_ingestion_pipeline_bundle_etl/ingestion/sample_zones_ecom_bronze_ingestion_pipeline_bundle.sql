-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

-- CREATE MATERIALIZED VIEW sample_zones_ecom_bronze_ingestion_pipeline_bundle AS
-- SELECT
--     pickup_zip,
--     SUM(fare_amount) AS total_fare
-- FROM sample_trips_ecom_bronze_ingestion_pipeline_bundle
-- GROUP BY pickup_zip
