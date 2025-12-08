----- CREATING WAREHOUSE AND DATABASE IN SNOWFLAKE --------

CREATE OR REPLACE DATABASE WAREHOUSE_WEATHER_DATA_DB;

CREATE OR REPLACE WAREHOUSE WAREHOUSE_WEATHER_DATA_DB
  WITH WAREHOUSE_SIZE = 'XSMALL'
  INITIALLY_SUSPENDED = TRUE;

-------------- CREATING CATALOG INTEGRATION ----------------

CREATE OR REPLACE CATALOG INTEGRATION glueCatalog_WarehouseWeatherData
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'warehouse_weather_data'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxxxxx:role/snowflake_service_role'
  GLUE_CATALOG_ID = 'xxxxxxxxxxxx'
  GLUE_REGION = 'us-east-1'
  ENABLED = TRUE
  REFRESH_INTERVAL_SECONDS = 600;


DESCRIBE CATALOG INTEGRATION glueCatalog_WarehouseWeatherData;

-------------------- CREATING EXTERNAL VOLUME TO S3 -------------------------

CREATE OR REPLACE EXTERNAL VOLUME warehouse_weather_data_vol
   STORAGE_LOCATIONS =
        (
            (
               NAME = 's3_warehouse_weather_data.db'
               STORAGE_PROVIDER= 'S3'
               STORAGE_BASE_URL = 's3://weather-datalake-projects/pipeline-weather-data/warehouse/warehouse_weather_data.db/'
               STORAGE_AWS_ROLE_ARN='arn:aws:iam::xxxxxxxxxxxx:role/snowflake_service_role'
            )
        )
    ALLOW_WRITES=FALSE
    ;

DESC EXTERNAL VOLUME warehouse_weather_data_vol;

