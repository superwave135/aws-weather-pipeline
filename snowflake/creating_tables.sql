
------- CREATING TABLE CONNECTIONS TO SNOWFLAKE --------
                                                                                                                                                                            
USE WAREHOUSE_WEATHER_DATA_DB;
USE WAREHOUSE_WEATHER_DATA_DB.PUBLIC;
USE WAREHOUSE WAREHOUSE_WEATHER_DATA_DB;


CREATE OR REPLACE ICEBERG TABLE dim_locations
    EXTERNAL_VOLUME='warehouse_weather_data_vol'
    CATALOG='glueCatalog_WarehouseWeatherData'
    CATALOG_TABLE_NAME='dim_locations'
    AUTO_REFRESH = TRUE;


CREATE OR REPLACE ICEBERG TABLE weather_actual_data_timeseries
    EXTERNAL_VOLUME='warehouse_weather_data_vol'
    CATALOG='glueCatalog_WarehouseWeatherData'
    CATALOG_TABLE_NAME='weather_actual_data_timeseries'
    AUTO_REFRESH = TRUE;

    
CREATE OR REPLACE ICEBERG TABLE weather_actual_data_timeseries_agg
    EXTERNAL_VOLUME='warehouse_weather_data_vol'
    CATALOG='glueCatalog_WarehouseWeatherData'
    CATALOG_TABLE_NAME='weather_actual_data_timeseries_agg'
    AUTO_REFRESH = TRUE;

    
CREATE OR REPLACE ICEBERG TABLE weather_forecast_data_timeseries
    EXTERNAL_VOLUME='warehouse_weather_data_vol'
    CATALOG='glueCatalog_WarehouseWeatherData'
    CATALOG_TABLE_NAME='weather_forecast_data_timeseries'
    AUTO_REFRESH = TRUE;
