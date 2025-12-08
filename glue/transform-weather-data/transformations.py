#CLUSTER BY - this command is necessary when you are adding data to a partitioned table in iceberg (it sorts the partitions) in the tmp table
#
#

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, mean
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from awsglue.dynamicframe import DynamicFrameCollection
from pyspark.sql.dataframe import DataFrame
import logging

def check_partition_values(spark: SparkSession, column_partition: str, full_path_table_stg:str):
    partition_str = ""
    for partition in spark.sql(f"SELECT distinct {column_partition} FROM {full_path_table_stg}").collect():
        partition_str += str(partition[column_partition]) + ","

    partitions_values = partition_str[:-1]
    return partitions_values


def process_timeseries_actual_data(spark:SparkSession, dfc:DynamicFrameCollection, dict_tables_info:dict, logger: logging.Logger):
    
    location_df = dfc.select("root").toDF()

    ############ WORKING WITH TIMESERIES HOUR DATASET #################
    table_name = dict_tables_info["timeseries_actual_data"]["table_name"]
    database_name = dict_tables_info["timeseries_actual_data"]["database_name"]

    logger.info(f"Start to process the timeseries data - {table_name}")
    timeseries_hour_df = dfc.select("root_forecast.forecastday.val.hour").toDF()

    selected_columns_location_df = ["`location.id`",
                                    "`forecast.forecastday`"]

    selected_columns_hour_df = [
        "id",
        "`forecast.forecastday.val.hour.val.time`",
        "`forecast.forecastday.val.hour.val.temp_c`",
        "`forecast.forecastday.val.hour.val.wind_kph`",
        "`forecast.forecastday.val.hour.val.wind_degree`",
        "`forecast.forecastday.val.hour.val.pressure_mb`",
        "`forecast.forecastday.val.hour.val.precip_mm`",
        "`forecast.forecastday.val.hour.val.snow_cm`",
        "`forecast.forecastday.val.hour.val.humidity`",
        "`forecast.forecastday.val.hour.val.uv`",
        "`forecast.forecastday.val.hour.val.cloud`",
        "`forecast.forecastday.val.hour.val.feelslike_c`",
        "`forecast.forecastday.val.hour.val.chance_of_rain`",
        "`forecast.forecastday.val.hour.val.chance_of_snow`"
        ]

    timeseries_actual_data_df = timeseries_hour_df.select([col(c).alias(c.replace("forecast.forecastday.val.hour.val.","").replace("`","")) for c in selected_columns_hour_df])

    location_df = location_df.select([col(c).alias(c.replace(".","_").replace("`","")) for c in selected_columns_location_df])

    location_df = location_df.withColumnRenamed("forecast_forecastday","id_forecastday")
    timeseries_actual_data_df = timeseries_actual_data_df.withColumnRenamed("id","id_forecastday")

    timeseries_actual_data_df = timeseries_actual_data_df.withColumn(
        "time", F.to_timestamp("time")
    )

    timeseries_actual_data_df = timeseries_actual_data_df.withColumn(
        "year", F.year("time")
    ).withColumn(
        "month", F.month("time")
    ).withColumn(
        "day", F.dayofmonth("time") # or F.day("datetime_column")
    )


    # join daily
    timeseries_actual_data_df = timeseries_actual_data_df.join(F.broadcast(location_df), on="id_forecastday", how="inner")
    timeseries_actual_data_df = timeseries_actual_data_df.drop("id_forecastday")


    timeseries_actual_data_df.createOrReplaceTempView(f"tmp_{table_name}")

    full_path_table = f"glue_catalog.{database_name}.{table_name}"
    full_path_table_stg = f"glue_catalog.{database_name}.{table_name}_stg"

    ### create or replace the stg table
    logger.info(f"Create or replace the STG table - {table_name}")
    query_create_stg_table = f"""
        CREATE OR REPLACE TABLE {full_path_table_stg}
        USING iceberg
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES ("format-version"="2")
        AS SELECT * FROM tmp_{table_name} CLUSTER BY year,month,day
    """
    spark.sql(query_create_stg_table)

    ### MERGE STG TABLE TO FINAL TABLE
    logger.info(f"Merge the STG table to final table - {table_name}")
    partition_values = check_partition_values(spark=spark,
                                              column_partition="year",
                                              full_path_table_stg=full_path_table_stg)

    query_merge = f"""
        MERGE INTO {full_path_table} t 
        USING {full_path_table_stg} s 
        ON 
            t.year IN ({partition_values}) AND
            t.year = s.year AND
            t.location_id = s.location_id AND
            t.time = s.time 

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(query_merge)

    return timeseries_actual_data_df



def process_timeseries_actual_data_agg(spark: SparkSession, timeseries_actual_data_df: DataFrame, dict_tables_info:dict, logger):
    
    ####### WORKING WITH DAY TIMESERIES ########
    table_name = dict_tables_info["timeseries_actual_data_agg"]["table_name"]
    database_name = dict_tables_info["timeseries_actual_data_agg"]["database_name"]
    logger.info(f"Start to process the timeseries data aggregated- {table_name}")
    timeseries_actual_data_hour_df = timeseries_actual_data_df.withColumn(
        "hour", F.hour("time")
    )

    full_path_table = f"glue_catalog.{database_name}.{table_name}"
    full_path_table_stg = f"glue_catalog.{database_name}.{table_name}_stg"

    location_year_month_hour_window = Window.partitionBy("year", "month", "hour", "location_id")
    location_year_month_window = Window.partitionBy("year", "month", "location_id")
    location_year_window = Window.partitionBy("year", "location_id")

    dict_window_functions = {
        "year_month_hour": location_year_month_hour_window,
        "year_month": location_year_month_window,
        "year": location_year_window
    }

    list_columns_to_apply_window = ["temp_c","wind_kph","wind_degree",
                                    "pressure_mb","precip_mm","snow_cm","humidity",
                                    "uv","cloud","feelslike_c"]
    list_windowed_columns = []
    for column in list_columns_to_apply_window:
        for key in dict_window_functions.keys():
                partition_by_str = key
                name_column_windowed = f"{partition_by_str}_avg_{column}"
                timeseries_actual_data_hour_df = timeseries_actual_data_hour_df.withColumn(name_column_windowed, mean(column).over(dict_window_functions[key]))
                list_windowed_columns.append(mean(name_column_windowed).alias(name_column_windowed))

    df_timeseries_agg_hourly = timeseries_actual_data_hour_df.groupBy("year","month","hour","location_id").agg(
        *list_windowed_columns
    )

    df_timeseries_agg_hourly.createOrReplaceTempView(f"tmp_{table_name}")

    ### create or replace the stg table
    logger.info(f"Create or replace the STG table - {table_name}")
    query_create_stg_table = f"""
        CREATE OR REPLACE TABLE {full_path_table_stg}
        USING iceberg
        PARTITIONED BY (year, month)
        TBLPROPERTIES ("format-version"="2")
        AS SELECT * FROM tmp_{table_name} CLUSTER BY year,month
    """
    spark.sql(query_create_stg_table)

    ### MERGE STG TABLE TO FINAL TABLE
    logger.info(f"Merge the STG table to final table - {table_name}")

    partition_values = check_partition_values(spark=spark,
                                              column_partition="year",
                                              full_path_table_stg=full_path_table_stg)

    query_merge = f"""
        MERGE INTO {full_path_table} t   
        USING {full_path_table_stg} s   
        ON 
            t.year IN ({partition_values}) AND
            t.location_id = s.location_id AND
            t.year = s.year AND
            t.month = s.month AND
            t.hour = s.hour

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(query_merge)



def process_timeseries_forecast_data(spark:SparkSession, dfc:DynamicFrameCollection, dict_tables_info:dict, logger: logging.Logger):
    
    location_df = dfc.select("root").toDF()

    location_df = location_df.withColumn(
        "current.last_updated", F.to_timestamp("`current.last_updated`")
    )
    location_df = location_df.withColumn(
        "date_last_updated", F.to_date("`current.last_updated`")
    )

    timeseries_forecast_day_df = dfc.select("root_forecast.forecastday").toDF()

    ############ WORKING WITH TIMESERIES HOUR DATASET #################
    table_name = dict_tables_info["timeseries_forecast_data"]["table_name"]
    database_name = dict_tables_info["timeseries_forecast_data"]["database_name"]

    logger.info(f"Start to process the timeseries data - {table_name}")
    timeseries_forecast_hour_df = dfc.select("root_forecast.forecastday.val.hour").toDF()

    selected_columns_location_df = ["`location.id`",
                                    "`forecast.forecastday`",
                                    "date_last_updated"]

    selected_columns_forecast_day_df = [
        "id",
        "`forecast.forecastday.val.hour`"
        ]


    selected_columns_hour_df = [
        "id",
        "`forecast.forecastday.val.hour.val.time`",
        "`forecast.forecastday.val.hour.val.temp_c`",
        "`forecast.forecastday.val.hour.val.wind_kph`",
        "`forecast.forecastday.val.hour.val.wind_degree`",
        "`forecast.forecastday.val.hour.val.pressure_mb`",
        "`forecast.forecastday.val.hour.val.precip_mm`",
        "`forecast.forecastday.val.hour.val.snow_cm`",
        "`forecast.forecastday.val.hour.val.humidity`",
        "`forecast.forecastday.val.hour.val.uv.double`",
        "`forecast.forecastday.val.hour.val.cloud`",
        "`forecast.forecastday.val.hour.val.feelslike_c`",
        "`forecast.forecastday.val.hour.val.chance_of_rain`",
        "`forecast.forecastday.val.hour.val.chance_of_snow`"
        ]
    

    timeseries_forecast_data_df = timeseries_forecast_hour_df.select([col(c).alias(c.replace("forecast.forecastday.val.hour.val.","").replace("`","")) for c in selected_columns_hour_df])

    location_df = location_df.select([col(c).alias(c.replace(".","_").replace("`","")) for c in selected_columns_location_df])

    timeseries_forecast_day_df = timeseries_forecast_day_df.select([col(c).alias(c.replace("forecast.forecastday.val.hour.val.","").replace("`","")) for c in selected_columns_forecast_day_df])

    location_df = location_df.withColumnRenamed("forecast_forecastday","id")
    timeseries_forecast_day_df = timeseries_forecast_day_df.withColumnRenamed("forecast.forecastday.val.hour","id_forecasthour")
    timeseries_forecast_data_df = timeseries_forecast_data_df.withColumnRenamed("id","id_forecasthour")

    timeseries_forecast_data_df = timeseries_forecast_data_df.withColumn(
        "time", F.to_timestamp("time")
    )

    location_df = location_df.withColumn(
        "ref_year", F.year("date_last_updated")
    ).withColumn(
        "ref_month", F.month("date_last_updated")
    ).withColumn(
        "ref_day", F.dayofmonth("date_last_updated")
    )


    # join daily
    location_df = timeseries_forecast_day_df.join(F.broadcast(location_df), on="id", how="inner")

    timeseries_forecast_data_df = timeseries_forecast_data_df.join(F.broadcast(location_df), on="id_forecasthour", how="inner")
    timeseries_forecast_data_df = timeseries_forecast_data_df.drop("id_forecasthour")
    timeseries_forecast_data_df = timeseries_forecast_data_df.withColumnRenamed(
         existing="date_last_updated",
         new="ref_date"
    ).withColumnRenamed(
         existing="time",
         new="forecast_time"
    ).withColumnRenamed(
         existing="uv.double",
         new="uv"
    )


    timeseries_forecast_data_df.createOrReplaceTempView(f"tmp_{table_name}")

    full_path_table = f"glue_catalog.{database_name}.{table_name}"
    full_path_table_stg = f"glue_catalog.{database_name}.{table_name}_stg"

    ### create or replace the stg table
    logger.info(f"Create or replace the STG table - {table_name}")
    query_create_stg_table = f"""
        CREATE OR REPLACE TABLE {full_path_table_stg}
        USING iceberg
        PARTITIONED BY (ref_year, ref_month, ref_day)
        TBLPROPERTIES ("format-version"="2")
        AS SELECT * FROM tmp_{table_name} CLUSTER BY ref_year, ref_month, ref_day
    """
    spark.sql(query_create_stg_table)

    ### MERGE STG TABLE TO FINAL TABLE
    logger.info(f"Merge the STG table to final table - {table_name}")
    partition_values = check_partition_values(spark=spark,
                                              column_partition="ref_year",
                                              full_path_table_stg=full_path_table_stg)

    query_merge = f"""
        MERGE INTO {full_path_table} t 
        USING {full_path_table_stg} s 
        ON 
            t.ref_year IN ({partition_values}) AND
            t.ref_year = s.ref_year AND
            t.ref_month = s.ref_month AND
            t.ref_day = s.ref_day AND
            t.location_id = s.location_id AND
            t.forecast_time = s.forecast_time 

        WHEN MATCHED THEN UPDATE SET *

        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(query_merge)

    return timeseries_forecast_data_df

