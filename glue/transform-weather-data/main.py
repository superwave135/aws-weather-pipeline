from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from datetime import  date, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType
import transformations
import json
import logging
import sys
from awsglue.utils import getResolvedOptions
import boto3


    
# Convert JSON schema to Spark StructType
def parse_schema(table_name: str, project_name:str):

    bucket_name = "weather-datalake-projects-scripts-01"
    file_key = f"{project_name}/glue/transform-weather-data/schemas/{table_name}.json"
    s3_client = boto3.client('s3')

    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    streaming_body = response['Body']
    schema_dict = json.load(streaming_body)

    fields = []
    for field in schema_dict["fields"]:
        field_name = field["name"]
        field_type = field["type"].lower()
        if field_type == "string":
            spark_type = StringType()
        elif field_type == "int" or field_type == "integer":
            spark_type = IntegerType()
        elif field_type == "double":
            spark_type = DoubleType()
        elif field_type == "timestamp":
            spark_type = TimestampType()
        elif field_type == "date":
            spark_type = DateType()
        else:
            raise ValueError(f"Unsupported type: {field_type}")
        fields.append(StructField(field_name, spark_type, True))
    schema = StructType(fields)
    partition_keys = schema_dict["partitionKeys"]

    return schema, partition_keys


def create_table_if_not_exists(spark: SparkSession, dict_tables_info: str , weather_data_type_extraction: str):
    logger.info("Checking if table already exists")

    list_tables_info = [dict_tables_info[key] for key in dict_tables_info.keys() if weather_data_type_extraction in key]
    for record in list_tables_info:
        table_name_to_create = record["table_name"]
        database_name = record["database_name"]
        full_path_table_to_create = f"glue_catalog.{database_name}.{table_name_to_create}"

        schema, partition_keys = parse_schema(table_name=table_name_to_create, project_name=project_name)
        query = f"""
            CREATE TABLE IF NOT EXISTS {full_path_table_to_create} (
                {', '.join([f'{field.name} {field.dataType.simpleString()}' for field in schema])}
            )
            USING iceberg
            PARTITIONED BY ({', '.join(partition_keys)})
            TBLPROPERTIES ("format-version"="2")
        """
        spark.sql(query)
    return True


bucket_name = "weather-datalake-projects-01"
project_name = "pipeline-weather-data"
database_name = "warehouse_weather_data"
dict_tables_info = {
    "timeseries_actual_data": {"table_name": "weather_actual_data_timeseries", "database_name": database_name},
    "timeseries_actual_data_agg": {"table_name": "weather_actual_data_timeseries_agg", "database_name": database_name},
    "timeseries_forecast_data": {"table_name": "weather_forecast_data_timeseries", "database_name": database_name},
}
# year_month = "2025-01"

args = getResolvedOptions(sys.argv,
                          ['weather_data_type_extraction'])

weather_data_type_extraction = args.get("weather_data_type_extraction")

spark = SparkSession.builder \
                    .appName("pipeline-weather-data") \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{bucket_name}/{project_name}/warehouse") \
                    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
                    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
                    .getOrCreate()

glueContext = GlueContext(spark)
logger: logging.Logger = glueContext.get_logger()

create_table_if_not_exists(spark=spark,
                            dict_tables_info=dict_tables_info,
                            weather_data_type_extraction=weather_data_type_extraction)



if weather_data_type_extraction == "actual":
    list_s3_bucket_objects_path = [f"s3://{bucket_name}/{project_name}/staging/actual_data"]
    # for actual data we will get the data of all the month to calculate the agg by year,month,hour
    df_frame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                connection_options= {
                                                    "paths": list_s3_bucket_objects_path,
                                                    "recurse": True},
                                                format = "json",
                                                format_options={
                                                    "withHeader": True,
                                                    "jsonPath": "$[*]"                                             
                                                    })


    dfc = df_frame.relationalize(root_table_name="root", staging_path=f"s3://{bucket_name}/{project_name}/tmp")
    # dfc.keys() #check keys
    
    ####### COMPLETE TIMESERIES ########
    timeseries_actual_data_df = transformations.process_timeseries_actual_data(
                                                        spark = spark,
                                                        dfc = dfc,
                                                        dict_tables_info = dict_tables_info,
                                                        logger = logger)

    ####### AGG TIMESERIES ########
    transformations.process_timeseries_actual_data_agg(
                                    spark = spark,
                                    timeseries_actual_data_df= timeseries_actual_data_df,
                                    dict_tables_info = dict_tables_info,
                                    logger = logger)
    
elif weather_data_type_extraction=="forecast":
    list_s3_bucket_objects_path = [f"s3://{bucket_name}/{project_name}/staging/forecast_data"]
    df_frame = glueContext.create_dynamic_frame_from_options(connection_type="s3",
                                                connection_options= {
                                                    "paths": list_s3_bucket_objects_path,
                                                    "recurse": True},
                                                format = "json",
                                                format_options={
                                                    "withHeader": True,
                                                    "jsonPath": "$[*]"                                            
                                                    })


    dfc = df_frame.relationalize(root_table_name="root", staging_path=f"s3://{bucket_name}/{project_name}/tmp")
    # dfc.keys() #check keys

    ####### COMPLETE TIMESERIES FORECAST ########
    timeseries_actual_data_df = transformations.process_timeseries_forecast_data(
                                                        spark = spark,
                                                        dfc = dfc,
                                                        dict_tables_info = dict_tables_info,
                                                        logger = logger)

else:
    raise ValueError("Error: Invalid weather_data_type_extraction. Must be 'actual' or 'forecast'.")