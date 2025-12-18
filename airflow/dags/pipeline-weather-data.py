from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import BaseSessionFactory
import awswrangler as wr
from awswrangler.exceptions import QueryFailed
import pandas as pd
from datetime import datetime, timedelta
from botocore.config import Config
import json
from airflow.models import Variable
import logging

# Get logger for better debugging
logger = logging.getLogger(__name__)


def generate_list_location_dates_to_collect(aws_conn_id, batch_size):
    """Generate list of locations and dates to collect weather data for"""
    bucket_name = "weather-datalake-projects-01"
    project_name = "pipeline-weather-data"
    database_name = "warehouse_weather_data"
    today_date = str(datetime.now().date())
    initial_date = "2025-09-01"  # if we don't have data

    conn_aws = BaseHook.get_connection(aws_conn_id)
    aws_session = BaseSessionFactory(conn=conn_aws).create_session()

    s3_bucket_name = "weather-datalake-projects-01"
    project_pipeline_name = "pipeline-weather-data"
    s3_output_tmp = f's3://{bucket_name}/{project_name}/tmp'

    s3_client = aws_session.client('s3')

    query_dim_locations = f'SELECT location_id FROM "{database_name}"."dim_locations"'
    df_locations = wr.athena.read_sql_query(
        sql=query_dim_locations, 
        database=database_name,
        s3_output=s3_output_tmp,
        boto3_session=aws_session
    )

    dict_config = {
        "actual": {
            "query": f'''
                    SELECT  
                        location_id,
                        max(date_parse(concat(CAST(year AS VARCHAR), '-', CAST(month AS VARCHAR), '-', CAST(day AS VARCHAR)), '%Y-%m-%d')) AS max_date_collected
                    FROM "{database_name}"."weather_actual_data_timeseries"
                    GROUP BY location_id;''',
        },
        "forecast": {
            "query": f'''
                    SELECT  
                        location_id,
                        max(date_parse(concat(CAST(ref_year AS VARCHAR), '-', CAST(ref_month AS VARCHAR), '-', CAST(ref_day AS VARCHAR)), '%Y-%m-%d')) AS max_date_collected
                    FROM "{database_name}"."weather_forecast_data_timeseries"
                    GROUP BY location_id;''',
        }
    }

    for key in dict_config.keys():
        # delete all files inside staging folder
        resp_list_objects = s3_client.list_objects_v2(
            Bucket=bucket_name, 
            Prefix=f"{project_name}/staging/{key}_data"
        )

        for object in resp_list_objects.get('Contents', []):
            logger.info(f'Deleting {object["Key"]}')
            s3_client.delete_object(Bucket=bucket_name, Key=object['Key'])

        query = dict_config[key]["query"]
        try:
            df = wr.athena.read_sql_query(
                sql=query, 
                database=database_name,
                s3_output=s3_output_tmp,
                boto3_session=aws_session
            )
            df.set_index("location_id", inplace=True)
            df_location_and_dates = df_locations.join(df, on="location_id")
                    
            df_location_and_dates.reset_index(inplace=True)
            df_location_and_dates["max_date_collected"] = df_location_and_dates["max_date_collected"].fillna(initial_date)
            
            if key == "actual":
                # for actual data I need to use always the complete month for aggregations
                df_location_and_dates["start_date"] = df_location_and_dates['max_date_collected'].dt.to_period('M').dt.to_timestamp()
                df_location_and_dates["start_date"] = df_location_and_dates["start_date"].dt.date
                df_location_and_dates["start_date"] = df_location_and_dates["start_date"].astype(str)
                df_location_and_dates["end_date"] = today_date
            else:
                df_location_and_dates["start_date"] = today_date
                df_location_and_dates["end_date"] = today_date

            df_location_and_dates.drop(columns=["max_date_collected"], inplace=True)
            df_location_and_dates.reset_index(inplace=True)

        except QueryFailed as e:
            error_message = str(e) 
            # table doesn't exist, so lets get the data from the begin
            if "NOT_FOUND" in error_message:
                if key == "actual":
                    start_date = initial_date
                else:
                    start_date = today_date
                
                end_date = today_date
                df_location_and_dates = df_locations.copy()
                df_location_and_dates["start_date"] = start_date
                df_location_and_dates["end_date"] = end_date
                df_location_and_dates.reset_index(inplace=True)
            else:
                raise

        df_location_and_dates["start_date"] = pd.to_datetime(df_location_and_dates["start_date"])
        df_location_and_dates["end_date"] = pd.to_datetime(df_location_and_dates["end_date"])
        df_location_and_dates['num_days'] = (df_location_and_dates['end_date'] - df_location_and_dates['start_date']).dt.days + 1
        
        batches_list = []
        current_batch_points = 0
        current_batch = None

        for index, row in df_location_and_dates.iterrows():
            start_date = str(row['start_date'].date())
            end_date = str(row['end_date'].date())
            location_id = row['location_id']
            num_days = row['num_days']

            if current_batch is None:
                current_batch = {"start_date": start_date, "end_date": end_date, "location_ids": []}

            if current_batch['start_date'] != start_date or current_batch['end_date'] != end_date:
                if current_batch['location_ids']:  # check if batch has locations before appending
                    batches_list.append(current_batch)
                current_batch = {"start_date": start_date, "end_date": end_date, "location_ids": []}
                current_batch_points = 0

            if current_batch_points + num_days <= batch_size:
                current_batch['location_ids'].append(location_id)
                current_batch_points += num_days
            else:
                batches_list.append(current_batch)
                current_batch = {"start_date": start_date, "end_date": end_date, "location_ids": [location_id]}
                current_batch_points = num_days

        if current_batch and current_batch['location_ids']:
            batches_list.append(current_batch)

        for i in range(0, len(batches_list)):
            batch = json.dumps(batches_list[i])
            s3_object_key = f"{project_pipeline_name}/staging/list_locations_dates_to_process/list_locations_dates_to_process_{key}_data_batch_{i}.json"
            s3_client.put_object(
                Body=batch.encode('utf-8'),
                Bucket=s3_bucket_name,
                Key=s3_object_key
            )

        # adding the complete series
        s3_object_key = f"{project_pipeline_name}/staging/list_locations_dates_to_process/list_locations_dates_to_process_{key}_data.json"
        batches_list_str = json.dumps(batches_list)
        s3_client.put_object(
            Body=batches_list_str.encode('utf-8'),
            Bucket=s3_bucket_name,
            Key=s3_object_key
        )
        
        Variable.set((key + '_num_batches'), len(batches_list))
        logger.info(f"Set {key}_num_batches = {len(batches_list)}")

    return True


def create_or_update_dim_locations_table(aws_conn_id):
    """Create or update dimension locations table in Athena"""
    bucket_name = "weather-datalake-projects-01"
    project_name = "pipeline-weather-data"
    database_name = "warehouse_weather_data"

    conn_aws = BaseHook.get_connection(aws_conn_id)
    aws_session = BaseSessionFactory(conn=conn_aws).create_session()

    s3_bucket_name = "weather-datalake-projects-01"
    table_location = f"s3://{bucket_name}/{project_name}/warehouse/warehouse_weather_data.db/dim_locations"
    
    s3_client = aws_session.client('s3')
    s3_object_key_dim_locations = f"{project_name}/aux_data/dim_locations.json"
    s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_object_key_dim_locations)
    json_locations = json.loads(s3_object['Body'].read().decode('utf-8'))
    df_locations = pd.DataFrame(json_locations)

    dict_dtypes = {
        "country": "string", 
        "name": "string", 
        "lat": "double", 
        "lon": "double", 
        "location_id": "int"
    }

    wr.athena.to_iceberg(
        df=df_locations, 
        database=database_name,
        table="dim_locations",
        temp_path=table_location + "/tmp",
        dtype=dict_dtypes,
        mode="overwrite",
        table_location=table_location,
        keep_files=False,
        index=False,
        boto3_session=aws_session
    )

    logger.info("Successfully created/updated dim_locations table")
    return True


def process_all_lambda_invocations(weather_type: str, aws_conn_id: str, api_token: str):
    """Process all batches for a given weather type"""
    logger.info(f"Starting Lambda invocations for {weather_type}")
    
    # Get number of batches from Airflow Variable
    try:
        num_batches = int(Variable.get(f"{weather_type}_num_batches"))
        logger.info(f"Found {num_batches} batches to process for {weather_type}")
    except KeyError:
        logger.error(f"Variable {weather_type}_num_batches not found!")
        raise
    
    # Get AWS connection and create session
    conn_aws = BaseHook.get_connection(aws_conn_id)
    aws_session = BaseSessionFactory(conn=conn_aws).create_session()
    
    # Create Lambda client with proper timeout configuration
    config = Config(
        retries={'max_attempts': 0},
        connect_timeout=900,
        read_timeout=900,
        tcp_keepalive=True
    )
    
    lambda_client = aws_session.client('lambda', config=config)
    
    results = []
    failed_batches = []
    
    for batch_id in range(num_batches):
        try:
            logger.info(f"Invoking Lambda for {weather_type} batch {batch_id}/{num_batches-1}")
            
            payload = json.dumps({
                'weather_data_type_extraction': weather_type,
                'api_token': api_token,
                'batch_id': batch_id
            })
            
            response = lambda_client.invoke(
                FunctionName='get_api_weather_data_lambda',
                InvocationType='RequestResponse',
                Payload=payload
            )
            
            # Read response
            response_payload = json.loads(response['Payload'].read())
            
            # Check if Lambda execution was successful
            if 'FunctionError' in response:
                logger.error(f"Lambda error for batch {batch_id}: {response}")
                failed_batches.append(batch_id)
            else:
                logger.info(f"Successfully processed batch {batch_id}")
                results.append(response_payload)
                
        except Exception as e:
            logger.error(f"Exception invoking Lambda for batch {batch_id}: {str(e)}")
            failed_batches.append(batch_id)
            # Continue processing other batches instead of failing immediately
    
    if failed_batches:
        error_msg = f"Failed to process batches: {failed_batches} for {weather_type}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info(f"Completed all {num_batches} Lambda invocations for {weather_type}")
    return results


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Instantiate the DAG
with DAG(
    dag_id='pipeline-weather-data',
    default_args=default_args,
    description='DAG to run AWS services for the ETL weather Data',
    schedule_interval='0 10 * * *',  # 10 AM UTC = 6 PM Singapore Time
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'etl', 'aws']
) as dag:
    
    # Task 1: Create or update dimension locations table
    create_or_update_dim_locations_table_task = PythonOperator(
        task_id='create_or_update_dim_locations_table',
        python_callable=create_or_update_dim_locations_table,
        op_kwargs={
            'aws_conn_id': 'aws_conn',
        },
    )
    
    # Task 2: Generate list of dates and locations to collect
    generate_dates_task = PythonOperator(
        task_id='generate_dates_task',
        python_callable=generate_list_location_dates_to_collect,
        op_kwargs={
            'aws_conn_id': 'aws_conn',
            'batch_size': 8000
        },
    )
    
    # Process both actual and forecast weather data
    list_weather_data_type_extraction = ["actual", "forecast"]
    
    for weather_data_type_extraction in list_weather_data_type_extraction:
        
        # Task 3: Invoke Lambda functions for all batches
        invoke_all_lambdas = PythonOperator(
            task_id=f'invoke_all_lambdas_{weather_data_type_extraction}',
            python_callable=process_all_lambda_invocations,
            op_kwargs={
                'weather_type': weather_data_type_extraction,
                'aws_conn_id': 'aws_conn',
                'api_token': Variable.get('weather_api_token', default_var='ca7fd84867944b67bd685355251512')
            },
            execution_timeout=timedelta(hours=2),
        )
        
        # Task 4: Transform weather data using AWS Glue
        transform_weather_data_glue = GlueJobOperator(
            task_id=f'transform_{weather_data_type_extraction}_weather_data',
            job_name='transform-weather-data',
            script_location='s3://weather-datalake-projects-scripts-01/pipeline-weather-data/glue/transform-weather-data/main.py',
            aws_conn_id='aws_conn',
            iam_role_name='glue_role',
            script_args={
                '--weather_data_type_extraction': weather_data_type_extraction,
            },
            create_job_kwargs={
                'GlueVersion': '4.0',
                'NumberOfWorkers': 2,
                'WorkerType': 'G.1X',
                'Timeout': 2880,
                'MaxRetries': 0,
            },
            wait_for_completion=True,
        )
        
        # Set task dependencies
        create_or_update_dim_locations_table_task >> generate_dates_task >> invoke_all_lambdas >> transform_weather_data_glue