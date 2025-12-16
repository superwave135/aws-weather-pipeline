from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.base_aws import BaseSessionFactory
import awswrangler as wr
from awswrangler.exceptions import QueryFailed
import pandas as pd
from datetime import datetime
from botocore.config import Config
import json
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup


def generate_list_location_dates_to_collect(aws_conn_id, batch_size):
    bucket_name = "weather-datalake-projects-01"
    project_name = "pipeline-weather-data"
    database_name = "warehouse_weather_data"
    today_date = str(datetime.now().date())
    initial_date = "2025-09-01" # if we don't have data


    conn_aws = BaseHook.get_connection(aws_conn_id)

    aws_session = BaseSessionFactory(conn=conn_aws).create_session()

    s3_bucket_name= "weather-datalake-projects-01"
    project_pipeline_name = "pipeline-weather-data"
    database_name = "warehouse_weather_data"
    s3_output_tmp = f's3://{bucket_name}/{project_name}/tmp'



    s3_client = aws_session.client('s3')

    query_dim_locations = f'SELECT location_id FROM "{database_name}"."dim_locations"'
    df_locations = wr.athena.read_sql_query(sql=query_dim_locations, 
                                        database=database_name,
                                        s3_output=s3_output_tmp,
                                        boto3_session=aws_session)

    dict_config = {
        "actual":{
            "query":f'''
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
        resp_list_objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{project_name}/staging/{key}_data")

        for object in resp_list_objects.get('Contents',[]):
            print('Deleting', object['Key'])
            s3_client.delete_object(Bucket=bucket_name, Key=object['Key'])

        query = dict_config[key]["query"]
        try:
            df = wr.athena.read_sql_query(sql=query, 
                                        database=database_name,
                                        s3_output=s3_output_tmp,
                                        boto3_session=aws_session)
            df.set_index("location_id", inplace=True)
            df_location_and_dates = df_locations.join(df, on="location_id")
                    
            df_location_and_dates.reset_index(inplace=True)
            df_location_and_dates["max_date_collected"] = df_location_and_dates["max_date_collected"].fillna(initial_date)
            if key=="actual":
                #for actual data I need to use always the complete month for aggregations
                df_location_and_dates["start_date"] = df_location_and_dates['max_date_collected'].dt.to_period('M').dt.to_timestamp()
                df_location_and_dates["start_date"] = df_location_and_dates["start_date"].dt.date
                df_location_and_dates["start_date"] = df_location_and_dates["start_date"].astype(str)
                df_location_and_dates["end_date"] = today_date

            else:
                df_location_and_dates["start_date"] = today_date
                df_location_and_dates["end_date"] = today_date

            df_location_and_dates.drop(columns=["max_date_collected"],inplace=True)
            df_location_and_dates.reset_index(inplace=True)

        except QueryFailed as e:
            error_message = str(e) 
            #table doesn't exist, so lets get the data from the begin
            if "NOT_FOUND" in error_message:
                if key=="actual":
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
                if current_batch['location_ids']: # check if batch has locations before appending
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

        for i in range(0,len(batches_list)):
            batch = json.dumps(batches_list[i])
            s3_object_key =f"{project_pipeline_name}/staging/list_locations_dates_to_process/list_locations_dates_to_process_{key}_data_batch_{i}.json"
            s3_client.put_object(
                Body=batch.encode('utf-8'),  # Encode JSON string to bytes
                Bucket=s3_bucket_name,
                Key=s3_object_key
            )

        #adding the complete series
        s3_object_key =f"{project_pipeline_name}/staging/list_locations_dates_to_process/list_locations_dates_to_process_{key}_data.json"
        batches_list_str = json.dumps(batches_list)
        s3_client.put_object(
                Body=batches_list_str.encode('utf-8'),  # Encode JSON string to bytes
                Bucket=s3_bucket_name,
                Key=s3_object_key
            )
        
        Variable.set((key +'_num_batches'),len(batches_list))
 

    return True

def create_or_update_dim_locations_table(aws_conn_id):
    
    bucket_name = "weather-datalake-projects-01"
    project_name = "pipeline-weather-data"
    database_name = "warehouse_weather_data"

    conn_aws = BaseHook.get_connection(aws_conn_id)

    aws_session = BaseSessionFactory(conn=conn_aws).create_session()

    s3_bucket_name= "weather-datalake-projects-01"
    database_name = "warehouse_weather_data"
    table_location =f"s3://{bucket_name}/{project_name}/warehouse/warehouse_weather_data.db/dim_locations"
    
    s3_client = aws_session.client('s3')
    s3_object_key_dim_locations = f"{project_name}/aux_data/dim_locations.json"
    s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_object_key_dim_locations)
    json_locations = json.loads(s3_object['Body'].read().decode('utf-8'))
    df_locations = pd.DataFrame(json_locations)

    dict_dtypes = {"country":"string", "name": "string", "lat": "double", "lon": "double", "location_id": "int"}

    wr.athena.to_iceberg(df=df_locations, 
                            database=database_name,
                            table="dim_locations",
                            temp_path=table_location + "/tmp",
                            dtype = dict_dtypes,
                            mode="overwrite",
                            table_location=table_location,
                            keep_files=False,
                            index=False,
                            boto3_session=aws_session)

    return True


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 0, 
}


# Instantiate a DAG
with DAG(
    dag_id='pipeline-wheater-data',
    default_args=default_args,
    description='DAG to run AWS services for the ETL weather Data',
    schedule_interval=None, 
    catchup=False,
    concurrency=2
) as dag:

    current_date = datetime.now()

    create_or_update_dim_locations_table_task = PythonOperator(
            task_id='create_or_update_dim_locations_table',
            python_callable=create_or_update_dim_locations_table,
            dag=dag,
            op_kwargs={
                'aws_conn_id': 'aws_conn',
                'batch_size':  8000
            },
        )


    generate_dates_task = PythonOperator(
            task_id='generate_dates_task',
            python_callable=generate_list_location_dates_to_collect,
            dag=dag,
            op_kwargs={
                'aws_conn_id': 'aws_conn',
                'batch_size':  8000
            },
        )


    list_weather_data_type_extraction = ["actual","forecast"]

    for weather_data_type_extraction in list_weather_data_type_extraction:

        transform_weather_data_glue = GlueJobOperator(
            task_id=f'transform_{weather_data_type_extraction}_weather_data', # Dynamic task_id for Glue
            job_name='transform-weather-data',
            aws_conn_id='aws_conn',
            script_args={
                '--weather_data_type_extraction': "{{ params.weather_data_type_extraction }}", 
                },
            params={ 
                "weather_data_type_extraction": weather_data_type_extraction,
            },
            wait_for_completion=True,
            dag=dag,
        )

        num_batches = int(Variable.get(f"{weather_data_type_extraction}_num_batches"))

        with TaskGroup(group_id=f'get_{weather_data_type_extraction}_data_lambda_raw') as tg:
            for batch_id in range(0,num_batches):

                
                get_api_weather_data_lambda = LambdaInvokeFunctionOperator(
                    task_id=f'get_api_weather_{weather_data_type_extraction}_data_lambda_batch_{batch_id}',
                    function_name = 'get_api_weather_data_lambda',
                    invocation_type = 'RequestResponse',
                    aws_conn_id='aws_conn',
                    qualifier='$LATEST',
                    payload =json.dumps({
                            'weather_data_type_extraction': weather_data_type_extraction,
                            'api_token': 'ca7fd84867944b67bd685355251512', #replace with your weather api key  
                            'batch_id': batch_id
                    }),
                    botocore_config={"retries":{"max_attempts":0},"connect_timeout": 900,"read_timeout": 900,"tcp_keepalive": True},
                    dag=dag,
                )
                
                create_or_update_dim_locations_table_task >> generate_dates_task >> get_api_weather_data_lambda >> transform_weather_data_glue

