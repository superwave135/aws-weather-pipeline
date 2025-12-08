from weather_api import WeatherAPI  # Now you can import the class
import json
import boto3
import datetime
import asyncio

def send_data_to_s3(json_data: dict, s3_bucket_name:str, s3_object_key:str):
    """
    Sends JSON data as a Parquet file to Amazon S3.

    Args:
        json_data (list or dict): The JSON data to be sent.
                                    (e.g., list of dictionaries, dictionary of lists, etc.).
        s3_bucket_name (str): The name of the S3 bucket to upload to.
        s3_object_key (str): The key (path within the bucket) for the Parquet file in S3.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    try:
        # 1. Serialize the dictionary to JSON string
        json_data_str = json.dumps(json_data)

        # 2. Initialize S3 client
        s3_client = boto3.client(
            's3',
            # aws_access_key_id=aws_access_key_id,
            # aws_secret_access_key=aws_secret_access_key,
            # region_name=aws_region_name
        )

        # 3. Upload data file to S3
        s3_client.put_object(
            Body=json_data_str.encode('utf-8'),  # Encode JSON string to bytes
            Bucket=s3_bucket_name,
            Key=s3_object_key
        )

        return True

    except Exception as e:
        raise ValueError(f"Error sending data to S3: {e}")
        return False



def generate_list_date(start_date:datetime.date, end_date: datetime.date):
    list_dates = []
    increment_date = start_date

    while increment_date <= end_date:
        list_dates.append(increment_date)
        increment_date += datetime.timedelta(days=1)

    return list_dates


async def _async_lambda_handler(event, context):  # Define an *async* function for the core logic
    """
    Asynchronous part of the lambda handler to fetch weather data and send to S3.
    """
    s3_bucket_name = "weather-datalake-projects"
    project_pipeline_name = "pipeline-weather-data"
    database_name = "warehouse_weather_data"

    weather_data_type_extraction = event.get('weather_data_type_extraction')
    api_token = event.get('api_token')
    batch_id = event.get('batch_id')

    s3_client = boto3.client('s3')
    s3_object_key =f"{project_pipeline_name}/staging/list_locations_dates_to_process/list_locations_dates_to_process_{weather_data_type_extraction}_data_batch_{batch_id}.json"
    s3_object = s3_client.get_object(Bucket=s3_bucket_name,
                                     Key=s3_object_key)
    list_locations_dates_to_process = [json.loads(s3_object['Body'].read().decode('utf-8'))]

    weather = WeatherAPI(api_token)  # Initialize WeatherAPI
    semaphore = asyncio.Semaphore(15)  # Limit concurrent requests to 10

    async def fetch_and_save_weather_data(location_id, list_dates=None, days=None, weather_data_type=None, s3_object_key=None):
        """
        Fetches weather data from the API and saves it to S3, respecting semaphore limit.
        """
        retry_delay = 2 
        max_retries = 3
        for attempt in range(max_retries):
            async with semaphore:
                try:
                    aggregated_data = []
                    if weather_data_type == "forecast":
                        data = await weather.get_forecast_weather(q=f"id:{location_id}", days=days)
                        data["location"].update({"id": location_id})
                        aggregated_data.append(data)

                    elif weather_data_type == "actual":
                        for date in list_dates:
                            data = await weather.get_actual_weather(q=f"id:{location_id}", dt=str(date))
                            data["location"].update({"id": location_id})
                            aggregated_data.append(data)
                    else:
                        raise ValueError("Invalid weather_data_type in task")
                    

                    if aggregated_data: # Ensure data is not None before saving
                        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_object_key, Body=json.dumps(aggregated_data))
                        print(f"Data saved to s3://{s3_bucket_name}/{s3_object_key}")
                        return True
                    else:
                        print(f"No data received from API for location {location_id} and date/days {date or days}. Skipping S3 save.")
                        return True

                except Exception as e:
                    print(f"Error processing location {location_id} for date/days {date or days}: {e}")
                    if attempt < max_retries - 1: 
                        await asyncio.sleep(retry_delay)
                    else:
                        print(f"Max retries reached for location {location_id}. Processing failed.")
                        return False 


    tasks = []  # Create a list to store tasks for asyncio.gather
    for record in list_locations_dates_to_process:
        start_date_str = record["start_date"]
        end_date_str = record["end_date"]
        location_ids = record["location_ids"]
        print(f"Get {weather_data_type_extraction} data from {start_date_str} to {end_date_str}")
        print(f"Locations to process: {location_ids}")


        for location_id in location_ids:
        
            start_date = datetime.date.fromisoformat(start_date_str)
            end_date = datetime.date.fromisoformat(end_date_str)

            if weather_data_type_extraction == "forecast":
                if start_date > end_date:
                    raise ValueError("start date is greather than end date")

                today_date = datetime.datetime.date(datetime.datetime.now())
                if start_date != today_date:
                    raise ValueError("For forecasted data the start_date has to be equal today")

                days_forecast = 15

                s3_object_key = f"{project_pipeline_name}/staging/forecast_data/{location_id}.json"
                task = fetch_and_save_weather_data(
                    location_id=location_id,
                    days=days_forecast,
                    weather_data_type="forecast",
                    s3_object_key=s3_object_key
                )
                tasks.append(task)

            elif weather_data_type_extraction == "actual":

                list_dates = generate_list_date(start_date=start_date, end_date=end_date)

                s3_object_key = f"{project_pipeline_name}/staging/actual_data/{location_id}.json"
                task = fetch_and_save_weather_data(
                    location_id=location_id,
                    list_dates=list_dates,
                    weather_data_type="actual",
                    s3_object_key=s3_object_key
                )
                tasks.append(task)
            else:
                raise ValueError('weather_data_type_extraction param is wrong')

    # Gather all tasks and process results
    await asyncio.gather(*tasks)  # Await all tasks concurrently
            

    msg = 'Function executed successfully!'
    print(msg)
    response_lambda = {
        'statusCode': 200,
        'body': msg
    }
    return response_lambda


def lambda_handler(event, context): # Keep lambda_handler synchronous
    """
    Synchronous Lambda handler entry point.
    """
    try:
        return asyncio.run(_async_lambda_handler(event, context)) # Run the async part and return result
    except Exception as e:
        msg = str(e)
        print(msg)
        raise ValueError(msg)



if __name__ == "__main__": # Example of local execution
    event_example = {
        "weather_data_type_extraction": "actual",
        "api_token": "8b8c4f3c02be4c40964170107252302",
        "batch_id": 1
    }
    context_example = None
    response = lambda_handler(event_example, context_example)
    print(response)