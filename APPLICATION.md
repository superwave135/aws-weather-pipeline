AWS/pipeline-weather-data/
│
├── README.md
├── airflow/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── requirements.txt
│   └── dags/
│       └── pipeline-weather-data.py
├── datasets/
│   └── dim_locations.json
├── glue/
│   └── transform-weather-data/
│       ├── buildspec.yml
│       ├── glue_job_config.json
│       ├── main.py
│       ├── requirements.txt
│       ├── transformations.py
│       └── schemas/
│           ├── weather_actual_data_timeseries_agg.json
│           ├── weather_actual_data_timeseries.json
│           └── weather_forecast_data_timeseries.json
├── lambda/
│   ├── buildspec.yml
│   ├── get-data-api.py
│   ├── requirements.txt
│   └── weather_api.py
├── snowflake/
│   ├── creating_resources_in_snowflake.sql
│   ├── creating_tables.sql
│   └── querying_the_data.sql
├── src/
│   └── architecture_overview.excalidraw
├── tableau_viz/
│   └── Weather Analysis.twbx
└── terraform/
    ├── codebuild.tf
    ├── glue.tf
    ├── iam.tf
    ├── locals.tf
    ├── provider.tf
    ├── s3.tf
    └── variables.tf


* ARCHITECTURE *

┌─────────────────────────────────────────────────────────────────────────────┐
│                          WEATHER API (External)                             │
│                   (WeatherAPI.com - Forecast & Historical Data)             │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ HTTP Requests
                                 │ (Triggered by Airflow)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION LAYER                                    │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                    Apache Airflow (Docker)                         │     │
│  │                                                                    │     │
│  │  DAG: pipeline-weather-data.py                                     │     │
│  │  ├─ Task 1: Trigger Lambda (Forecast Data)                         │     │
│  │  ├─ Task 2: Trigger Lambda (Actual Data)                           │     │
│  │  ├─ Task 3: Wait for S3 Data Arrival                               │     │
│  │  └─ Task 4: Trigger Glue Job                                       │     │
│  │                                                                    │     │
│  │  Manages: Dependencies, Scheduling, Error Handling                 │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 │ Invokes
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       DATA EXTRACTION LAYER                                 │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                    AWS Lambda Function                             │     │
│  │                                                                    │     │
│  │  Files: get-data-api.py, weather_api.py                            │     │
│  │                                                                    │     │
│  │  Process:                                                          │     │
│  │  1. Read dim_locations.json from S3                                │     │
│  │  2. For each location:                                             │     │
│  │     - Call Weather API (Forecast/Actual)                           │     │
│  │     - Get JSON response                                            │     │
│  │  3. Write raw JSON to S3 staging area                              │     │
│  │                                                                    │     │
│  │  Serverless, event-driven, parallel processing                     │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ Writes JSON
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA LAKE - RAW ZONE                               │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │              Amazon S3: weather-datalake-projects/                 │     │
│  │                    pipeline-weather-data/staging/                  │     │
│  │                                                                    │     │
│  │  ├── forecast_data/                                                │     │
│  │  │   ├── {location_id}_1.json                                      │     │
│  │  │   ├── {location_id}_2.json                                      │     │
│  │  │   └── ...                                                       │     │
│  │  │                                                                 │     │
│  │  └── actual_data/                                                  │     │
│  │      ├── {location_id}_1.json                                      │     │
│  │      ├── {location_id}_2.json                                      │     │
│  │      └── ...                                                       │     │
│  │                                                                    │     │
│  │  Raw, unstructured JSON files from Weather API                     │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 │ Reads
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       DATA PROCESSING LAYER                                 │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                      AWS Glue ETL Job                              │     │
│  │                     (Apache Spark/PySpark)                         │     │
│  │                                                                    │     │
│  │  Files: main.py, transformations.py                                │     │
│  │  Schemas: JSON schema definitions                                  │     │
│  │                                                                    │     │
│  │  Processing Steps:                                                 │     │
│  │  1. Read raw JSON from S3 staging                                  │     │
│  │  2. Parse and flatten nested JSON structures                       │     │
│  │  3. Apply schema validation                                        │     │
│  │  4. Transform and clean data:                                      │     │
│  │     - Normalize timestamps                                         │     │
│  │     - Extract weather metrics                                      │     │
│  │     - Join with dim_locations                                      │     │
│  │  5. Create aggregations:                                           │     │
│  │     - Group by: year, month, hour, location_id                     │     │
│  │     - Apply window functions (avg, min, max)                       │     │
│  │  6. Write to Apache Iceberg format:                                │     │
│  │     - weather_actual_data_timeseries                               │     │
│  │     - weather_actual_data_timeseries_agg                           │     │
│  │     - weather_forecast_data_timeseries                             │     │
│  │     - dim_locations                                                │     │
│  │  7. Update AWS Glue Data Catalog with metadata                     │     │
│  │                                                                    │     │
│  │  Features: ACID transactions, Schema evolution, Partitioning       │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │ Writes Iceberg
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA LAKE - WAREHOUSE ZONE                               │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │              Amazon S3: weather-datalake-projects/                 │     │
│  │                pipeline-weather-data/warehouse/                    │     │
│  │                  warehouse_weather_data.db/                        │     │
│  │                                                                    │     │
│  │  ├── dim_locations/                                                │     │
│  │  │   ├── data/ (Parquet files)                                     │     │
│  │  │   └── metadata/ (Iceberg metadata)                              │     │
│  │  │                                                                 │     │
│  │  ├── weather_actual_data_timeseries/                               │     │
│  │  │   ├── data/ (Parquet files)                                     │     │
│  │  │   └── metadata/ (Iceberg metadata)                              │     │
│  │  │                                                                 │     │
│  │  ├── weather_actual_data_timeseries_agg/                           │     │
│  │  │   ├── data/ (Parquet files)                                     │     │
│  │  │   └── metadata/ (Iceberg metadata)                              │     │
│  │  │                                                                 │     │
│  │  └── weather_forecast_data_timeseries/                             │     │
│  │      ├── data/ (Parquet files)                                     │     │
│  │      └── metadata/ (Iceberg metadata)                              │     │
│  │                                                                    │     │
│  │  Apache Iceberg Tables: ACID, Time Travel, Schema Evolution        │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                    AWS Glue Data Catalog                           │     │
│  │                                                                    │     │
│  │  Database: warehouse_weather_data                                  │     │
│  │  Tables:                                                           │     │
│  │    - dim_locations                                                 │     │
│  │    - weather_actual_data_timeseries                                │     │
│  │    - weather_actual_data_timeseries_agg                            │     │
│  │    - weather_forecast_data_timeseries                              │     │
│  │                                                                    │     │
│  │  Stores: Schema, Partitions, Statistics, Snapshots                 │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└───────────────────┬─────────────────────────────────────────────────────────┘
                    │
                    │ Reads metadata
                    │ Reads data (via External Volume)
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       DATA WAREHOUSE LAYER                                  │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                        Snowflake                                   │     │
│  │                                                                    │     │
│  │  Configuration:                                                    │     │
│  │  1. External Volume → Points to S3 bucket                          │     │
│  │  2. Catalog Integration → AWS Glue Data Catalog                    │     │
│  │  3. Create Iceberg Tables:                                         │     │
│  │     - EXTERNAL_VOLUME = 'warehouse_weather_data_vol'               │     │
│  │     - CATALOG = 'glueCatalog_WarehouseWeatherData'                 │     │
│  │     - AUTO_REFRESH = TRUE                                          │     │
│  │                                                                    │     │
│  │  Tables (Read-Only):                                               │     │
│  │    - dim_locations                                                 │     │
│  │    - weather_actual_data_timeseries                                │     │
│  │    - weather_actual_data_timeseries_agg                            │     │
│  │    - weather_forecast_data_timeseries                              │     │
│  │                                                                    │     │
│  │  Features:                                                         │     │
│  │    - Separation of compute and storage                             │     │
│  │    - Automatic scaling                                             │     │
│  │    - Time travel queries                                           │     │
│  │    - Complex analytics and joins                                   │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 │ SQL Queries
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ANALYTICS & VISUALIZATION                              │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                       Tableau Desktop                              │     │
│  │                                                                    │     │
│  │  Workbook: Weather Analysis.twbx                                   │     │
│  │                                                                    │     │
│  │  Visualizations:                                                   │     │
│  │    - Temperature trends over time                                  │     │
│  │    - Forecast vs Actual comparison                                 │     │
│  │    - Location-based weather patterns                               │     │
│  │    - Hourly/Daily/Monthly aggregations                             │     │
│  │    - Precipitation and wind speed analysis                         │     │
│  │                                                                    │     │
│  │  Connects to Snowflake for live data access                        │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                      Amazon Athena                                 │     │
│  │                  (Alternative Query Engine)                        │     │
│  │                                                                    │     │
│  │  - Queries Iceberg tables directly from Glue Catalog               │     │
│  │  - Serverless SQL queries on S3 data                               │     │
│  │  - Used for validation and debugging                               │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────┐
│                    INFRASTRUCTURE AS CODE (IaC)                             │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                        Terraform                                   │     │
│  │                                                                    │     │
│  │  Provisions and manages:                                           │     │
│  │    - S3 buckets with lifecycle policies                            │     │
│  │    - IAM roles and policies                                        │     │
│  │    - AWS Glue jobs and database                                    │     │
│  │    - AWS CodeBuild projects for CI/CD                              │     │
│  │    - Lambda functions and triggers                                 │     │
│  │                                                                    │     │
│  │  Benefits: Version control, Reproducibility, Automation            │     │
│  └────────────────────────────────────────────────────────────────────┘     │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                      AWS CodeBuild                                 │     │
│  │                                                                    │     │
│  │  CI/CD Pipelines:                                                  │     │
│  │    - Lambda deployment (buildspec.yml)                             │     │
│  │    - Glue job deployment (buildspec.yml)                           │     │
│  │    - Automated testing and validation                              │     │
│  │    - Artifact management                                           │     │
│  └────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘