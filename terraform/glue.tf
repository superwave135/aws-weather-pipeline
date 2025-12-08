resource "aws_glue_catalog_database" "warehouse_weather_data" {
    name = "warehouse_weather_data"
}


resource "aws_glue_catalog_table" "dim_locations" {
  database_name = aws_glue_catalog_database.warehouse_weather_data.name # Reference the database name from the database resource
  name          = "dim_locations"
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
    }
  }

  storage_descriptor {
    location = "s3://weather-datalake-projects/${var.pipeline_name}/warehouse/warehouse_weather_data.db/dim_locations/" 

    columns {
      name = "country"
      type = "string"
    }

    columns {
      name = "name"
      type = "string"
    }

    columns {
      name = "lat"
      type = "double"
    }

    columns {
      name = "lon"
      type = "double"
    }

    columns {
      name = "location_id"
      type = "int"
    }
  }

  depends_on = [aws_glue_catalog_database.warehouse_weather_data] # Explicit dependency on the database
}