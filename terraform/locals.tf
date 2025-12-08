locals {
  pipeline_name = var.pipeline_name

  lambda_script_path_in_bucket = "${var.pipeline_name}/lambda/lambda_code.zip"
  lambda_script_path_in_github = "AWS/${var.pipeline_name}/lambda"
  glue_transform_weather_data_script_path_in_github   = "AWS/${var.pipeline_name}/glue/transform-weather-data"
}

