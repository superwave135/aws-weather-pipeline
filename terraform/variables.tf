variable "github_owner" {
  type        = string
  description = "GitHub repository owner (username or organization)"
  default     = "superwave135"  // not geekytan
}

variable "github_repo" {
  type        = string
  description = "GitHub repository name"
  default     = "aws-weather-pipeline"  // Replaced with repo name
}

variable "account_id" {
  type        = string
  description = "Account Id AWS"
  default     = "881786084229" // Replaced with account id
}

variable "region" {
  type        = string
  description = "Region AWS"
  default     = "ap-southeast-1"
}

variable "lambda_function_name" {
  type        = string
  description = "Lambda function name"
  default     = "get_api_weather_data_lambda"
}

variable "pipeline_name" {
  type        = string
  description = "Name of pipeline"
  default     = "pipeline-weather-data"
}

///////// SNOWFLAKE VARIABLES CONNECTIONS /////////////////

variable "aws_iam_user_arn_by_snowflake" {
  type        = string
  description = "Iam user ARN by snowflake"
  default     = "arn:aws:iam::706599862050:user/3q191000-s" // Replace with your Iam user ARN by snowflake
}

variable "glue_aws_external_id_by_snowflake" {
  type        = string
  description = "Glue aws external id by snowflake"
  default     = "EWB70839_SFCRole=2_n9ydkXx5o8MiqdDYaOgyfWzFncc=" # ← UPDATED on 19dec2025
}

variable "storage_aws_external_id_by_snowflake" {
  type        = string
  description = "Storage aws external id by snowflake"
  default     = "EWB70839_SFCRole=2_hz4x/Y+sqn/rcHhQkraOCyu+fto=" # ← UPDATED on 19dec2025
}
