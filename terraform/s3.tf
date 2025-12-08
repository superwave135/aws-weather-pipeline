resource "aws_s3_bucket" "weather_datalake" {
  bucket = "weather-datalake-projects"
  force_destroy = true

  tags = {
    Name        = "weather-datalake-projects"
    Environment = "Dev"
  }

}

resource "aws_s3_bucket_lifecycle_configuration" "weather_datalake_clean_tmp_folder" {
  bucket = aws_s3_bucket.weather_datalake.id

  rule {
    id = "clean-tmp-folder-lifecycle-${var.pipeline_name}" 

    expiration {
      days = 1
    }

    filter {
      prefix = "${var.pipeline_name}/tmp/"
    }

    # ... other transition/expiration actions ...

    status = "Enabled"
  }
}


resource "aws_s3_bucket_ownership_controls" "weather_datalake" {
  bucket = aws_s3_bucket.weather_datalake.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "weather_datalake" {
  bucket = aws_s3_bucket.weather_datalake.id
  acl    = "private"

  depends_on = [
    aws_s3_bucket_ownership_controls.weather_datalake
  ]
}

resource "aws_s3_object" "dim_locations" {
  bucket = aws_s3_bucket.weather_datalake.id
  key    = "${var.pipeline_name}/aux_data/dim_locations.json" # Path inside the bucket
  source = "../datasets/dim_locations.json" # Local path to your file
  etag = filemd5("../datasets/dim_locations.json") # Recommended to track changes
}

# ----------------------------------------------------------------------------------
# S3 Scripts
# ----------------------------------------------------------------------------------

resource "aws_s3_bucket" "weather_datalake_scripts" {
  bucket = "weather-datalake-projects-scripts"
  force_destroy = true

  tags = {
    Name        = "weather-datalake-projects-scripts"
    Environment = "Dev"
  }
}


resource "aws_s3_bucket_ownership_controls" "weather_datalake_scripts" {
  bucket = aws_s3_bucket.weather_datalake_scripts.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "weather_datalake_scripts" {
  bucket = aws_s3_bucket.weather_datalake_scripts.id
  acl    = "private"

  depends_on = [
    aws_s3_bucket_ownership_controls.weather_datalake_scripts
  ]
}

