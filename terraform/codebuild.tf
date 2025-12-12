# ----------------------------------------------------------------------------------
# CodeBuild Project for Lambda Deployment ### to update necessary vars' values 12 Dec 2025
# ----------------------------------------------------------------------------------
resource "aws_codebuild_project" "deploy_lambda_pipeline_weather_data" {
    name          = "deploy-lambda-pipeline-weather-data"
    description   = "CodeBuild project to deploy Lambda functions for pipeline-weather-data"
    build_timeout = 60 # minutes

    service_role = aws_iam_role.codebuild_role.arn

    artifacts {
        type = "NO_ARTIFACTS"
    }

    environment {
        compute_type                = "BUILD_GENERAL1_SMALL"
        image                       = "aws/codebuild/standard:5.0"
        type                        = "LINUX_CONTAINER"
        privileged_mode             = false
        environment_variable {
            name  = "AWS_REGION"
            value = "us-east-1" # Replace with your AWS region
        }
    }

    source {
        type      = "GITHUB" # Using GitHub as source
        location  = "https://github.com/${var.github_owner}/${var.github_repo}.git" # Constructing GitHub URL

        git_clone_depth = 1 # Optional: Clones only the latest commit

        git_submodules_config { # Optional: if you use git submodules
        fetch_submodules = false
        }

        buildspec = "${local.lambda_script_path_in_github}/buildspec.yml" # Path to external buildspec for Lambda - ADJUST PATH
    }

    source_version = "main"

}

resource "aws_codebuild_webhook" "codebuild_webhook_lambda" {
  project_name = aws_codebuild_project.deploy_lambda_pipeline_weather_data.name
  build_type   = "BUILD"
  filter_group {
    filter {
        type    = "EVENT"
        pattern = "PUSH"
    }

    filter {
        type    = "FILE_PATH"
        pattern = "${local.lambda_script_path_in_github}/*"
    }
  }
}

# ----------------------------------------------------------------------------------
# CodeBuild Project for Glue Jobs Deployment
# ----------------------------------------------------------------------------------

# JOB TRANSFORM ACTUAL WEATHER DATA
resource "aws_codebuild_project" "deploy_glue_job_transform_weather_data" {
    name          = "deploy-glue-job-transform-weather-data"
    description   = "CodeBuild project to deploy Glue job-transform-weather-data from pipeline-weather-data"
    build_timeout = 60 # minutes

    service_role = aws_iam_role.codebuild_role.arn # Reusing the same role for simplicity

    artifacts {
        type = "NO_ARTIFACTS"
    }

    environment {
        compute_type                = "BUILD_GENERAL1_SMALL"
        image                       = "aws/codebuild/standard:5.0"
        type                        = "LINUX_CONTAINER"
        privileged_mode             = false
        environment_variable {
            name  = "AWS_REGION"
            value = "us-east-1" # Replace with your AWS region
        }
    }

    source {
        type      = "GITHUB" # Using GitHub as source
        location  = "https://github.com/${var.github_owner}/${var.github_repo}.git" # Constructing GitHub URL

        git_clone_depth = 1 # Optional: Clones only the latest commit

        git_submodules_config { # Optional: if you use git submodules
        fetch_submodules = false
        }

        buildspec = "${local.glue_transform_weather_data_script_path_in_github}/buildspec.yml"
    }

    source_version = "main"

}

resource "aws_codebuild_webhook" "codebuild_webhook_glue_job_transform_weather_data" {
  project_name = aws_codebuild_project.deploy_glue_job_transform_weather_data.name
  build_type   = "BUILD"
  filter_group {
    filter {
        type    = "EVENT"
        pattern = "PUSH"
    }

    filter {
        type    = "FILE_PATH"
        pattern = "${local.glue_transform_weather_data_script_path_in_github}/*"
    }
  }
}


