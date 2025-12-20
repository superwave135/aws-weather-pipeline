resource "aws_iam_role" "glue_service_role" {
  name = "glue_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}


resource "aws_iam_policy" "glue_service_role_policy" {
  name        = "glue_policy"
  description = "Policy for Glue Role to access S3 scripts bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*"
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:s3:::weather-datalake-projects-01",
          "arn:aws:s3:::weather-datalake-projects-01/*"
        ]
      },
      # ✅ ADD THIS NEW BLOCK - Access to scripts bucket
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:s3:::weather-datalake-projects-scripts-01",
          "arn:aws:s3:::weather-datalake-projects-scripts-01/*"
        ]
      },
      {
        Action = [
          "glue:*"
        ],
        Effect   = "Allow",
        Resource = ["*"]
      },
      {
        Action = [
          "cloudwatch:*"
        ]
        Effect   = "Allow",
        Resource = ["*"]
      },
      {
        Action = [
          "logs:*"
        ],
        Effect   = "Allow",
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_role_policy_attachment" {
  role       = aws_iam_role.glue_service_role.id
  policy_arn = aws_iam_policy.glue_service_role_policy.arn
}

# Create an IAM role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "lambda-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# Create an IAM policy for Lambda to read from S3
resource "aws_iam_policy" "lambda_policy" {
  name        = "lambda-policy"
  description = "Policy for Lambda to read from S3 and write logs"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:*"
        ],
        "Resource" :  [
          "arn:aws:s3:::weather-datalake-projects-01",
          "arn:aws:s3:::weather-datalake-projects-01/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "athena:*"
        ],
        "Resource" : "*"
      },
      {  
        "Effect": "Allow",  
        "Action": "logs:CreateLogGroup",  
        "Resource": "arn:aws:logs:${var.region}:${var.account_id}:*"  
      },  
      {  
        "Effect": "Allow",  
        "Action": [  
          "logs:CreateLogStream",  
          "logs:PutLogEvents"  
        ],  
        "Resource": [  
          "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${var.lambda_function_name}:*"  
        ]  
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "glue:*"
        ],
        "Resource" : "*"
    }]
  })
}

# Attach the policy to the Lambda role
resource "aws_iam_role_policy_attachment" "lambda_attachment" {
  policy_arn = aws_iam_policy.lambda_policy.arn
  role       = aws_iam_role.lambda_role.name
}



# ----------------------------------------------------------------------------------
# CodeBuild IAM Role and Policy
# ----------------------------------------------------------------------------------
resource "aws_iam_role" "codebuild_role" {
  name = "codebuild-deploy-pipeline-weather-data-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "codebuild.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "codebuild_policy_attach" {
  name       = "codebuild-policy-attachment"
  policy_arn = aws_iam_policy.codebuild_policy.arn
  roles      = [aws_iam_role.codebuild_role.name]
}

resource "aws_iam_policy" "codebuild_policy" {
  name        = "codebuild-deploy-pipeline-weather-data-policy"
  description = "Policy for CodeBuild to deploy Lambda and Glue for pipeline-weather-data"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow",
        Action = [
          "lambda:*",
          "glue:*",
          "iam:PassRole",
          "iam:CreateRole",
          "iam:GetRole",
          "iam:DeleteRolePolicy",
          "iam:DeleteRole",
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy",
          "iam:PutRolePolicy",
          "s3:*" # Example - Refine as needed
        ],
        Resource = "*" # Refine resources for security best practices
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = aws_iam_role.lambda_role.arn
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = aws_iam_role.glue_service_role.arn
      }
    ]
  })
}




# ----------------------------------------------------------------------------------
# Snowflake IAM Role and Policy
# ----------------------------------------------------------------------------------
resource "aws_iam_role" "snowflake_service_role" {
  name = "snowflake_service_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "${var.aws_iam_user_arn_by_snowflake}"
          },
        Condition = {
          StringEquals = {
              "sts:ExternalId" = "${var.storage_aws_external_id_by_snowflake}"
            }
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "${var.aws_iam_user_arn_by_snowflake}"
          },
        Condition = {
          StringEquals = {
              "sts:ExternalId" = "${var.glue_aws_external_id_by_snowflake}"
            }
        }
      }
    ]
  })
}


resource "aws_iam_policy" "snowflake_service_role_policy" {
  name        = "snowflake_glue_policy"
  description = "Policy for Snowflake to access glue tables"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation" 
        ]
        Effect = "Allow"
        Resource = [
          "arn:aws:s3:::weather-datalake-projects-01",
          "arn:aws:s3:::weather-datalake-projects-01/*"
        ]
      },
      {
        Action = [
          "glue:GetCatalogImportStatus",
          "glue:GetConnection",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTags",
          "glue:SearchTables"
        ],
        Effect   = "Allow",
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "snowflake_role_policy_attachment" {
  role       = aws_iam_role.snowflake_service_role.id
  policy_arn = aws_iam_policy.snowflake_service_role_policy.arn
}
