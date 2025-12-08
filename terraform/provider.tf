provider "aws" {
  region  = "us-east-1" # Change this to your desired region
#   profile = "dev"
}

data "aws_caller_identity" "current" {}

