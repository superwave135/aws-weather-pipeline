provider "aws" {
  region  = "ap-southeast-1" # Changed to singapore region
#   profile = "dev"
}

data "aws_caller_identity" "current" {}

