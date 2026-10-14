terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "eu-central-1"
}

variable "environment" {
  default = "dev"
}

variable "project_name" {
  default = "dataops"
}

# S3 bucket for raw data
resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${var.environment}"

  tags = {
    Name        = "Raw Data Bucket"
    Environment = var.environment
    Project     = var.project_name
  }
}

# S3 bucket for processed data
resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed-${var.environment}"

  tags = {
    Name        = "Processed Data Bucket"
    Environment = var.environment
    Project     = var.project_name
  }
}

# Enable versioning (backup!)
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "processed_bucket" {
  value = aws_s3_bucket.processed.bucket
}