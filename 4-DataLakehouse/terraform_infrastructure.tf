"""
Data Lakehouse Terraform Configuration
Infrastructure as Code for AWS S3 and Delta Lake
"""

# main.tf - Delta Lake resources

terraform {
  required_version = ">= 1.0"
  
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

# S3 bucket for data lake
resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket_name
  
  tags = {
    Name        = "data-lake"
    Environment = var.environment
  }
}

# Enable versioning for S3 bucket
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 bucket policy for data lake access
resource "aws_s3_bucket_policy" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowSparkAccess"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.spark_job_role.arn
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}

# EMR cluster for Spark jobs
resource "aws_emr_cluster" "spark_cluster" {
  name           = "delta-lake-cluster"
  release_label  = "emr-7.0.0"
  log_uri        = "s3://${aws_s3_bucket.data_lake.id}/logs/"
  service_role   = aws_iam_role.emr_service_role.arn
  
  ec2_attributes {
    instance_profile = aws_iam_instance_profile.spark_job_profile.arn
    key_name         = var.ec2_key_name
    subnet_id        = var.subnet_id
  }
  
  master_node_type      = var.master_instance_type
  core_node_count       = var.core_node_count
  core_node_type        = var.core_instance_type
  task_node_count       = var.task_node_count
  task_node_type        = var.task_instance_type
  
  applications {
    name = "Hadoop"
  }
  
  applications {
    name = "Spark"
  }
  
  configurations = jsonencode([
    {
      "Classification" : "spark-defaults",
      "ConfigurationProperties" : {
        "spark.jars.packages" : "io.delta:delta-core_2.12:2.4.0"
      }
    }
  ])
  
  tags = {
    Environment = var.environment
  }
}

# IAM role for EMR service
resource "aws_iam_role" "emr_service_role" {
  name = "emr-service-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
}

# IAM role for Spark job execution
resource "aws_iam_role" "spark_job_role" {
  name = "spark-job-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

# Instance profile for Spark job
resource "aws_iam_instance_profile" "spark_job_profile" {
  name = "spark-job-profile"
  role = aws_iam_role.spark_job_role.name
}

# IAM policy for S3 access
resource "aws_iam_role_policy" "spark_s3_access" {
  name = "spark-s3-access"
  role = aws_iam_role.spark_job_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:*"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}

# CloudWatch log group for monitoring
resource "aws_cloudwatch_log_group" "delta_lake_logs" {
  name              = "/aws/emr/delta-lake"
  retention_in_days = 7
  
  tags = {
    Environment = var.environment
  }
}

output "s3_bucket_name" {
  value = aws_s3_bucket.data_lake.id
}

output "emr_cluster_id" {
  value = aws_emr_cluster.spark_cluster.id
}

output "emr_cluster_master_dns" {
  value = aws_emr_cluster.spark_cluster.master_public_dns
}
