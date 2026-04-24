provider "aws" {
  region = "us-east-1"
}

# S3 Bucket for the Iceberg/Parquet Lakehouse
resource "aws_s3_bucket" "ledger_sync_lakehouse" {
  bucket = "ledger-sync-lakehouse-prod"
}

# Enable S3 Versioning for data safety
resource "aws_s3_bucket_versioning" "lakehouse_versioning" {
  bucket = aws_s3_bucket.ledger_sync_lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# AWS Glue Database for cataloging
resource "aws_glue_catalog_database" "ledger_sync_db" {
  name = "ledger_sync_logistics_db"
  description = "Glue catalog for Ledger Sync Iceberg tables"
}