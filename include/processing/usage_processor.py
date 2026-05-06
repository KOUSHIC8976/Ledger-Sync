import duckdb
import boto3

def process_bronze_to_silver():
    print("Checking S3 for Bronze data...")
    s3 = boto3.client('s3', region_name='us-east-1')
    BRONZE_BUCKET = 'ledger-sync-bronze'
    SILVER_BUCKET = 'ledger-sync-silver'
    
    response = s3.list_objects_v2(Bucket=BRONZE_BUCKET)
    if 'Contents' not in response:
        print("No Bronze data found in S3. Skipping DuckDB processing to prevent IO Error.")
        return

    print("Initializing DuckDB In-Memory Engine...")
    con = duckdb.connect(':memory:')
    
    con.execute("""
        INSTALL httpfs; LOAD httpfs;
        INSTALL aws; LOAD aws;
        CALL load_aws_credentials();
        SET s3_region='us-east-1';
    """)

    bronze_path = f's3://{BRONZE_BUCKET}/*.json'
    silver_path = f's3://{SILVER_BUCKET}/logistics_aggregated.parquet'

    print(f"Reading raw Bronze data from {bronze_path}...")
    
    con.execute(f"""
        COPY (
            SELECT 
                tracking_number, 
                MAX(scan_type) as latest_status,
                AVG(ping_latency_ms) as avg_latency
            FROM read_json_auto('{bronze_path}')
            WHERE ping_latency_ms > 0
            GROUP BY tracking_number
        ) TO '{silver_path}' (FORMAT PARQUET);
    """)
    print("Silver layer successfully written to S3.")

if __name__ == "__main__":
    process_bronze_to_silver()