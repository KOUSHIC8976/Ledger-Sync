import great_expectations as gx
import pandas as pd
import boto3
import json

def run_quality_checks():
    print("Fetching actual Bronze Data from S3...")
    s3 = boto3.client('s3', region_name='us-east-1')
    BRONZE_BUCKET = 'ledger-sync-bronze'
    
    try:
        response = s3.list_objects_v2(Bucket=BRONZE_BUCKET)
        if 'Contents' not in response:
            print("No data in S3 to validate. Passing gate.")
            return
            
        all_records = []
        for obj in response['Contents']:
            file_obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=obj['Key'])
            all_records.extend(json.loads(file_obj['Body'].read().decode('utf-8')))
            
        df = pd.DataFrame(all_records)
    except Exception as e:
        print(f"Failed to read from S3: {e}")
        raise

    context = gx.get_context()
    data_source = context.data_sources.add_pandas("s3_bronze_source")
    data_asset = data_source.add_dataframe_asset(name="bronze_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("bronze_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    print(f"Validating {len(df)} records in Data Quality Suite...")
    
    exp1 = gx.expectations.ExpectColumnValuesToNotBeNull(column='tracking_number')
    res1 = batch.validate(exp1)
    
    #  `mostly=0.85` to tolerate up to 15% sensor errors in the stream
    exp2 = gx.expectations.ExpectColumnValuesToBeBetween(
        column='ping_latency_ms', 
        min_value=0.0, 
        max_value=10000.0,
        mostly=0.85  
    )
    res2 = batch.validate(exp2)

    if not res1.success or not res2.success:
        print("Data Quality Gate FAILED. Anomaly threshold exceeded.")
        raise ValueError("Data Quality checks failed. Halting pipeline.")
        
    print("Data Quality Gate PASSED. Acceptable anomaly threshold maintained. Proceeding to Silver.")

if __name__ == "__main__":
    run_quality_checks()