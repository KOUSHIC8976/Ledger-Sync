import json
import boto3
from pydantic import BaseModel, ValidationError, Field
from typing import Optional
from datetime import datetime

# Pydantic schema for strict validation
class LogisticsEvent(BaseModel):
    event_id: str
    tracking_number: str
    scan_type: str = Field(..., pattern="^(PICKUP|TRANSIT|OUT_FOR_DELIVERY|DELIVERED)$")
    timestamp: datetime
    facility_code: Optional[str] = None
    is_duplicate: bool = False

s3 = boto3.client('s3')
BUCKET_NAME = 'ledger-sync-raw-events'

def lambda_handler(event, context):
    try:
        # Parse incoming webhook payload
        body = json.loads(event.get('body', '{}'))
        
        # Validate payload strictly with Pydantic
        validated_event = LogisticsEvent(**body)
        
        # Save validated event to S3 (Simulating raw ingestion before Iceberg/Kinesis)
        file_name = f"events/{validated_event.tracking_number}_{validated_event.timestamp.isoformat()}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=validated_event.json()
        )
        
        return {"statusCode": 200, "body": "Event Validated and Ingested"}

    except ValidationError as e:
        print(f"Schema Validation Error: {e.json()}")
        return {"statusCode": 400, "body": "Invalid Payload Schema"}
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}