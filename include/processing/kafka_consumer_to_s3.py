import json
import boto3
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from pydantic import BaseModel, ValidationError, Field
from typing import Optional

class LogisticsEvent(BaseModel):
    tracking_number: str
    scan_type: str = Field(..., pattern="^(PICKUP|TRANSIT|DELIVERED|UNKNOWN_ERROR)$")
    timestamp: datetime
    ping_latency_ms: Optional[float] = None

def consume_and_upload_to_bronze():
    conf = {
        'bootstrap.servers': 'host.docker.internal:9092', 
        'group.id': 'ledger-sync-consumer-v2',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**conf)
    consumer.subscribe(['ledger-sync-logistics-stream'])
    
    s3 = boto3.client('s3', region_name='us-east-1')
    BRONZE_BUCKET = 'ledger-sync-bronze'
    
    valid_records = []
    print("Consuming batch from Kafka...", flush=True)
    
    msg_count = 0
    empty_polls = 0
    
    while msg_count < 100 and empty_polls < 5:
        msg = consumer.poll(1.0)
        
        if msg is None: 
            empty_polls += 1
            print(f"Waiting for data... ({empty_polls}/5)", flush=True)
            continue
            
        if msg.error():
            print(f"Consumer error: {msg.error()}", flush=True)
            continue
        
        empty_polls = 0    
            
        try:
            raw_data = json.loads(msg.value().decode('utf-8'))
            validated_event = LogisticsEvent(**raw_data)
            valid_records.append(validated_event.dict())
        except ValidationError as e:
            print(f"Schema Validation Error (Sent to DLQ): {raw_data.get('tracking_number', 'Unknown')}", flush=True)
        
        msg_count += 1

    print(f"Finished polling. Gathered {len(valid_records)} valid records.", flush=True)

    if valid_records:
        file_name = f"batch_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
        json_data = json.dumps(valid_records, default=str)
        s3.put_object(Bucket=BRONZE_BUCKET, Key=file_name, Body=json_data)
        print(f"Successfully uploaded {len(valid_records)} records to Bronze S3.", flush=True)
    else:
        print("No valid records to upload.", flush=True)

    consumer.close()

if __name__ == "__main__":
    consume_and_upload_to_bronze()