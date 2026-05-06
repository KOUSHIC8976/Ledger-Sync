import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:29092'}
producer = Producer(**conf)
TOPIC_NAME = 'ledger-sync-logistics-stream'

def generate_telemetry():
    """Generates logistics events with deliberate physics-defying chaos (negative latencies)."""
    return {
        'tracking_number': f"TRK-{random.randint(1000, 9999)}",
        'scan_type': random.choice(['PICKUP', 'TRANSIT', 'TRANSIT', 'DELIVERED', 'UNKNOWN_ERROR']),
        'timestamp': datetime.utcnow().isoformat(),
        'ping_latency_ms': random.uniform(-10.0, 500.0) if random.random() > 0.1 else -99.0, 
    }

def delivery_report(err, msg):
    """Callback for Kafka to confirm message delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")

print(f"Initiating Chaos Stream to Kafka Topic: {TOPIC_NAME}...")
try:
    while True:
        payload = generate_telemetry()
        producer.produce(
            TOPIC_NAME, 
            key=payload['tracking_number'], 
            value=json.dumps(payload), 
            callback=delivery_report
        )
        producer.poll(0) 
        print(f"Streamed: {payload['tracking_number']} - {payload['scan_type']}")
        time.sleep(random.uniform(0.1, 0.5)) 
except KeyboardInterrupt:
    pass
finally:
    producer.flush() 