"""
Kafka to MinIO Consumer
-----------------------
Consumes CDC messages from Kafka and writes to MinIO as Parquet files.
Flow: Kafka → Python Consumer → MinIO (S3-compatible storage)
"""

import boto3
from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

# Load environment variables from .env file
load_dotenv()

# Kafka settings
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092')
KAFKA_GROUP = os.getenv('KAFKA_GROUP', 'banking-consumer-group')

# MinIO settings
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'raw')

# Kafka topics to consume
TOPICS = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions'
]

# Batch size for writing to MinIO
BATCH_SIZE = 50

# ============================================================================
# KAFKA CONSUMER SETUP
# ============================================================================

def create_kafka_consumer():
    """Create and configure Kafka consumer"""
    
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': KAFKA_GROUP,
        'auto.offset.reset': 'earliest',        # Start from beginning if no offset
        'enable.auto.commit': True,             # Auto-commit offsets
        'auto.commit.interval.ms': 5000,        # Commit every 5 seconds
        
        # Timeouts
        'session.timeout.ms': 60000,            # 60 seconds
        'heartbeat.interval.ms': 20000,         # 20 seconds
        'max.poll.interval.ms': 300000,         # 5 minutes
        
        # Fetch limits (prevent memory issues)
        'fetch.max.bytes': 52428800,            # 50 MB
        'max.partition.fetch.bytes': 1048576,   # 1 MB per partition
    }
    
    print("🔗 Creating Kafka consumer...")
    print(f"   Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"   Group: {KAFKA_GROUP}")
    print(f"   Topics: {len(TOPICS)}\n")
    
    consumer = Consumer(config)
    consumer.subscribe(TOPICS)
    
    # Wait for partition assignment
    print("⏳ Waiting for partition assignment...")
    for attempt in range(1, 11):
        msg = consumer.poll(timeout=5.0)
        if consumer.assignment():
            print(f"✅ Assigned {len(consumer.assignment())} partitions\n")
            return consumer
        print(f"   Attempt {attempt}/10...")
    
    raise Exception("Failed to get partition assignment after 10 attempts")

# ============================================================================
# MINIO CLIENT SETUP
# ============================================================================

def create_minio_client():
    """Create and configure MinIO (S3) client"""
    
    print("☁️  Connecting to MinIO...")
    print(f"   Endpoint: {MINIO_ENDPOINT}")
    print(f"   Bucket: {MINIO_BUCKET}\n")
    
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    # Create bucket if it doesn't exist
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if MINIO_BUCKET not in buckets:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"✅ Created bucket: {MINIO_BUCKET}")
    else:
        print(f"✅ Bucket exists: {MINIO_BUCKET}")
    
    return s3

# ============================================================================
# DATA PROCESSING
# ============================================================================

def extract_record(message):
    """
    Extract record from Debezium CDC message
    
    Debezium format:
    {
        "payload": {
            "before": {...},  # Old data (for updates/deletes)
            "after": {...},   # New data (for inserts/updates)
            "op": "c"         # Operation: c=create, u=update, d=delete
        }
    }
    """
    try:
        event = json.loads(message.value().decode('utf-8'))
        payload = event.get("payload", {})
        return payload.get("after")  # Return new record data
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"⚠️  Error parsing message: {e}")
        return None

def write_to_minio(s3_client, table_name, records):
    """
    Write records to MinIO as Parquet file
    
    Args:
        s3_client: boto3 S3 client
        table_name: Name of the table (e.g., 'customers')
        records: List of record dictionaries
    """
    if not records:
        return
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Generate file paths
        date_str = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%H%M%S%f')
        
        # Temporary local file
        local_path = f'/tmp/{table_name}_{int(time.time())}.parquet'
        
        # S3 key (MinIO path)
        s3_key = f'{table_name}/date={date_str}/{table_name}_{timestamp}.parquet'
        
        # Write Parquet file
        df.to_parquet(local_path, engine='fastparquet', index=False)
        
        # Upload to MinIO
        s3_client.upload_file(local_path, MINIO_BUCKET, s3_key)
        
        # Clean up local file
        os.remove(local_path)
        
        print(f'✅ Uploaded {len(records)} {table_name} records')
        
    except Exception as e:
        print(f'❌ Upload error for {table_name}: {e}')

# ============================================================================
# MAIN CONSUMER LOOP
# ============================================================================

def main():
    """Main consumer loop"""
    
    print("="*70)
    print("🏦 BANKING CDC CONSUMER - KAFKA TO MINIO")
    print("="*70 + "\n")
    
    # Initialize
    consumer = create_kafka_consumer()
    s3_client = create_minio_client()
    
    # Buffers for batching
    buffers = {topic: [] for topic in TOPICS}
    
    print("\n" + "="*70)
    print("✅ CONSUMER READY - Starting to consume messages...")
    print("="*70)
    print("💡 Press Ctrl+C to stop\n")
    
    # Statistics
    message_count = 0
    last_log_time = time.time()
    
    try:
        while True:
            # Poll for new message (1 second timeout)
            msg = consumer.poll(timeout=1.0)
            
            # No message received
            if msg is None:
                # Log status every 10 seconds
                if time.time() - last_log_time > 10:
                    print(f"⏳ Waiting... (Total processed: {message_count})")
                    last_log_time = time.time()
                continue
            
            # Check for errors
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # End of partition (not an error)
                else:
                    print(f"❌ Consumer error: {msg.error()}")
                    break
            
            # Process message
            topic = msg.topic()
            record = extract_record(msg)
            
            if record:
                # Add to buffer
                buffers[topic].append(record)
                message_count += 1
                
                # Log progress every 100 messages
                if message_count % 100 == 0:
                    print(f"📊 Processed {message_count} messages")
                
                # Write to MinIO when buffer is full
                if len(buffers[topic]) >= BATCH_SIZE:
                    table_name = topic.split('.')[-1]  # Extract table name
                    write_to_minio(s3_client, table_name, buffers[topic])
                    buffers[topic] = []  # Clear buffer
    
    except KeyboardInterrupt:
        print("\n⚠️  Shutting down...")
    
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Flush remaining records
        print("\n📦 Flushing remaining records...")
        for topic, records in buffers.items():
            if records:
                table_name = topic.split('.')[-1]
                print(f"   {table_name}: {len(records)} records")
                write_to_minio(s3_client, table_name, records)
        
        # Close consumer
        consumer.close()
        
        print(f"\n✅ Consumer closed gracefully")
        print(f"📊 Total messages processed: {message_count}\n")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()