from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType
import os

# Get configuration from environment variables (for K8s deployment)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Initialize Spark with MinIO/S3A configuration
spark = SparkSession.builder \
    .appName("KafkaToMinIO_Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
    .config("spark.hadoop.fs.s3a.multipart.size", "67108864") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
    .config("spark.hadoop.fs.s3a.connection.ttl", "60000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum.interval", "20") \
    .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "3600") \
    .getOrCreate()

# Start Metadata Printing
print("="*60)
print("KAFKA TO MINIO INGESTION LAYER")
print(f"Time: {os.popen('date').read().strip()}")
print("="*60)
print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print("="*60, flush=True)

# Read from Kafka - Subscribe to all 6 event topics
print("Initializing Kafka connection...", flush=True)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "auth_topic,assessment_topic,video_topic,course_topic,profile_topic,notification_topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
print("Kafka connection established.", flush=True)

# Write Raw Data to MinIO (S3-compatible)
print("Configuring streaming query...", flush=True)
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp") \
    .writeStream \
    .format("parquet") \
    .option("path", "s3a://bucket-0/master_dataset/") \
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/ingest/") \
    .partitionBy("topic") \
    .trigger(processingTime="30 seconds") \
    .start()

import time

print("="*60)
print("INGESTION LAYER STARTED SUCCESSFULLY")
print("="*60)
print("Destination: s3a://bucket-0/master_dataset/")
print("Checkpoint: s3a://bucket-0/checkpoints/ingest/")
print("Trigger: Every 1 minute")
print("Monitoring batch progress (printing every 10s)...")
print("="*60, flush=True)

try:
    while query.isActive:
        if query.lastProgress:
            rows = query.lastProgress.get('numInputRows', 0)
            status = query.status.get('message', 'Processing')
            print(f"[{time.strftime('%H:%M:%S')}] Status: {status} | Last Batch Rows: {rows}", flush=True)
            if rows > 0:
                print(f"  > Sources: {query.lastProgress.get('sources', [])}")
        else:
            print(f"[{time.strftime('%H:%M:%S')}] Query active, waiting for first batch...", flush=True)
        
        time.sleep(10)
    
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping Ingestion Layer...")
    query.stop()
except Exception as e:
    print(f"Error: {e}")
    query.stop()

