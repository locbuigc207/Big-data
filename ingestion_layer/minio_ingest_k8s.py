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
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("="*60)
print("KAFKA TO MINIO INGESTION LAYER")
print("="*60)
print(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
print("="*60)

# Read from Kafka - Subscribe to all 6 event topics
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "auth_topic,assessment_topic,video_topic,course_topic,profile_topic,notification_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Write Raw Data to MinIO (S3-compatible) using Checkpointing
# Store raw JSON events as-is, no transformation at ingestion layer
# Partitioned by topic for efficient querying in batch layer
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp") \
    .writeStream \
    .format("parquet") \
    .option("path", "s3a://bucket-0/master_dataset/") \
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/ingest/") \
    .partitionBy("topic") \
    .trigger(processingTime="1 minute") \
    .start()

print("="*60)
print("INGESTION LAYER STARTED")
print("="*60)
print("Topics: auth_topic, assessment_topic, video_topic, course_topic, profile_topic, notification_topic")
print("Destination: s3a://bucket-0/master_dataset/")
print("Checkpoint: s3a://bucket-0/checkpoints/ingest/")
print("Partitioning: By topic")
print("Trigger: Every 1 minute")
print("="*60)
print("Streaming query running... Press Ctrl+C to stop")
print("="*60)

query.awaitTermination()
