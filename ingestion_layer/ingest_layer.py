from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType

# Initialize Spark
spark = SparkSession.builder \
    .appName("KafkaToHDFS_Ingestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()

# Read from Kafka - Subscribe to all 6 event topics
# Use "localhost:9092" for local execution, "kafka:29092" for Docker execution
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "auth_topic,assessment_topic,video_topic,course_topic,profile_topic,notification_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Write Raw Data to HDFS (Append mode) using Checkpointing
# Store raw JSON events as-is, no transformation at ingestion layer
# Partitioned by topic for efficient querying in batch layer
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp") \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data/master_dataset/") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/ingest/") \
    .partitionBy("topic") \
    .trigger(processingTime="1 minute") \
    .start()

print("="*60)
print("INGESTION LAYER - Kafka to HDFS")
print("="*60)
print("Topics: auth, assessment, video, course, profile, notification")
print("Destination: hdfs://namenode:9000/data/master_dataset/")
print("Checkpoint: hdfs://namenode:9000/checkpoints/ingest/")
print("Partitioning: By topic")
print("Trigger: Every 1 minute")
print("="*60)
query.awaitTermination()
