from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, approx_count_distinct, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, ArrayType, TimestampType
)
import sys

import os

# Configuration from Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

# Initialize Spark Session with Kafka support and S3A JARs
jar_list = "/app/jars/hadoop-aws-3.3.4.jar,/app/jars/aws-java-sdk-bundle-1.12.262.jar"
classpath = "/app/jars/hadoop-aws-3.3.4.jar:/app/jars/aws-java-sdk-bundle-1.12.262.jar"

spark = SparkSession.builder \
    .appName("SpeedLayer_RealTimeProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.jars", jar_list) \
    .config("spark.driver.extraClassPath", classpath) \
    .config("spark.executor.extraClassPath", classpath) \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.default.parallelism", "20") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "20") \
    .config("spark.hadoop.fs.s3a.threads.max", "15") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
    .config("spark.hadoop.fs.s3a.connection.ttl", "60000") \
    .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "3600") \
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Superset Schema (contains all possible fields from producer.py)
common_schema = StructType([
    # Common
    StructField("event_category", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("role", StringType()),
    StructField("timestamp", StringType()),
    
    # Auth
    StructField("session_id", StringType()),
    StructField("email", StringType()),
    StructField("registration_source", StringType()),
    
    # Assessment
    StructField("assignment_id", StringType()),
    StructField("assignment_type", StringType()),
    StructField("file_url", StringType()),
    StructField("quiz_id", StringType()),
    StructField("correct_answers", IntegerType()),
    StructField("student_id", StringType()),
    StructField("score", IntegerType()),
    StructField("new_due_date", StringType()),
    
    # Video
    StructField("video_id", StringType()),
    StructField("watch_duration_seconds", IntegerType()),
    
    # Course
    StructField("course_id", StringType()),
    StructField("course_name", StringType()),
    StructField("course_code", StringType()),
    StructField("semester", StringType()),
    StructField("max_students", IntegerType()),
    StructField("item_id", StringType()),
    StructField("item_type", StringType()),
    StructField("material_id", StringType()),
    
    # Profile
    StructField("full_name", StringType()),
    StructField("date_of_birth", StringType()),
    StructField("gender", StringType()),
    StructField("phone_number", StringType()),
    StructField("major", StringType()),
    StructField("enrollment_year", IntegerType()),
    StructField("picture_url", StringType()),
    StructField("updated_fields", ArrayType(StringType())),
    
    # Notification
    StructField("notification_id", StringType()),
    StructField("notification_type", StringType()),
    StructField("channel", StringType()),
    StructField("related_object_id", StringType())
])

# 1. READ FROM KAFKA
# Subscribe to all 6 topics
topics = "auth_topic,assessment_topic,video_topic,course_topic,profile_topic,notification_topic"

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", topics) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), common_schema).alias("data")) \
    .select("data.*")

# Handle timestamp
df_clean = df_parsed.withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("processing_time", current_timestamp())

# Define Watermark - Reduced to 1 minute to decrease dashboard lag
df_watermarked = df_clean.withWatermark("timestamp", "1 minute")

# 2. DEFINE AGGREGATIONS

# Aggregation 1: Real-time Active Users (DAU equivalent, but minute-level)
# Count unique users login/active in the last window
rt_active_users = df_watermarked \
    .filter(col("event_type") == "LOGIN") \
    .groupBy(
        window(col("timestamp"), "1 minutes", "30 seconds")
    ) \
    .agg(approx_count_distinct("user_id").alias("active_users")) \
    .select("window.start", "window.end", "active_users")

# Aggregation 2: Real-time Course Popularity
# Count VIEW/INTERACTION events per course in the last window
rt_course_popularity = df_watermarked \
    .filter(col("course_id").isNotNull()) \
    .groupBy(
        window(col("timestamp"), "1 minutes", "1 minutes"),
        col("course_id")
    ) \
    .agg(count("*").alias("interactions")) \
    .select("window.start", "window.end", "course_id", "interactions")

# Aggregation 3: Real-time Video Engagement
# Total watch duration (in seconds, summed) per video in the last window
# Note: VIDEO_WATCHED event has 'watch_duration_seconds'
rt_video_engagement = df_watermarked \
    .filter((col("event_category") == "VIDEO") & (col("event_type") == "VIDEO_WATCHED")) \
    .groupBy(
        window(col("timestamp"), "1 minutes", "1 minutes"),
        col("video_id")
    ) \
    .agg(
        count("*").alias("views"),
        # sum("watch_duration_seconds").alias("total_watch_seconds") # sum might be heavy if many events, stick to count for simplicity or add sum if schema allows (it does)
    ) \
    .select("window.start", "window.end", "video_id", "views")


# 3. WRITE TO STORAGE (MinIO) & CONSOLE

# We use Checkpointing to ensure fault tolerance.
# Location: s3a://bucket-0/checkpoints/...
# Output: s3a://bucket-0/speed_views/...

def write_to_minio(df, epoch_id, path):
    """Batch writer function to append data to MinIO"""
    # We use Append mode for batch write
    df.persist()
    print(f"Writing batch {epoch_id} to {path}")
    df.write \
        .mode("append") \
        .parquet(path)
    df.unpersist()

# Write Stream 1: Active Users
query_dau = rt_active_users.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://bucket-0/speed_views/active_users") \
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/active_users") \
    .trigger(processingTime="30 seconds") \
    .start()

# Write Stream 2: Course Popularity
query_course = rt_course_popularity.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://bucket-0/speed_views/course_popularity") \
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/course_popularity") \
    .trigger(processingTime="30 seconds") \
    .start()

# Write Stream 3: Video Engagement
query_video = rt_video_engagement.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://bucket-0/speed_views/video_engagement") \
    .option("checkpointLocation", "s3a://bucket-0/checkpoints/video_engagement") \
    .trigger(processingTime="30 seconds") \
    .start()

# Console output disabled to save memory
# Re-enable for debugging by uncommenting below:
# query_console = rt_active_users.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("numRows", "5") \
#     .option("truncate", "false") \
#     .trigger(processingTime="2 minutes") \
#     .start()

print("Speed Layer Streams Started...")
print(f"Active Queries: {len(spark.streams.active)}")
spark.streams.awaitAnyTermination()