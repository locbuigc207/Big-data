from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
import glob

def create_spark_session(app_name="Batch_Job"):
    """Initialize Spark Session with MinIO configuration and S3 JARs"""
    
    # Debug: Check if JARs exist (to verify Docker build)
    print(f"DEBUG: Checking /app/jars content:")
    jars = glob.glob("/app/jars/*.jar")
    print(jars)
    
    # JAR paths
    jar_list = "/app/jars/hadoop-aws-3.3.4.jar,/app/jars/aws-java-sdk-bundle-1.12.262.jar"
    classpath = "/app/jars/hadoop-aws-3.3.4.jar:/app/jars/aws-java-sdk-bundle-1.12.262.jar"

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.connection.ttl", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "3600") \
        .config("spark.jars", jar_list) \
        .config("spark.driver.extraClassPath", classpath) \
        .config("spark.executor.extraClassPath", classpath) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def read_topic_data(spark, input_path, topic):
    """Safely read a specific topic partition, returning an empty DF if not found"""
    try:
        # Instead of reading the subfolder directly which causes [PATH_NOT_FOUND]
        # We read the root and filter. This handles missing partitions gracefully.
        return spark.read.parquet(input_path).filter(f"topic = '{topic}'")
    except Exception as e:
        print(f"INFO: Topic partition {topic} not found in {input_path} (likely no events yet). Returning empty DataFrame.")
        schema = StructType([
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("topic", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        return spark.createDataFrame([], schema)
