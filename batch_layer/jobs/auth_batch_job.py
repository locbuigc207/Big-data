"""
Auth Batch Job - Authentication Analytics
Precomputes authentication-related batch views from raw events in MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, date_format, hour, 
    min as spark_min, max as spark_max, avg, 
    window, unix_timestamp, when, sum as spark_sum,
    from_json
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
from datetime import datetime

from spark_config import create_spark_session, read_topic_data

def get_auth_schema():
    """Define schema for AUTH events"""
    return StructType([
        StructField("event_category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("role", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registration_source", StringType(), True)
    ])

def compute_daily_active_users(df):
    """Compute daily active users (DAU)"""
    return df.filter(col("event_type") == "LOGIN") \
        .withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date") \
        .agg(
            countDistinct("user_id").alias("daily_active_users"),
            countDistinct("session_id").alias("total_sessions")
        ) \
        .orderBy("date")

def compute_hourly_login_patterns(df):
    """Compute hourly login patterns for peak usage analysis"""
    return df.filter(col("event_type") == "LOGIN") \
        .withColumn("hour", hour(col("timestamp_parsed"))) \
        .withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "hour") \
        .agg(count("user_id").alias("login_count")) \
        .orderBy("date", "hour")

def compute_user_session_metrics(df):
    """Compute session duration metrics per user"""
    # Get LOGIN and LOGOUT pairs
    logins = df.filter(col("event_type") == "LOGIN") \
        .select(
            col("user_id"),
            col("session_id"),
            col("timestamp_parsed").alias("login_time")
        )
    
    logouts = df.filter(col("event_type") == "LOGOUT") \
        .select(
            col("session_id"),
            col("timestamp_parsed").alias("logout_time")
        )
    
    # Join to calculate session duration
    sessions = logins.join(logouts, "session_id", "inner") \
        .withColumn(
            "session_duration_minutes",
            (unix_timestamp("logout_time") - unix_timestamp("login_time")) / 60
        )
    
    # Aggregate per user
    return sessions.groupBy("user_id") \
        .agg(
            count("session_id").alias("total_sessions"),
            avg("session_duration_minutes").alias("avg_session_duration_minutes"),
            spark_min("session_duration_minutes").alias("min_session_duration_minutes"),
            spark_max("session_duration_minutes").alias("max_session_duration_minutes"),
            spark_sum("session_duration_minutes").alias("total_time_online_minutes")
        ) \
        .orderBy(col("total_sessions").desc())

def compute_user_activity_summary(df):
    """Compute overall user activity summary"""
    return df.groupBy("user_id", "role") \
        .agg(
            count(when(col("event_type") == "LOGIN", 1)).alias("total_logins"),
            count(when(col("event_type") == "LOGOUT", 1)).alias("total_logouts"),
            spark_min("timestamp_parsed").alias("first_activity"),
            spark_max("timestamp_parsed").alias("last_activity"),
            countDistinct("session_id").alias("unique_sessions")
        ) \
        .orderBy(col("total_logins").desc())

def compute_registration_analytics(df):
    """Compute registration analytics"""
    signups = df.filter(col("event_type") == "SIGNUP")
    
    return signups.withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "registration_source", "role") \
        .agg(count("user_id").alias("new_signups")) \
        .orderBy("date")

def main(input_path, output_path):
    """Main batch job execution"""
    spark = create_spark_session("Auth_Batch_Job")
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[AUTH BATCH] Reading auth events from: {input_path}")
    
    # Read raw auth events from MinIO (partitioned by topic=auth_topic)
    df_raw = read_topic_data(spark, input_path, "auth_topic")
    
    # Parse JSON body
    json_schema = get_auth_schema()
    df_parsed = df_raw.withColumn("data", from_json(col("value").cast("string"), json_schema)).select("data.*")
    
    # Parse timestamp
    df = df_parsed.withColumn("timestamp_parsed", col("timestamp").cast(TimestampType()))
    
    print(f"[AUTH BATCH] Total auth events: {df.count()}")
    
    # Compute batch views
    print("[AUTH BATCH] Computing daily active users...")
    dau = compute_daily_active_users(df)
    dau.write.mode("overwrite").parquet(f"{output_path}/auth_daily_active_users")
    
    print("[AUTH BATCH] Computing hourly login patterns...")
    hourly_patterns = compute_hourly_login_patterns(df)
    hourly_patterns.write.mode("overwrite").parquet(f"{output_path}/auth_hourly_login_patterns")
    
    print("[AUTH BATCH] Computing user session metrics...")
    session_metrics = compute_user_session_metrics(df)
    session_metrics.write.mode("overwrite").parquet(f"{output_path}/auth_user_session_metrics")
    
    print("[AUTH BATCH] Computing user activity summary...")
    activity_summary = compute_user_activity_summary(df)
    activity_summary.write.mode("overwrite").parquet(f"{output_path}/auth_user_activity_summary")
    
    print("[AUTH BATCH] Computing registration analytics...")
    registration_analytics = compute_registration_analytics(df)
    registration_analytics.write.mode("overwrite").parquet(f"{output_path}/auth_registration_analytics")
    
    print("[AUTH BATCH] Auth batch job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: auth_batch_job.py <input_path> <output_path>")
        print("Example: auth_batch_job.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
