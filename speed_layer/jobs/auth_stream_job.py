"""
Auth Stream Processing Job
Real-time processing of authentication events from Kafka

Real-time Views Generated:
1. auth_realtime_active_users_1min - Active users in 1-minute windows
2. auth_realtime_active_users_5min - Active users in 5-minute windows  
3. auth_realtime_login_rate - Login events per minute
4. auth_realtime_session_activity - Session metrics (login/logout pairs)
5. auth_realtime_failed_logins - Failed login attempts tracking
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, countDistinct, 
    sum as spark_sum, avg, min as spark_min, max as spark_max,
    current_timestamp, to_timestamp, expr, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType
)
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import speed_config, get_kafka_connection_options, get_output_path, get_checkpoint_path, get_spark_config


# Define schema for auth events
auth_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_category", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("success", BooleanType(), True)
])


def create_spark_session():
    """Create and configure Spark session for streaming"""
    builder = SparkSession.builder \
        .appName("SpeedLayer_AuthStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
    # Apply configurations
    for key, value in get_spark_config().items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark, topic_name):
    """Read streaming data from Kafka topic"""
    kafka_options = get_kafka_connection_options(topic_name)
    
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()
    
    # Parse JSON and extract fields
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), auth_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return parsed_df


def compute_active_users_1min(df):
    """
    Real-time View: auth_realtime_active_users_1min
    Count unique active users in 1-minute tumbling windows
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            countDistinct("user_id").alias("active_users"),
            count("*").alias("total_events"),
            spark_sum(when(col("action") == "LOGIN", 1).otherwise(0)).alias("login_count"),
            spark_sum(when(col("action") == "LOGOUT", 1).otherwise(0)).alias("logout_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "active_users",
            "total_events",
            "login_count",
            "logout_count",
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_active_users_5min(df):
    """
    Real-time View: auth_realtime_active_users_5min
    Count unique active users in 5-minute tumbling windows
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(window(col("timestamp"), "5 minutes")) \
        .agg(
            countDistinct("user_id").alias("active_users"),
            count("*").alias("total_events"),
            spark_sum(when(col("action") == "LOGIN", 1).otherwise(0)).alias("login_count"),
            spark_sum(when(col("action") == "LOGOUT", 1).otherwise(0)).alias("logout_count"),
            spark_sum(when(col("action") == "REGISTER", 1).otherwise(0)).alias("register_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "active_users",
            "total_events",
            "login_count",
            "logout_count",
            "register_count",
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_login_rate(df):
    """
    Real-time View: auth_realtime_login_rate
    Calculate login rate per minute (successful vs failed)
    """
    result = df \
        .filter(col("action") == "LOGIN") \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            count("*").alias("total_logins"),
            spark_sum(when(col("success") == True, 1).otherwise(0)).alias("successful_logins"),
            spark_sum(when(col("success") == False, 1).otherwise(0)).alias("failed_logins"),
            countDistinct("user_id").alias("unique_users")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_logins",
            "successful_logins",
            "failed_logins",
            "unique_users",
            expr("ROUND((successful_logins / total_logins) * 100, 2)").alias("success_rate_pct"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_session_activity(df):
    """
    Real-time View: auth_realtime_session_activity
    Track active sessions using 5-minute windows with sliding
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            "user_id"
        ) \
        .agg(
            count("*").alias("event_count"),
            spark_min("timestamp").alias("first_event"),
            spark_max("timestamp").alias("last_event"),
            spark_sum(when(col("action") == "LOGIN", 1).otherwise(0)).alias("logins"),
            spark_sum(when(col("action") == "LOGOUT", 1).otherwise(0)).alias("logouts")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_id",
            "event_count",
            "first_event",
            "last_event",
            "logins",
            "logouts",
            expr("(unix_timestamp(last_event) - unix_timestamp(first_event))").alias("session_duration_seconds"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_failed_logins(df):
    """
    Real-time View: auth_realtime_failed_logins
    Track failed login attempts for security monitoring
    """
    result = df \
        .filter((col("action") == "LOGIN") & (col("success") == False)) \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            "user_id"
        ) \
        .agg(
            count("*").alias("failed_attempt_count"),
            spark_min("timestamp").alias("first_failed_attempt"),
            spark_max("timestamp").alias("last_failed_attempt")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_id",
            "failed_attempt_count",
            "first_failed_attempt",
            "last_failed_attempt",
            current_timestamp().alias("processed_at")
        )
    
    return result


def write_stream(df, view_name, job_name):
    """Write streaming DataFrame to output sink"""
    output_path = get_output_path(view_name)
    checkpoint_path = get_checkpoint_path(job_name, view_name)
    
    query = df.writeStream \
        .format(speed_config["output"]["format"]) \
        .outputMode(speed_config["output"]["output_mode"]) \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime=speed_config["output"]["trigger_interval"]) \
        .start()
    
    return query


def main():
    """Main execution function"""
    print("Starting Auth Stream Processing Job...")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read Kafka stream
    auth_stream = read_kafka_stream(spark, speed_config["kafka"]["topics"]["auth"])
    
    # Compute real-time views
    active_users_1min = compute_active_users_1min(auth_stream)
    active_users_5min = compute_active_users_5min(auth_stream)
    login_rate = compute_login_rate(auth_stream)
    session_activity = compute_session_activity(auth_stream)
    failed_logins = compute_failed_logins(auth_stream)
    
    # Write streams
    queries = [
        write_stream(active_users_1min, "auth_realtime_active_users_1min", "auth_stream"),
        write_stream(active_users_5min, "auth_realtime_active_users_5min", "auth_stream"),
        write_stream(login_rate, "auth_realtime_login_rate", "auth_stream"),
        write_stream(session_activity, "auth_realtime_session_activity", "auth_stream"),
        write_stream(failed_logins, "auth_realtime_failed_logins", "auth_stream")
    ]
    
    print(f"Started {len(queries)} streaming queries for Auth events")
    print("Real-time views being generated:")
    for view in speed_config["realtime_views"]["auth"]:
        print(f"  - {view}")
    
    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
