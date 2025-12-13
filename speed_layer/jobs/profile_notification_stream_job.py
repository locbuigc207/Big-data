"""
Profile and Notification Stream Processing Job
Real-time processing of profile and notification events from Kafka

Real-time Views Generated:
1. profile_realtime_updates_1min - Profile update activity in 1-minute windows
2. notification_realtime_sent_1min - Notifications sent per minute
3. notification_realtime_click_rate_5min - Notification click-through rate
4. notification_realtime_delivery_status - Notification delivery monitoring
5. notification_realtime_engagement_metrics - Overall notification engagement
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, countDistinct,
    sum as spark_sum, avg, min as spark_min, max as spark_max,
    current_timestamp, to_timestamp, expr, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import speed_config, get_kafka_connection_options, get_output_path, get_checkpoint_path, get_spark_config


# Define schema for profile events
profile_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_category", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("field_name", StringType(), True)
])


# Define schema for notification events
notification_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_category", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("notification_id", StringType(), True),
    StructField("notification_type", StringType(), True)
])


def create_spark_session():
    """Create and configure Spark session for streaming"""
    builder = SparkSession.builder \
        .appName("SpeedLayer_ProfileNotificationStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
    for key, value in get_spark_config().items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream_profile(spark):
    """Read profile events from Kafka"""
    kafka_options = get_kafka_connection_options(speed_config["kafka"]["topics"]["profile"])
    
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()
    
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), profile_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return parsed_df


def read_kafka_stream_notification(spark):
    """Read notification events from Kafka"""
    kafka_options = get_kafka_connection_options(speed_config["kafka"]["topics"]["notification"])
    
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()
    
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), notification_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return parsed_df


def compute_profile_updates_1min(df):
    """
    Real-time View: profile_realtime_updates_1min
    Track profile update activity in 1-minute windows
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            countDistinct("user_id").alias("users_updated_profile"),
            count("*").alias("total_updates"),
            spark_sum(when(col("action") == "UPDATE_PROFILE", 1).otherwise(0)).alias("general_updates"),
            spark_sum(when(col("action") == "CHANGE_AVATAR", 1).otherwise(0)).alias("avatar_changes"),
            countDistinct("field_name").alias("unique_fields_updated")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "users_updated_profile",
            "total_updates",
            "general_updates",
            "avatar_changes",
            "unique_fields_updated",
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_notifications_sent_1min(df):
    """
    Real-time View: notification_realtime_sent_1min
    Track notifications sent per minute
    """
    sent_notifications = df.filter(col("action") == "SEND_NOTIFICATION")
    
    result = sent_notifications \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            count("*").alias("total_sent"),
            countDistinct("user_id").alias("unique_recipients"),
            countDistinct("notification_type").alias("unique_types")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_sent",
            "unique_recipients",
            "unique_types",
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_click_rate_5min(df):
    """
    Real-time View: notification_realtime_click_rate_5min
    Calculate notification click-through rate in 5-minute windows
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "notification_type"
        ) \
        .agg(
            spark_sum(when(col("action") == "SEND_NOTIFICATION", 1).otherwise(0)).alias("sent"),
            spark_sum(when(col("action") == "CLICK_NOTIFICATION", 1).otherwise(0)).alias("clicked"),
            countDistinct("user_id").alias("unique_users")
        ) \
        .filter(col("sent") > 0) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "notification_type",
            "sent",
            "clicked",
            "unique_users",
            expr("ROUND((clicked / sent) * 100, 2)").alias("click_through_rate_pct"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_delivery_status(df):
    """
    Real-time View: notification_realtime_delivery_status
    Monitor notification delivery status
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "notification_id",
            "notification_type"
        ) \
        .agg(
            spark_sum(when(col("action") == "SEND_NOTIFICATION", 1).otherwise(0)).alias("sent_count"),
            spark_sum(when(col("action") == "VIEW_NOTIFICATION", 1).otherwise(0)).alias("viewed_count"),
            spark_sum(when(col("action") == "CLICK_NOTIFICATION", 1).otherwise(0)).alias("clicked_count"),
            spark_min(when(col("action") == "SEND_NOTIFICATION", col("timestamp"))).alias("sent_time"),
            spark_min(when(col("action") == "VIEW_NOTIFICATION", col("timestamp"))).alias("first_view_time"),
            spark_min(when(col("action") == "CLICK_NOTIFICATION", col("timestamp"))).alias("first_click_time"),
            countDistinct("user_id").alias("unique_users")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "notification_id",
            "notification_type",
            "sent_count",
            "viewed_count",
            "clicked_count",
            "unique_users",
            "sent_time",
            "first_view_time",
            "first_click_time",
            expr("unix_timestamp(first_view_time) - unix_timestamp(sent_time)").alias("time_to_view_seconds"),
            expr("unix_timestamp(first_click_time) - unix_timestamp(sent_time)").alias("time_to_click_seconds"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_engagement_metrics(df):
    """
    Real-time View: notification_realtime_engagement_metrics
    Calculate overall notification engagement metrics
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            "user_id"
        ) \
        .agg(
            count("*").alias("total_events"),
            spark_sum(when(col("action") == "SEND_NOTIFICATION", 1).otherwise(0)).alias("notifications_received"),
            spark_sum(when(col("action") == "VIEW_NOTIFICATION", 1).otherwise(0)).alias("notifications_viewed"),
            spark_sum(when(col("action") == "CLICK_NOTIFICATION", 1).otherwise(0)).alias("notifications_clicked"),
            countDistinct("notification_type").alias("unique_notification_types")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_id",
            "total_events",
            "notifications_received",
            "notifications_viewed",
            "notifications_clicked",
            "unique_notification_types",
            expr("ROUND((notifications_viewed / NULLIF(notifications_received, 0)) * 100, 2)").alias("view_rate_pct"),
            expr("ROUND((notifications_clicked / NULLIF(notifications_received, 0)) * 100, 2)").alias("click_rate_pct"),
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
    print("Starting Profile and Notification Stream Processing Job...")
    
    spark = create_spark_session()
    
    # Read both profile and notification streams
    profile_stream = read_kafka_stream_profile(spark)
    notification_stream = read_kafka_stream_notification(spark)
    
    # Compute profile views
    profile_updates_1min = compute_profile_updates_1min(profile_stream)
    
    # Compute notification views
    notifications_sent_1min = compute_notifications_sent_1min(notification_stream)
    click_rate_5min = compute_click_rate_5min(notification_stream)
    delivery_status = compute_delivery_status(notification_stream)
    engagement_metrics = compute_engagement_metrics(notification_stream)
    
    # Write streams
    queries = [
        write_stream(profile_updates_1min, "profile_realtime_updates_1min", "profile_notification_stream"),
        write_stream(notifications_sent_1min, "notification_realtime_sent_1min", "profile_notification_stream"),
        write_stream(click_rate_5min, "notification_realtime_click_rate_5min", "profile_notification_stream"),
        write_stream(delivery_status, "notification_realtime_delivery_status", "profile_notification_stream"),
        write_stream(engagement_metrics, "notification_realtime_engagement_metrics", "profile_notification_stream")
    ]
    
    print(f"Started {len(queries)} streaming queries for Profile and Notification events")
    print("Real-time views being generated:")
    for view in speed_config["realtime_views"]["profile_notification"]:
        print(f"  - {view}")
    
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
