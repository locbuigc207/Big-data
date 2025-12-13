"""
Video Stream Processing Job
Real-time processing of video interaction events from Kafka

Real-time Views Generated:
1. video_realtime_viewers_1min - Active video viewers in 1-minute windows
2. video_realtime_watch_time_5min - Watch time aggregated in 5-minute windows
3. video_realtime_engagement_rate - Video engagement metrics
4. video_realtime_popular_videos - Most viewed videos in real-time
5. video_realtime_concurrent_viewers - Concurrent viewer counts per video
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, countDistinct,
    sum as spark_sum, avg, min as spark_min, max as spark_max,
    current_timestamp, to_timestamp, expr, when, desc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType
)
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import speed_config, get_kafka_connection_options, get_output_path, get_checkpoint_path, get_spark_config


# Define schema for video events
video_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_category", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("course_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("watch_duration_seconds", IntegerType(), True)
])


def create_spark_session():
    """Create and configure Spark session for streaming"""
    builder = SparkSession.builder \
        .appName("SpeedLayer_VideoStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    
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
    
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), video_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return parsed_df


def compute_viewers_1min(df):
    """
    Real-time View: video_realtime_viewers_1min
    Count unique video viewers in 1-minute windows
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            countDistinct("user_id").alias("unique_viewers"),
            countDistinct("video_id").alias("videos_watched"),
            count("*").alias("total_events"),
            spark_sum(when(col("action") == "START_VIDEO", 1).otherwise(0)).alias("video_starts"),
            spark_sum(when(col("action") == "PAUSE_VIDEO", 1).otherwise(0)).alias("video_pauses"),
            spark_sum(when(col("action") == "COMPLETE_VIDEO", 1).otherwise(0)).alias("video_completions"),
            spark_sum("watch_duration_seconds").alias("total_watch_seconds")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "unique_viewers",
            "videos_watched",
            "total_events",
            "video_starts",
            "video_pauses",
            "video_completions",
            "total_watch_seconds",
            expr("ROUND(total_watch_seconds / 60.0, 2)").alias("total_watch_minutes"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_watch_time_5min(df):
    """
    Real-time View: video_realtime_watch_time_5min
    Aggregate watch time per video in 5-minute windows
    """
    result = df \
        .filter(col("watch_duration_seconds").isNotNull()) \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "video_id",
            "course_id"
        ) \
        .agg(
            countDistinct("user_id").alias("unique_viewers"),
            spark_sum("watch_duration_seconds").alias("total_watch_seconds"),
            avg("watch_duration_seconds").alias("avg_watch_seconds"),
            spark_max("watch_duration_seconds").alias("max_watch_seconds"),
            count("*").alias("view_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "video_id",
            "course_id",
            "unique_viewers",
            "total_watch_seconds",
            expr("ROUND(total_watch_seconds / 60.0, 2)").alias("total_watch_minutes"),
            expr("ROUND(avg_watch_seconds, 2)").alias("avg_watch_seconds"),
            "max_watch_seconds",
            "view_count",
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_engagement_rate(df):
    """
    Real-time View: video_realtime_engagement_rate
    Calculate video engagement rate (completions vs starts)
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "video_id"
        ) \
        .agg(
            spark_sum(when(col("action") == "START_VIDEO", 1).otherwise(0)).alias("starts"),
            spark_sum(when(col("action") == "PAUSE_VIDEO", 1).otherwise(0)).alias("pauses"),
            spark_sum(when(col("action") == "COMPLETE_VIDEO", 1).otherwise(0)).alias("completions"),
            countDistinct("user_id").alias("unique_users"),
            spark_sum("watch_duration_seconds").alias("total_watch_seconds")
        ) \
        .filter(col("starts") > 0) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "video_id",
            "starts",
            "pauses",
            "completions",
            "unique_users",
            "total_watch_seconds",
            expr("ROUND((completions / starts) * 100, 2)").alias("completion_rate_pct"),
            expr("ROUND((pauses / starts) * 100, 2)").alias("pause_rate_pct"),
            expr("ROUND(total_watch_seconds / starts, 2)").alias("avg_watch_per_start"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_popular_videos(df):
    """
    Real-time View: video_realtime_popular_videos
    Identify most popular videos based on viewer count
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "video_id",
            "course_id"
        ) \
        .agg(
            countDistinct("user_id").alias("unique_viewers"),
            count("*").alias("total_interactions"),
            spark_sum("watch_duration_seconds").alias("total_watch_seconds"),
            spark_sum(when(col("action") == "START_VIDEO", 1).otherwise(0)).alias("starts"),
            spark_sum(when(col("action") == "COMPLETE_VIDEO", 1).otherwise(0)).alias("completions")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "video_id",
            "course_id",
            "unique_viewers",
            "total_interactions",
            "total_watch_seconds",
            "starts",
            "completions",
            expr("ROUND((completions / NULLIF(starts, 0)) * 100, 2)").alias("completion_rate_pct"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_concurrent_viewers(df):
    """
    Real-time View: video_realtime_concurrent_viewers
    Track concurrent viewers using sliding windows
    """
    # Use sliding window to capture overlapping sessions
    result = df \
        .filter(col("action").isin(["START_VIDEO", "WATCH_VIDEO"])) \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            "video_id"
        ) \
        .agg(
            countDistinct("user_id").alias("concurrent_viewers"),
            count("*").alias("event_count"),
            spark_sum("watch_duration_seconds").alias("total_watch_seconds")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "video_id",
            "concurrent_viewers",
            "event_count",
            "total_watch_seconds",
            expr("ROUND(total_watch_seconds / 60.0, 2)").alias("total_watch_minutes"),
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
    print("Starting Video Stream Processing Job...")
    
    spark = create_spark_session()
    video_stream = read_kafka_stream(spark, speed_config["kafka"]["topics"]["video"])
    
    # Compute real-time views
    viewers_1min = compute_viewers_1min(video_stream)
    watch_time_5min = compute_watch_time_5min(video_stream)
    engagement_rate = compute_engagement_rate(video_stream)
    popular_videos = compute_popular_videos(video_stream)
    concurrent_viewers = compute_concurrent_viewers(video_stream)
    
    # Write streams
    queries = [
        write_stream(viewers_1min, "video_realtime_viewers_1min", "video_stream"),
        write_stream(watch_time_5min, "video_realtime_watch_time_5min", "video_stream"),
        write_stream(engagement_rate, "video_realtime_engagement_rate", "video_stream"),
        write_stream(popular_videos, "video_realtime_popular_videos", "video_stream"),
        write_stream(concurrent_viewers, "video_realtime_concurrent_viewers", "video_stream")
    ]
    
    print(f"Started {len(queries)} streaming queries for Video events")
    print("Real-time views being generated:")
    for view in speed_config["realtime_views"]["video"]:
        print(f"  - {view}")
    
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
