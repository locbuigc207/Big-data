"""
Course Stream Processing Job
Real-time processing of course interaction events from Kafka

Real-time Views Generated:
1. course_realtime_active_courses_1min - Active course interactions in 1-minute windows
2. course_realtime_enrollment_rate - Real-time enrollment tracking
3. course_realtime_material_access_5min - Material access patterns
4. course_realtime_download_activity - Download activity monitoring
5. course_realtime_engagement_metrics - Overall course engagement metrics
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


# Define schema for course events
course_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_category", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("course_id", StringType(), True),
    StructField("material_id", StringType(), True),
    StructField("resource_id", StringType(), True)
])


def create_spark_session():
    """Create and configure Spark session for streaming"""
    builder = SparkSession.builder \
        .appName("SpeedLayer_CourseStreaming") \
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
        .select(from_json(col("json_value"), course_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return parsed_df


def compute_active_courses_1min(df):
    """
    Real-time View: course_realtime_active_courses_1min
    Track active course interactions in 1-minute windows
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            countDistinct("course_id").alias("active_courses"),
            countDistinct("user_id").alias("active_students"),
            count("*").alias("total_interactions"),
            spark_sum(when(col("action") == "ENROLL_COURSE", 1).otherwise(0)).alias("enrollments"),
            spark_sum(when(col("action") == "ACCESS_MATERIAL", 1).otherwise(0)).alias("material_accesses"),
            spark_sum(when(col("action") == "DOWNLOAD_RESOURCE", 1).otherwise(0)).alias("downloads")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "active_courses",
            "active_students",
            "total_interactions",
            "enrollments",
            "material_accesses",
            "downloads",
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_enrollment_rate(df):
    """
    Real-time View: course_realtime_enrollment_rate
    Track course enrollment rate per course
    """
    enrollments = df.filter(col("action") == "ENROLL_COURSE")
    
    result = enrollments \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "course_id"
        ) \
        .agg(
            count("*").alias("enrollment_count"),
            countDistinct("user_id").alias("unique_students")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "course_id",
            "enrollment_count",
            "unique_students",
            expr("ROUND(enrollment_count / 5.0, 2)").alias("enrollments_per_minute"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_material_access_5min(df):
    """
    Real-time View: course_realtime_material_access_5min
    Track material access patterns in 5-minute windows
    """
    material_events = df.filter(col("action") == "ACCESS_MATERIAL")
    
    result = material_events \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "course_id",
            "material_id"
        ) \
        .agg(
            countDistinct("user_id").alias("unique_students"),
            count("*").alias("access_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "course_id",
            "material_id",
            "unique_students",
            "access_count",
            expr("ROUND(access_count / 5.0, 2)").alias("accesses_per_minute"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_download_activity(df):
    """
    Real-time View: course_realtime_download_activity
    Monitor download activity in real-time
    """
    downloads = df.filter(col("action") == "DOWNLOAD_RESOURCE")
    
    result = downloads \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "course_id",
            "resource_id"
        ) \
        .agg(
            countDistinct("user_id").alias("unique_downloaders"),
            count("*").alias("download_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "course_id",
            "resource_id",
            "unique_downloaders",
            "download_count",
            expr("ROUND(download_count / 5.0, 2)").alias("downloads_per_minute"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_engagement_metrics(df):
    """
    Real-time View: course_realtime_engagement_metrics
    Calculate overall engagement metrics per course
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            "course_id"
        ) \
        .agg(
            countDistinct("user_id").alias("active_students"),
            count("*").alias("total_interactions"),
            countDistinct("action").alias("distinct_actions"),
            spark_sum(when(col("action") == "ENROLL_COURSE", 1).otherwise(0)).alias("enrollments"),
            spark_sum(when(col("action") == "ACCESS_MATERIAL", 1).otherwise(0)).alias("material_accesses"),
            spark_sum(when(col("action") == "DOWNLOAD_RESOURCE", 1).otherwise(0)).alias("downloads"),
            countDistinct(when(col("action") == "ACCESS_MATERIAL", col("material_id"))).alias("unique_materials_accessed"),
            countDistinct(when(col("action") == "DOWNLOAD_RESOURCE", col("resource_id"))).alias("unique_resources_downloaded")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "course_id",
            "active_students",
            "total_interactions",
            "distinct_actions",
            "enrollments",
            "material_accesses",
            "downloads",
            "unique_materials_accessed",
            "unique_resources_downloaded",
            expr("ROUND(total_interactions / NULLIF(active_students, 0), 2)").alias("avg_interactions_per_student"),
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
    print("Starting Course Stream Processing Job...")
    
    spark = create_spark_session()
    course_stream = read_kafka_stream(spark, speed_config["kafka"]["topics"]["course"])
    
    # Compute real-time views
    active_courses_1min = compute_active_courses_1min(course_stream)
    enrollment_rate = compute_enrollment_rate(course_stream)
    material_access_5min = compute_material_access_5min(course_stream)
    download_activity = compute_download_activity(course_stream)
    engagement_metrics = compute_engagement_metrics(course_stream)
    
    # Write streams
    queries = [
        write_stream(active_courses_1min, "course_realtime_active_courses_1min", "course_stream"),
        write_stream(enrollment_rate, "course_realtime_enrollment_rate", "course_stream"),
        write_stream(material_access_5min, "course_realtime_material_access_5min", "course_stream"),
        write_stream(download_activity, "course_realtime_download_activity", "course_stream"),
        write_stream(engagement_metrics, "course_realtime_engagement_metrics", "course_stream")
    ]
    
    print(f"Started {len(queries)} streaming queries for Course events")
    print("Real-time views being generated:")
    for view in speed_config["realtime_views"]["course"]:
        print(f"  - {view}")
    
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
