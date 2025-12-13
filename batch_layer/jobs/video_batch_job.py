"""
Video Batch Job - Video Engagement Analytics
Precomputes video watching behavior batch views from raw events in MinIO
Implements cumulative watch time tracking philosophy
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, date_format, 
    sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, expr
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import sys

def create_spark_session():
    """Initialize Spark Session with MinIO configuration"""
    return SparkSession.builder \
        .appName("Video_Batch_Job") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def get_video_schema():
    """Define schema for VIDEO events"""
    return StructType([
        StructField("event_category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("video_id", StringType(), False),
        StructField("course_id", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("watch_duration_seconds", IntegerType(), True),
        StructField("session_id", StringType(), True)
    ])

def compute_total_video_watch_time(df):
    """
    Compute total cumulative watch time per student per video
    This aggregates all watch sessions for each video
    """
    video_watched = df.filter(col("event_type") == "VIDEO_WATCHED")
    
    return video_watched.groupBy("user_id", "video_id", "course_id") \
        .agg(
            spark_sum("watch_duration_seconds").alias("total_watch_seconds"),
            count("session_id").alias("total_watch_sessions"),
            avg("watch_duration_seconds").alias("avg_watch_duration_seconds"),
            spark_max("watch_duration_seconds").alias("longest_session_seconds"),
            spark_min("watch_duration_seconds").alias("shortest_session_seconds")
        ) \
        .withColumn("total_watch_minutes", col("total_watch_seconds") / 60) \
        .withColumn("total_watch_hours", col("total_watch_seconds") / 3600) \
        .orderBy(col("total_watch_seconds").desc())

def compute_student_video_engagement(df):
    """
    Compute overall video engagement metrics per student per course
    """
    video_watched = df.filter(col("event_type") == "VIDEO_WATCHED")
    
    return video_watched.groupBy("user_id", "course_id") \
        .agg(
            countDistinct("video_id").alias("unique_videos_watched"),
            spark_sum("watch_duration_seconds").alias("total_watch_seconds"),
            count("video_id").alias("total_video_events"),
            avg("watch_duration_seconds").alias("avg_watch_duration_seconds")
        ) \
        .withColumn("total_watch_hours", col("total_watch_seconds") / 3600) \
        .orderBy(col("total_watch_seconds").desc())

def compute_video_popularity(df):
    """
    Compute video popularity metrics - which videos are most watched
    """
    video_watched = df.filter(col("event_type") == "VIDEO_WATCHED")
    
    return video_watched.groupBy("video_id", "course_id") \
        .agg(
            countDistinct("user_id").alias("unique_viewers"),
            spark_sum("watch_duration_seconds").alias("total_views_seconds"),
            count("user_id").alias("total_view_events"),
            avg("watch_duration_seconds").alias("avg_view_duration_seconds")
        ) \
        .withColumn("total_views_hours", col("total_views_seconds") / 3600) \
        .orderBy(col("unique_viewers").desc())

def compute_daily_video_engagement(df):
    """
    Compute daily video watching patterns
    """
    video_watched = df.filter(col("event_type") == "VIDEO_WATCHED")
    
    return video_watched.withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "course_id") \
        .agg(
            countDistinct("user_id").alias("active_video_watchers"),
            countDistinct("video_id").alias("unique_videos_watched"),
            spark_sum("watch_duration_seconds").alias("total_watch_seconds"),
            count("user_id").alias("total_watch_events")
        ) \
        .withColumn("total_watch_hours", col("total_watch_seconds") / 3600) \
        .orderBy("date")

def compute_course_video_completion_rate(df):
    """
    Compute video completion metrics per course
    Note: This assumes video metadata exists with video_duration_seconds
    For now, we compute engagement indicators without completion percentage
    """
    video_watched = df.filter(col("event_type") == "VIDEO_WATCHED")
    
    # Per course, count how many students watched each video
    course_metrics = video_watched.groupBy("course_id") \
        .agg(
            countDistinct("user_id").alias("total_students_watching"),
            countDistinct("video_id").alias("total_videos_in_course"),
            spark_sum("watch_duration_seconds").alias("total_course_watch_seconds")
        ) \
        .withColumn("total_course_watch_hours", col("total_course_watch_seconds") / 3600) \
        .orderBy("course_id")
    
    return course_metrics

def compute_student_course_video_summary(df):
    """
    Create a comprehensive summary: student x course x video metrics
    """
    video_watched = df.filter(col("event_type") == "VIDEO_WATCHED")
    
    # First, compute per-student per-course aggregates
    student_course = video_watched.groupBy("user_id", "course_id") \
        .agg(
            countDistinct("video_id").alias("videos_watched_count"),
            spark_sum("watch_duration_seconds").alias("total_watch_seconds"),
            count("session_id").alias("total_sessions")
        ) \
        .withColumn("total_watch_hours", col("total_watch_seconds") / 3600)
    
    # Get total videos per course (from all students' interactions)
    course_video_counts = video_watched.groupBy("course_id") \
        .agg(countDistinct("video_id").alias("total_videos_in_course"))
    
    # Join to compute engagement percentage
    return student_course.join(course_video_counts, "course_id", "left") \
        .withColumn(
            "video_coverage_percentage",
            (col("videos_watched_count") / col("total_videos_in_course") * 100).cast("decimal(5,2)")
        ) \
        .orderBy("user_id", "course_id")

def compute_video_drop_off_indicators(df):
    """
    Identify videos where students drop off quickly (short watch durations)
    This can indicate difficult or unengaging content
    """
    video_watched = df.filter(col("event_type") == "VIDEO_WATCHED")
    
    # Videos with average watch duration less than 60 seconds might indicate drop-off
    return video_watched.groupBy("video_id", "course_id") \
        .agg(
            count("user_id").alias("total_views"),
            avg("watch_duration_seconds").alias("avg_watch_duration"),
            spark_min("watch_duration_seconds").alias("min_watch_duration"),
            countDistinct("user_id").alias("unique_viewers")
        ) \
        .withColumn(
            "potential_drop_off",
            when(col("avg_watch_duration") < 60, "High Risk")
            .when(col("avg_watch_duration") < 180, "Medium Risk")
            .otherwise("Good Engagement")
        ) \
        .orderBy("avg_watch_duration")

def main(input_path, output_path):
    """Main batch job execution"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[VIDEO BATCH] Reading video events from: {input_path}")
    
    # Read raw video events from MinIO
    df_raw = spark.read.parquet(f"{input_path}/topic=video_topic")
    
    # Parse timestamp
    df = df_raw.withColumn("timestamp_parsed", col("timestamp").cast(TimestampType()))
    
    print(f"[VIDEO BATCH] Total video events: {df.count()}")
    
    # Compute batch views
    print("[VIDEO BATCH] Computing total video watch time per student per video...")
    total_watch_time = compute_total_video_watch_time(df)
    total_watch_time.write.mode("overwrite").parquet(f"{output_path}/video_total_watch_time")
    
    print("[VIDEO BATCH] Computing student video engagement per course...")
    student_engagement = compute_student_video_engagement(df)
    student_engagement.write.mode("overwrite").parquet(f"{output_path}/video_student_engagement")
    
    print("[VIDEO BATCH] Computing video popularity metrics...")
    video_popularity = compute_video_popularity(df)
    video_popularity.write.mode("overwrite").parquet(f"{output_path}/video_popularity")
    
    print("[VIDEO BATCH] Computing daily video engagement...")
    daily_engagement = compute_daily_video_engagement(df)
    daily_engagement.write.mode("overwrite").parquet(f"{output_path}/video_daily_engagement")
    
    print("[VIDEO BATCH] Computing course video metrics...")
    course_metrics = compute_course_video_completion_rate(df)
    course_metrics.write.mode("overwrite").parquet(f"{output_path}/video_course_metrics")
    
    print("[VIDEO BATCH] Computing student-course-video summary...")
    student_course_summary = compute_student_course_video_summary(df)
    student_course_summary.write.mode("overwrite").parquet(f"{output_path}/video_student_course_summary")
    
    print("[VIDEO BATCH] Computing video drop-off indicators...")
    drop_off = compute_video_drop_off_indicators(df)
    drop_off.write.mode("overwrite").parquet(f"{output_path}/video_drop_off_indicators")
    
    print("[VIDEO BATCH] Video batch job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: video_batch_job.py <input_path> <output_path>")
        print("Example: video_batch_job.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
