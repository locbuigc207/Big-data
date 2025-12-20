"""
Student-Course Analytics Batch Job
Computes enrollment mappings and performance metrics for course-student drill-down
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg, 
    max as spark_max, min as spark_min, from_json
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import sys
from spark_config import create_spark_session, read_topic_data

def get_course_schema():
    """Schema for COURSE events"""
    return StructType([
        StructField("event_category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("role", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("course_id", StringType(), True),
        StructField("material_id", StringType(), True),
        StructField("resource_type", StringType(), True)
    ])

def get_assessment_schema():
    """Schema for ASSESSMENT events"""
    return StructType([
        StructField("event_category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("role", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("assignment_id", StringType(), True),
        StructField("quiz_id", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("correct_answers", IntegerType(), True),
        StructField("total_questions", IntegerType(), True)
    ])

def get_video_schema():
    """Schema for VIDEO events"""
    return StructType([
        StructField("event_category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("role", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("video_id", StringType(), True),
        StructField("course_id", StringType(), True),
        StructField("watch_duration_seconds", IntegerType(), True),
        StructField("session_id", StringType(), True)
    ])

def compute_course_overview(df_course, df_video, df_assessment):
    """Overall course statistics"""
    # Enrollments per course
    enrollments = df_course.filter(col("event_type") == "COURSE_ENROLLED") \
        .groupBy("course_id") \
        .agg(countDistinct("user_id").alias("total_enrolled_students"))
    
    # Video watches per course
    video_stats = df_video.groupBy("course_id") \
        .agg(count("*").alias("total_videos_watched"))
    
    # Materials downloaded
    materials = df_course.filter(col("event_type") == "DOWNLOAD_MATERIAL") \
        .groupBy("course_id") \
        .agg(count("*").alias("total_materials_downloaded"))
    
    # Combine all stats
    overview = enrollments \
        .join(video_stats, "course_id", "left") \
        .join(materials, "course_id", "left") \
        .na.fill(0)
    
    return overview

def compute_student_overview(df_course, df_video, df_assessment, df_auth):
    """Overall student statistics"""
    # Courses per student
    courses = df_course.filter(col("event_type") == "COURSE_ENROLLED") \
        .groupBy("user_id") \
        .agg(countDistinct("course_id").alias("total_courses_enrolled"))
    
    # Videos watched
    videos = df_video.groupBy("user_id") \
        .agg(count("*").alias("total_videos_watched"))
    
    # Assignments submitted
    assignments = df_assessment.filter(col("event_type") == "SUBMIT_ASSIGNMENT") \
        .groupBy("user_id") \
        .agg(
            count("*").alias("total_assignments_submitted"),
            avg("score").alias("avg_assignment_score")
        )
    
    # Login hours (estimate from session count)
    logins = df_auth.filter(col("event_type") == "LOGIN") \
        .groupBy("user_id") \
        .agg(count("*").alias("total_logins"))
    
    # Combine
    overview = courses \
        .join(videos, "user_id", "left") \
        .join(assignments, "user_id", "left") \
        .join(logins, "user_id", "left") \
        .na.fill(0) \
        .withColumnRenamed("user_id", "student_id")
    
    return overview

def compute_student_course_enrollment(df_course, df_video, df_assessment):
    """Student-course mapping with performance metrics"""
    # Base enrollment
    enrollments = df_course.filter(col("event_type") == "COURSE_ENROLLED") \
        .select("user_id", "course_id", col("timestamp_parsed").alias("enrollment_date"))
    
    # Videos watched per student-course
    video_counts = df_video.groupBy("user_id", "course_id") \
        .agg(count("*").alias("total_videos_watched"))
    
    # Assignments submitted per student-course
    # Note: We need to infer course from assignment (not in event), so skip for now
    # In production, you'd join with an assignment-course mapping table
    
    # Combine
    student_courses = enrollments \
        .join(video_counts, ["user_id", "course_id"], "left") \
        .na.fill(0) \
        .withColumnRenamed("user_id", "student_id")
    
    return student_courses

def compute_student_course_detailed(df_course, df_video, df_assessment):
    """Detailed per-student-per-course metrics"""
    # Videos
    video_stats = df_video.groupBy("user_id", "course_id") \
        .agg(
            count("*").alias("videos_watched"),
            spark_sum("watch_duration_seconds").alias("watch_time_seconds")
        )
    
    # Materials downloaded
    material_stats = df_course.filter(col("event_type") == "DOWNLOAD_MATERIAL") \
        .groupBy("user_id", "course_id") \
        .agg(count("*").alias("materials_downloaded"))
    
    # Last activity
    last_activity = df_course.groupBy("user_id", "course_id") \
        .agg(spark_max("timestamp_parsed").alias("last_activity_date"))
    
    # Combine
    detailed = video_stats \
        .join(material_stats, ["user_id", "course_id"], "left") \
        .join(last_activity, ["user_id", "course_id"], "left") \
        .na.fill(0, ["materials_downloaded"]) \
        .withColumn("watch_time_minutes", col("watch_time_seconds") / 60) \
        .drop("watch_time_seconds") \
        .withColumnRenamed("user_id", "student_id")
    
    return detailed

def main(input_path, output_path):
    """Main batch job execution"""
    spark = create_spark_session("Student_Course_Analytics_Batch_Job")
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[STUDENT-COURSE BATCH] Reading events from: {input_path}")
    
    # Read and parse COURSE events
    df_course_raw = read_topic_data(spark, input_path, "course_topic")
    course_schema = get_course_schema()
    df_course = df_course_raw \
        .withColumn("data", from_json(col("value").cast("string"), course_schema)) \
        .select("data.*") \
        .withColumn("timestamp_parsed", col("timestamp").cast(TimestampType())) \
        .filter(col("role") == "student")  # Only student events
    
    # Read and parse VIDEO events
    df_video_raw = read_topic_data(spark, input_path, "video_topic")
    video_schema = get_video_schema()
    df_video = df_video_raw \
        .withColumn("data", from_json(col("value").cast("string"), video_schema)) \
        .select("data.*") \
        .withColumn("timestamp_parsed", col("timestamp").cast(TimestampType())) \
        .filter(col("role") == "student")
    
    # Read and parse ASSESSMENT events
    df_assessment_raw = read_topic_data(spark, input_path, "assessment_topic")
    assessment_schema = get_assessment_schema()
    df_assessment = df_assessment_raw \
        .withColumn("data", from_json(col("value").cast("string"), assessment_schema)) \
        .select("data.*") \
        .withColumn("timestamp_parsed", col("timestamp").cast(TimestampType())) \
        .filter(col("role") == "student")
    
    # Read AUTH for login stats
    from auth_batch_job import get_auth_schema
    df_auth_raw = read_topic_data(spark, input_path, "auth_topic")
    auth_schema = get_auth_schema()
    df_auth = df_auth_raw \
        .withColumn("data", from_json(col("value").cast("string"), auth_schema)) \
        .select("data.*") \
        .withColumn("timestamp_parsed", col("timestamp").cast(TimestampType())) \
        .filter(col("role") == "student")
    
    # Compute views
    print("[STUDENT-COURSE BATCH] Computing course overview...")
    course_overview = compute_course_overview(df_course, df_video, df_assessment)
    course_overview.write.mode("overwrite").parquet(f"{output_path}/course_overview")
    
    print("[STUDENT-COURSE BATCH] Computing student overview...")
    student_overview = compute_student_overview(df_course, df_video, df_assessment, df_auth)
    student_overview.write.mode("overwrite").parquet(f"{output_path}/student_overview")
    
    print("[STUDENT-COURSE BATCH] Computing student-course enrollment...")
    student_course_enrollment = compute_student_course_enrollment(df_course, df_video, df_assessment)
    student_course_enrollment.write.mode("overwrite").parquet(f"{output_path}/student_course_enrollment")
    
    print("[STUDENT-COURSE BATCH] Computing detailed student-course metrics...")
    student_course_detailed = compute_student_course_detailed(df_course, df_video, df_assessment)
    student_course_detailed.write.mode("overwrite").parquet(f"{output_path}/student_course_detailed")
    
    print("[STUDENT-COURSE BATCH] Batch job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: student_course_analytics_batch_job.py <input_path> <output_path>")
        print("Example: student_course_analytics_batch_job.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
