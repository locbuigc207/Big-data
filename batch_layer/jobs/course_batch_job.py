"""
Course Batch Job - Course Interaction Analytics
Precomputes course-related batch views from raw events in MinIO
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
        .appName("Course_Batch_Job") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def compute_course_enrollment_stats(df):
    """Compute enrollment statistics per course"""
    enrollments = df.filter(col("event_type") == "ENROLL_COURSE")
    
    return enrollments.withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "course_id") \
        .agg(
            count("user_id").alias("new_enrollments"),
            countDistinct("user_id").alias("unique_new_students")
        ) \
        .orderBy("date", "course_id")

def compute_course_material_access(df):
    """Track course material access patterns"""
    material_access = df.filter(col("event_type") == "ACCESS_COURSE_MATERIAL")
    
    return material_access.groupBy("user_id", "course_id") \
        .agg(
            count("material_id").alias("total_material_accesses"),
            countDistinct("material_id").alias("unique_materials_accessed"),
            spark_min("timestamp_parsed").alias("first_access"),
            spark_max("timestamp_parsed").alias("last_access")
        ) \
        .orderBy("user_id", "course_id")

def compute_material_popularity(df):
    """Compute which course materials are most accessed"""
    material_access = df.filter(col("event_type") == "ACCESS_COURSE_MATERIAL")
    
    return material_access.groupBy("course_id", "material_id", "material_type") \
        .agg(
            countDistinct("user_id").alias("unique_accessors"),
            count("user_id").alias("total_accesses")
        ) \
        .orderBy(col("total_accesses").desc())

def compute_download_analytics(df):
    """Analyze download patterns"""
    downloads = df.filter(col("event_type") == "DOWNLOAD_RESOURCE")
    
    return downloads.groupBy("user_id", "course_id") \
        .agg(
            count("resource_id").alias("total_downloads"),
            countDistinct("resource_id").alias("unique_resources_downloaded")
        ) \
        .orderBy(col("total_downloads").desc())

def compute_resource_download_stats(df):
    """Compute download statistics per resource"""
    downloads = df.filter(col("event_type") == "DOWNLOAD_RESOURCE")
    
    return downloads.groupBy("course_id", "resource_id") \
        .agg(
            countDistinct("user_id").alias("unique_downloaders"),
            count("user_id").alias("total_download_count")
        ) \
        .orderBy(col("total_download_count").desc())

def compute_course_activity_summary(df):
    """Comprehensive course activity summary per student"""
    # Enrollments
    enrollments = df.filter(col("event_type") == "ENROLL_COURSE") \
        .groupBy("user_id") \
        .agg(countDistinct("course_id").alias("courses_enrolled"))
    
    # Material access
    materials = df.filter(col("event_type") == "ACCESS_COURSE_MATERIAL") \
        .groupBy("user_id") \
        .agg(
            count("material_id").alias("total_material_accesses"),
            countDistinct("course_id").alias("courses_with_material_access")
        )
    
    # Downloads
    downloads = df.filter(col("event_type") == "DOWNLOAD_RESOURCE") \
        .groupBy("user_id") \
        .agg(count("resource_id").alias("total_downloads"))
    
    # Combine all metrics
    return enrollments \
        .join(materials, "user_id", "outer") \
        .join(downloads, "user_id", "outer") \
        .na.fill(0) \
        .orderBy("user_id")

def compute_daily_course_engagement(df):
    """Track daily engagement across all courses"""
    return df.withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "course_id", "event_type") \
        .agg(
            countDistinct("user_id").alias("unique_users"),
            count("user_id").alias("event_count")
        ) \
        .orderBy("date", "course_id", "event_type")

def compute_course_overall_metrics(df):
    """Overall metrics per course (enrollments, material access, downloads)"""
    course_metrics = df.groupBy("course_id") \
        .agg(
            count(when(col("event_type") == "ENROLL_COURSE", 1)).alias("total_enrollments"),
            count(when(col("event_type") == "ACCESS_COURSE_MATERIAL", 1)).alias("total_material_accesses"),
            count(when(col("event_type") == "DOWNLOAD_RESOURCE", 1)).alias("total_downloads"),
            countDistinct("user_id").alias("unique_students_interacting")
        ) \
        .orderBy(col("total_enrollments").desc())
    
    return course_metrics

def main(input_path, output_path):
    """Main batch job execution"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[COURSE BATCH] Reading course events from: {input_path}")
    
    # Read raw course events from MinIO
    df_raw = spark.read.parquet(f"{input_path}/topic=course_topic")
    
    # Parse timestamp
    df = df_raw.withColumn("timestamp_parsed", col("timestamp").cast(TimestampType()))
    
    print(f"[COURSE BATCH] Total course events: {df.count()}")
    
    # Compute batch views
    print("[COURSE BATCH] Computing enrollment statistics...")
    enrollment_stats = compute_course_enrollment_stats(df)
    enrollment_stats.write.mode("overwrite").parquet(f"{output_path}/course_enrollment_stats")
    
    print("[COURSE BATCH] Computing material access patterns...")
    material_access = compute_course_material_access(df)
    material_access.write.mode("overwrite").parquet(f"{output_path}/course_material_access")
    
    print("[COURSE BATCH] Computing material popularity...")
    material_popularity = compute_material_popularity(df)
    material_popularity.write.mode("overwrite").parquet(f"{output_path}/course_material_popularity")
    
    print("[COURSE BATCH] Computing download analytics...")
    download_analytics = compute_download_analytics(df)
    download_analytics.write.mode("overwrite").parquet(f"{output_path}/course_download_analytics")
    
    print("[COURSE BATCH] Computing resource download stats...")
    resource_stats = compute_resource_download_stats(df)
    resource_stats.write.mode("overwrite").parquet(f"{output_path}/course_resource_download_stats")
    
    print("[COURSE BATCH] Computing activity summary per student...")
    activity_summary = compute_course_activity_summary(df)
    activity_summary.write.mode("overwrite").parquet(f"{output_path}/course_activity_summary")
    
    print("[COURSE BATCH] Computing daily course engagement...")
    daily_engagement = compute_daily_course_engagement(df)
    daily_engagement.write.mode("overwrite").parquet(f"{output_path}/course_daily_engagement")
    
    print("[COURSE BATCH] Computing overall course metrics...")
    overall_metrics = compute_course_overall_metrics(df)
    overall_metrics.write.mode("overwrite").parquet(f"{output_path}/course_overall_metrics")
    
    print("[COURSE BATCH] Course batch job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: course_batch_job.py <input_path> <output_path>")
        print("Example: course_batch_job.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
