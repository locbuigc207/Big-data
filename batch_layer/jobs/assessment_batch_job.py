"""
Assessment Batch Job - Assessment Analytics
Precomputes assignment and quiz-related batch views from raw events in MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, date_format, 
    min as spark_min, max as spark_max, avg, sum as spark_sum,
    unix_timestamp, when, expr, datediff, lit
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
import sys

def create_spark_session():
    """Initialize Spark Session with MinIO configuration"""
    return SparkSession.builder \
        .appName("Assessment_Batch_Job") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def get_assessment_schema():
    """Define schema for ASSESSMENT events"""
    return StructType([
        StructField("event_category", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("role", StringType(), True),
        StructField("assignment_id", StringType(), True),
        StructField("quiz_id", StringType(), True),
        StructField("course_id", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("assignment_type", StringType(), True),
        StructField("file_url", StringType(), True),
        StructField("correct_answers", IntegerType(), True),
        StructField("score", IntegerType(), True),
        StructField("feedback", StringType(), True),
        StructField("student_id", StringType(), True)
    ])

def compute_student_submission_stats(df):
    """Compute submission statistics per student"""
    submissions = df.filter(col("event_type") == "SUBMIT_ASSIGNMENT")
    
    return submissions.groupBy("user_id", "course_id") \
        .agg(
            countDistinct("assignment_id").alias("total_assignments_submitted"),
            count("assignment_id").alias("total_submissions"),  # Including resubmissions
            spark_min("timestamp_parsed").alias("first_submission"),
            spark_max("timestamp_parsed").alias("last_submission")
        ) \
        .orderBy(col("total_assignments_submitted").desc())

def compute_assignment_engagement_timeline(df):
    """Track how students engage with assignments from view to submission"""
    views = df.filter(col("event_type") == "VIEW_ASSIGNMENT_FIRST_TIME") \
        .select(
            col("user_id"),
            col("assignment_id"),
            col("course_id"),
            col("timestamp_parsed").alias("view_time")
        )
    
    submissions = df.filter(col("event_type") == "SUBMIT_ASSIGNMENT") \
        .select(
            col("user_id"),
            col("assignment_id"),
            col("timestamp_parsed").alias("submit_time")
        )
    
    # Join to calculate time from view to submission
    timeline = views.join(submissions, ["user_id", "assignment_id"], "inner") \
        .withColumn(
            "hours_to_submit",
            (unix_timestamp("submit_time") - unix_timestamp("view_time")) / 3600
        )
    
    # Aggregate per student
    return timeline.groupBy("user_id", "course_id") \
        .agg(
            count("assignment_id").alias("assignments_completed"),
            avg("hours_to_submit").alias("avg_hours_to_submit"),
            spark_min("hours_to_submit").alias("min_hours_to_submit"),
            spark_max("hours_to_submit").alias("max_hours_to_submit")
        ) \
        .orderBy("user_id")

def compute_quiz_performance(df):
    """Compute quiz performance metrics per student"""
    quizzes = df.filter(col("event_type") == "QUIZ_COMPLETED")
    
    # Note: correct_answers is raw count. Need quiz metadata for total questions and score calculation
    # For now, we'll work with raw correct_answers
    return quizzes.groupBy("user_id", "course_id") \
        .agg(
            count("quiz_id").alias("total_quizzes_completed"),
            avg("correct_answers").alias("avg_correct_answers"),
            spark_sum("correct_answers").alias("total_correct_answers"),
            spark_min("correct_answers").alias("min_correct_answers"),
            spark_max("correct_answers").alias("max_correct_answers")
        ) \
        .orderBy(col("avg_correct_answers").desc())

def compute_grading_stats(df):
    """Compute grading statistics (teacher perspective)"""
    grading = df.filter(col("event_type") == "GRADE_ASSIGNMENT")
    
    return grading.groupBy("student_id", "course_id") \
        .agg(
            count("assignment_id").alias("total_assignments_graded"),
            avg("score").alias("avg_assignment_score"),
            spark_min("score").alias("min_assignment_score"),
            spark_max("score").alias("max_assignment_score"),
            spark_sum("score").alias("total_assignment_score")
        ) \
        .orderBy(col("avg_assignment_score").desc())

def compute_teacher_grading_workload(df):
    """Compute teacher grading workload"""
    teacher_views = df.filter(col("event_type") == "VIEW_STUDENT_WORK")
    teacher_grades = df.filter(col("event_type") == "GRADE_ASSIGNMENT")
    
    # Views per teacher
    views_agg = teacher_views.groupBy("user_id", "course_id") \
        .agg(count("student_id").alias("total_works_viewed"))
    
    # Grades given per teacher
    grades_agg = teacher_grades.groupBy("user_id", "course_id") \
        .agg(
            count("student_id").alias("total_assignments_graded"),
            avg("score").alias("avg_score_given")
        )
    
    # Combine
    return views_agg.join(grades_agg, ["user_id", "course_id"], "outer") \
        .orderBy("user_id", "course_id")

def compute_assignment_submission_distribution(df):
    """Compute daily assignment submission distribution"""
    submissions = df.filter(col("event_type") == "SUBMIT_ASSIGNMENT")
    
    return submissions.withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "course_id", "assignment_type") \
        .agg(
            count("user_id").alias("submissions_count"),
            countDistinct("user_id").alias("unique_students")
        ) \
        .orderBy("date", "course_id")

def compute_student_overall_performance(df):
    """Comprehensive student performance view combining quizzes and assignments"""
    # Quiz performance
    quiz_perf = df.filter(col("event_type") == "QUIZ_COMPLETED") \
        .groupBy("user_id", "course_id") \
        .agg(
            count("quiz_id").alias("quizzes_completed"),
            avg("correct_answers").alias("avg_quiz_correct")
        )
    
    # Assignment grades
    assignment_perf = df.filter(col("event_type") == "GRADE_ASSIGNMENT") \
        .groupBy(col("student_id").alias("user_id"), "course_id") \
        .agg(
            count("assignment_id").alias("assignments_graded"),
            avg("score").alias("avg_assignment_score")
        )
    
    # Combine
    return quiz_perf.join(assignment_perf, ["user_id", "course_id"], "outer") \
        .orderBy("user_id", "course_id")

def main(input_path, output_path):
    """Main batch job execution"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[ASSESSMENT BATCH] Reading assessment events from: {input_path}")
    
    # Read raw assessment events from MinIO
    df_raw = spark.read.parquet(f"{input_path}/topic=assessment_topic")
    
    # Parse timestamp
    df = df_raw.withColumn("timestamp_parsed", col("timestamp").cast(TimestampType()))
    
    print(f"[ASSESSMENT BATCH] Total assessment events: {df.count()}")
    
    # Compute batch views
    print("[ASSESSMENT BATCH] Computing student submission stats...")
    submission_stats = compute_student_submission_stats(df)
    submission_stats.write.mode("overwrite").parquet(f"{output_path}/assessment_student_submissions")
    
    print("[ASSESSMENT BATCH] Computing assignment engagement timeline...")
    engagement_timeline = compute_assignment_engagement_timeline(df)
    engagement_timeline.write.mode("overwrite").parquet(f"{output_path}/assessment_engagement_timeline")
    
    print("[ASSESSMENT BATCH] Computing quiz performance...")
    quiz_performance = compute_quiz_performance(df)
    quiz_performance.write.mode("overwrite").parquet(f"{output_path}/assessment_quiz_performance")
    
    print("[ASSESSMENT BATCH] Computing grading stats...")
    grading_stats = compute_grading_stats(df)
    grading_stats.write.mode("overwrite").parquet(f"{output_path}/assessment_grading_stats")
    
    print("[ASSESSMENT BATCH] Computing teacher grading workload...")
    teacher_workload = compute_teacher_grading_workload(df)
    teacher_workload.write.mode("overwrite").parquet(f"{output_path}/assessment_teacher_workload")
    
    print("[ASSESSMENT BATCH] Computing submission distribution...")
    submission_dist = compute_assignment_submission_distribution(df)
    submission_dist.write.mode("overwrite").parquet(f"{output_path}/assessment_submission_distribution")
    
    print("[ASSESSMENT BATCH] Computing overall student performance...")
    overall_perf = compute_student_overall_performance(df)
    overall_perf.write.mode("overwrite").parquet(f"{output_path}/assessment_student_overall_performance")
    
    print("[ASSESSMENT BATCH] Assessment batch job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: assessment_batch_job.py <input_path> <output_path>")
        print("Example: assessment_batch_job.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
