"""
Assessment Stream Processing Job
Real-time processing of assessment/assignment events from Kafka

Real-time Views Generated:
1. assessment_realtime_submissions_1min - Submission rate in 1-minute windows
2. assessment_realtime_active_students - Students actively working on assessments
3. assessment_realtime_quiz_attempts - Real-time quiz attempt tracking
4. assessment_realtime_grading_queue - Pending grading items
5. assessment_realtime_submission_rate - Submission patterns and trends
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, countDistinct,
    sum as spark_sum, avg, min as spark_min, max as spark_max,
    current_timestamp, to_timestamp, expr, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
)
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import speed_config, get_kafka_connection_options, get_output_path, get_checkpoint_path, get_spark_config


# Define schema for assessment events
assessment_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_category", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("course_id", StringType(), True),
    StructField("assignment_id", StringType(), True),
    StructField("quiz_id", StringType(), True),
    StructField("question_id", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("max_score", DoubleType(), True),
    StructField("correct_answers", IntegerType(), True),
    StructField("total_questions", IntegerType(), True)
])


def create_spark_session():
    """Create and configure Spark session for streaming"""
    builder = SparkSession.builder \
        .appName("SpeedLayer_AssessmentStreaming") \
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
        .select(from_json(col("json_value"), assessment_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return parsed_df


def compute_submissions_1min(df):
    """
    Real-time View: assessment_realtime_submissions_1min
    Track submission rate per minute with score statistics
    """
    submissions = df.filter(col("action") == "SUBMIT_ASSIGNMENT")
    
    result = submissions \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            count("*").alias("total_submissions"),
            countDistinct("user_id").alias("unique_students"),
            countDistinct("assignment_id").alias("unique_assignments"),
            avg("score").alias("avg_score"),
            spark_min("score").alias("min_score"),
            spark_max("score").alias("max_score")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_submissions",
            "unique_students",
            "unique_assignments",
            expr("ROUND(avg_score, 2)").alias("avg_score"),
            "min_score",
            "max_score",
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_active_students(df):
    """
    Real-time View: assessment_realtime_active_students
    Track students actively working on assessments in 5-minute windows
    """
    result = df \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "user_id",
            "course_id"
        ) \
        .agg(
            count("*").alias("event_count"),
            countDistinct("action").alias("distinct_actions"),
            spark_sum(when(col("action") == "VIEW_ASSIGNMENT_FIRST_TIME", 1).otherwise(0)).alias("views"),
            spark_sum(when(col("action") == "SUBMIT_ASSIGNMENT", 1).otherwise(0)).alias("submissions"),
            spark_sum(when(col("action") == "START_QUIZ", 1).otherwise(0)).alias("quiz_starts")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_id",
            "course_id",
            "event_count",
            "distinct_actions",
            "views",
            "submissions",
            "quiz_starts",
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_quiz_attempts(df):
    """
    Real-time View: assessment_realtime_quiz_attempts
    Track quiz attempts and performance in real-time
    """
    quiz_events = df.filter(col("action").isin(["START_QUIZ", "ANSWER_QUESTION", "COMPLETE_QUIZ"]))
    
    result = quiz_events \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "quiz_id",
            "user_id"
        ) \
        .agg(
            count("*").alias("event_count"),
            spark_sum(when(col("action") == "START_QUIZ", 1).otherwise(0)).alias("starts"),
            spark_sum(when(col("action") == "ANSWER_QUESTION", 1).otherwise(0)).alias("answers"),
            spark_sum(when(col("action") == "COMPLETE_QUIZ", 1).otherwise(0)).alias("completions"),
            avg("correct_answers").alias("avg_correct_answers"),
            avg("total_questions").alias("avg_total_questions")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "quiz_id",
            "user_id",
            "event_count",
            "starts",
            "answers",
            "completions",
            expr("ROUND(avg_correct_answers, 2)").alias("avg_correct_answers"),
            expr("ROUND(avg_total_questions, 2)").alias("avg_total_questions"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_grading_queue(df):
    """
    Real-time View: assessment_realtime_grading_queue
    Monitor pending grading items in real-time
    """
    submissions = df.filter(col("action") == "SUBMIT_ASSIGNMENT")
    graded = df.filter(col("action") == "GRADE_ASSIGNMENT")
    
    # Count submissions
    submission_counts = submissions \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "assignment_id"
        ) \
        .agg(
            count("*").alias("submissions"),
            countDistinct("user_id").alias("students_submitted")
        )
    
    # Count graded items
    graded_counts = graded \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "assignment_id"
        ) \
        .agg(
            count("*").alias("graded"),
            countDistinct("user_id").alias("students_graded")
        )
    
    # Join to calculate queue
    result = submission_counts.alias("sub") \
        .join(
            graded_counts.alias("grd"),
            [col("sub.window") == col("grd.window"),
             col("sub.assignment_id") == col("grd.assignment_id")],
            "left_outer"
        ) \
        .select(
            col("sub.window.start").alias("window_start"),
            col("sub.window.end").alias("window_end"),
            col("sub.assignment_id").alias("assignment_id"),
            col("sub.submissions").alias("submissions"),
            col("sub.students_submitted").alias("students_submitted"),
            when(col("grd.graded").isNotNull(), col("grd.graded")).otherwise(0).alias("graded"),
            when(col("grd.students_graded").isNotNull(), col("grd.students_graded")).otherwise(0).alias("students_graded"),
            expr("submissions - COALESCE(graded, 0)").alias("pending_grading"),
            current_timestamp().alias("processed_at")
        )
    
    return result


def compute_submission_rate(df):
    """
    Real-time View: assessment_realtime_submission_rate
    Calculate submission rate trends with 1-minute sliding windows
    """
    submissions = df.filter(col("action") == "SUBMIT_ASSIGNMENT")
    
    result = submissions \
        .withWatermark("timestamp", speed_config["windows"]["watermark"]) \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            "course_id"
        ) \
        .agg(
            count("*").alias("submissions"),
            countDistinct("user_id").alias("unique_students"),
            countDistinct("assignment_id").alias("unique_assignments"),
            avg("score").alias("avg_score"),
            avg(expr("(score / max_score) * 100")).alias("avg_percentage")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "course_id",
            "submissions",
            "unique_students",
            "unique_assignments",
            expr("ROUND(avg_score, 2)").alias("avg_score"),
            expr("ROUND(avg_percentage, 2)").alias("avg_percentage"),
            expr("ROUND(submissions / 5.0, 2)").alias("submissions_per_minute"),
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
    print("Starting Assessment Stream Processing Job...")
    
    spark = create_spark_session()
    assessment_stream = read_kafka_stream(spark, speed_config["kafka"]["topics"]["assessment"])
    
    # Compute real-time views
    submissions_1min = compute_submissions_1min(assessment_stream)
    active_students = compute_active_students(assessment_stream)
    quiz_attempts = compute_quiz_attempts(assessment_stream)
    grading_queue = compute_grading_queue(assessment_stream)
    submission_rate = compute_submission_rate(assessment_stream)
    
    # Write streams
    queries = [
        write_stream(submissions_1min, "assessment_realtime_submissions_1min", "assessment_stream"),
        write_stream(active_students, "assessment_realtime_active_students", "assessment_stream"),
        write_stream(quiz_attempts, "assessment_realtime_quiz_attempts", "assessment_stream"),
        write_stream(grading_queue, "assessment_realtime_grading_queue", "assessment_stream"),
        write_stream(submission_rate, "assessment_realtime_submission_rate", "assessment_stream")
    ]
    
    print(f"Started {len(queries)} streaming queries for Assessment events")
    print("Real-time views being generated:")
    for view in speed_config["realtime_views"]["assessment"]:
        print(f"  - {view}")
    
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
