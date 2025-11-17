"""
Batch Layer - Data Processing Module
Performs complex transformations, aggregations, and analytics
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchDataProcessor:
    """Handles batch data processing and transformations"""
    
    def __init__(self, hdfs_uri="hdfs://namenode:9000"):
        """Initialize batch processor"""
        
        self.hdfs_uri = hdfs_uri
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-BatchProcessing") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
            .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .config("spark.sql.shuffle.partitions", "16") \
            .getOrCreate()
        
        # Enable broadcast joins for small tables
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
        
        logger.info("Batch processor initialized")
    
    def load_raw_data(self):
        """Load all raw data from HDFS"""
        
        logger.info("Loading raw data from HDFS...")
        
        base_path = f"{self.hdfs_uri}/edu-analytics/raw"
        
        self.students = self.spark.read.parquet(f"{base_path}/students")
        self.teachers = self.spark.read.parquet(f"{base_path}/teachers")
        self.courses = self.spark.read.parquet(f"{base_path}/courses")
        self.classes = self.spark.read.parquet(f"{base_path}/classes")
        self.enrollments = self.spark.read.parquet(f"{base_path}/enrollments")
        self.grades = self.spark.read.parquet(f"{base_path}/grades")
        self.sessions = self.spark.read.parquet(f"{base_path}/sessions")
        self.attendance = self.spark.read.parquet(f"{base_path}/attendance")
        
        # Cache frequently used dimension tables
        self.students.cache()
        self.teachers.cache()
        self.courses.cache()
        
        logger.info("✓ Raw data loaded and cached")
    
    def clean_and_deduplicate(self):
        """Data cleaning and deduplication"""
        
        logger.info("Cleaning and deduplicating data...")
        
        # Deduplication using window functions
        window_spec = Window.partitionBy("student_id", "class_id") \
            .orderBy(col("ingestion_timestamp").desc())
        
        self.grades_clean = self.grades \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn")
        
        # Remove invalid records
        self.grades_clean = self.grades_clean.filter(
            (col("midterm_score").isNotNull()) & 
            (col("final_score").isNotNull()) &
            (col("midterm_score") >= 0) & 
            (col("midterm_score") <= 10) &
            (col("final_score") >= 0) & 
            (col("final_score") <= 10)
        )
        
        # Attendance deduplication
        window_att = Window.partitionBy("session_id", "student_id") \
            .orderBy(col("ingestion_timestamp").desc())
        
        self.attendance_clean = self.attendance \
            .withColumn("rn", row_number().over(window_att)) \
            .filter(col("rn") == 1) \
            .drop("rn")
        
        logger.info("✓ Data cleaning complete")
    
    def calculate_student_gpa(self):
        """
        Calculate semester and cumulative GPA for each student
        Using complex joins and aggregations
        """
        
        logger.info("Calculating student GPA...")
        
        # Join grades with courses to get credits
        grades_with_credits = self.grades_clean \
            .join(broadcast(self.classes), "class_id") \
            .join(broadcast(self.courses), "course_id") \
            .select(
                "student_id",
                "semester",
                "class_id",
                "course_id",
                "course_name",
                "credits",
                "total_score",
                "passed"
            )
        
        # Calculate semester GPA (weighted average)
        semester_gpa = grades_with_credits \
            .groupBy("student_id", "semester") \
            .agg(
                (sum(col("total_score") * col("credits")) / sum("credits")).alias("semester_gpa"),
                sum("credits").alias("total_credits"),
                sum(when(col("passed") == True, col("credits")).otherwise(0)).alias("passed_credits"),
                count("*").alias("courses_taken"),
                sum(when(col("passed") == True, 1).otherwise(0)).alias("courses_passed")
            ) \
            .withColumn("pass_rate", col("courses_passed") / col("courses_taken") * 100)
        
        # Calculate cumulative GPA
        window_cumulative = Window.partitionBy("student_id") \
            .orderBy("semester") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        cumulative_gpa = semester_gpa \
            .withColumn("cumulative_credits", sum("total_credits").over(window_cumulative)) \
            .withColumn("cumulative_passed_credits", sum("passed_credits").over(window_cumulative)) \
            .withColumn(
                "cumulative_gpa",
                sum(col("semester_gpa") * col("total_credits")).over(window_cumulative) / 
                sum("total_credits").over(window_cumulative)
            )
        
        # Classify students by performance
        self.student_gpa = cumulative_gpa \
            .withColumn(
                "performance_tier",
                when(col("cumulative_gpa") >= 3.6, "Excellent")
                .when(col("cumulative_gpa") >= 3.0, "Good")
                .when(col("cumulative_gpa") >= 2.5, "Average")
                .when(col("cumulative_gpa") >= 2.0, "Below Average")
                .otherwise("At Risk")
            ) \
            .withColumn("processing_timestamp", current_timestamp())
        
        logger.info(f"✓ GPA calculated for students")
        
        return self.student_gpa
    
    def calculate_class_rankings(self):
        """Calculate student rankings within each class"""
        
        logger.info("Calculating class rankings...")
        
        # Window for ranking within each class
        window_class = Window.partitionBy("class_id") \
            .orderBy(col("total_score").desc())
        
        self.class_rankings = self.grades_clean \
            .withColumn("class_rank", rank().over(window_class)) \
            .withColumn("class_dense_rank", dense_rank().over(window_class)) \
            .withColumn("percentile_rank", percent_rank().over(window_class)) \
            .withColumn("ntile_10", ntile(10).over(window_class))
        
        # Add percentile classification
        self.class_rankings = self.class_rankings \
            .withColumn(
                "performance_decile",
                when(col("ntile_10") <= 1, "Top 10%")
                .when(col("ntile_10") <= 2, "Top 20%")
                .when(col("ntile_10") <= 5, "Top 50%")
                .otherwise("Bottom 50%")
            )
        
        logger.info("✓ Class rankings calculated")
        
        return self.class_rankings
    
    def calculate_attendance_metrics(self):
        """Calculate attendance rates and patterns"""
        
        logger.info("Calculating attendance metrics...")
        
        # Join attendance with sessions and enrollments
        attendance_full = self.attendance_clean \
            .join(self.sessions, "session_id") \
            .join(self.enrollments, ["class_id", "student_id"])
        
        # Calculate attendance rates per student per class
        attendance_by_class = attendance_full \
            .groupBy("student_id", "class_id", "semester") \
            .agg(
                count("*").alias("total_sessions"),
                sum(when(col("status") == "present", 1).otherwise(0)).alias("present_count"),
                sum(when(col("status") == "absent", 1).otherwise(0)).alias("absent_count"),
                sum(when(col("status") == "excused", 1).otherwise(0)).alias("excused_count")
            ) \
            .withColumn("attendance_rate", col("present_count") / col("total_sessions") * 100) \
            .withColumn("absence_rate", col("absent_count") / col("total_sessions") * 100)
        
        # Calculate semester-level attendance
        semester_attendance = attendance_by_class \
            .groupBy("student_id", "semester") \
            .agg(
                avg("attendance_rate").alias("avg_attendance_rate"),
                sum("absent_count").alias("total_absences"),
                count("*").alias("classes_enrolled")
            ) \
            .withColumn(
                "attendance_status",
                when(col("avg_attendance_rate") >= 90, "Excellent")
                .when(col("avg_attendance_rate") >= 80, "Good")
                .when(col("avg_attendance_rate") >= 70, "Acceptable")
                .otherwise("Poor")
            )
        
        self.attendance_metrics = semester_attendance
        
        logger.info("✓ Attendance metrics calculated")
        
        return self.attendance_metrics
    
    def calculate_course_statistics(self):
        """Calculate course difficulty and performance statistics"""
        
        logger.info("Calculating course statistics...")
        
        # Aggregate grades by course and semester
        course_stats = self.grades_clean \
            .join(self.classes, "class_id") \
            .groupBy("course_id", "semester") \
            .agg(
                count("*").alias("enrollment_count"),
                avg("total_score").alias("avg_score"),
                stddev("total_score").alias("stddev_score"),
                min("total_score").alias("min_score"),
                max("total_score").alias("max_score"),
                expr("percentile_approx(total_score, 0.25)").alias("q1_score"),
                expr("percentile_approx(total_score, 0.5)").alias("median_score"),
                expr("percentile_approx(total_score, 0.75)").alias("q3_score"),
                sum(when(col("passed") == True, 1).otherwise(0)).alias("passed_count"),
                sum(when(col("passed") == False, 1).otherwise(0)).alias("failed_count")
            ) \
            .withColumn("pass_rate", col("passed_count") / col("enrollment_count") * 100) \
            .withColumn("fail_rate", col("failed_count") / col("enrollment_count") * 100)
        
        # Classify course difficulty
        self.course_statistics = course_stats \
            .withColumn(
                "difficulty_level",
                when((col("pass_rate") >= 80) & (col("avg_score") >= 7.0), "Easy")
                .when((col("pass_rate") >= 70) & (col("avg_score") >= 6.0), "Moderate")
                .when((col("pass_rate") >= 60) & (col("avg_score") >= 5.5), "Challenging")
                .otherwise("Difficult")
            ) \
            .join(broadcast(self.courses), "course_id")
        
        logger.info("✓ Course statistics calculated")
        
        return self.course_statistics
    
    def calculate_faculty_performance(self):
        """Calculate teacher performance metrics"""
        
        logger.info("Calculating faculty performance...")
        
        # Get all classes taught by each teacher
        teacher_classes = self.classes \
            .join(self.grades_clean, "class_id") \
            .join(broadcast(self.courses), "course_id") \
            .join(broadcast(self.teachers), "teacher_id")
        
        # Aggregate by teacher and semester
        faculty_perf = teacher_classes \
            .groupBy("teacher_id", "full_name", "semester") \
            .agg(
                countDistinct("class_id").alias("classes_taught"),
                count("student_id").alias("total_students"),
                avg("total_score").alias("avg_student_score"),
                avg(when(col("passed") == True, 1).otherwise(0)).alias("pass_rate")
            )
        
        # Calculate performance ranking
        window_semester = Window.partitionBy("semester") \
            .orderBy(col("avg_student_score").desc())
        
        self.faculty_performance = faculty_perf \
            .withColumn("faculty_rank", rank().over(window_semester)) \
            .withColumn(
                "performance_category",
                when(col("avg_student_score") >= 7.5, "Outstanding")
                .when(col("avg_student_score") >= 6.5, "Above Average")
                .when(col("avg_student_score") >= 5.5, "Average")
                .otherwise("Below Average")
            )
        
        logger.info("✓ Faculty performance calculated")
        
        return self.faculty_performance
    
    def create_student_profile_view(self):
        """Create comprehensive student profile view"""
        
        logger.info("Creating student profile view...")
        
        # Join student GPA with attendance
        student_profile = self.students \
            .join(self.student_gpa, "student_id", "left") \
            .join(self.attendance_metrics, ["student_id", "semester"], "left")
        
        # Add engagement metrics (if available)
        # This would join with speed layer results in production
        
        self.student_profile_view = student_profile \
            .withColumn("profile_generated_at", current_timestamp())
        
        logger.info("✓ Student profile view created")
        
        return self.student_profile_view
    
    def write_batch_views(self):
        """Write processed batch views to HDFS"""
        
        logger.info("Writing batch views to HDFS...")
        
        base_path = f"{self.hdfs_uri}/edu-analytics/views/batch"
        
        # Student GPA view (partitioned by semester)
        self.student_gpa.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/student_gpa")
        logger.info("  ✓ student_gpa written")
        
        # Class rankings (partitioned by semester)
        self.class_rankings.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/class_rankings")
        logger.info("  ✓ class_rankings written")
        
        # Attendance metrics (partitioned by semester)
        self.attendance_metrics.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/attendance_metrics")
        logger.info("  ✓ attendance_metrics written")
        
        # Course statistics (partitioned by semester)
        self.course_statistics.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/course_statistics")
        logger.info("  ✓ course_statistics written")
        
        # Faculty performance (partitioned by semester)
        self.faculty_performance.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/faculty_performance")
        logger.info("  ✓ faculty_performance written")
        
        # Student profile view (partitioned by semester)
        self.student_profile_view.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/student_profiles")
        logger.info("  ✓ student_profiles written")
        
        logger.info("✓ All batch views written to HDFS")
    
    def run_batch_pipeline(self):
        """Execute complete batch processing pipeline"""
        
        logger.info("=" * 70)
        logger.info("BATCH PROCESSING PIPELINE - Starting")
        logger.info("=" * 70)
        
        # Load data
        self.load_raw_data()
        
        # Clean data
        self.clean_and_deduplicate()
        
        # Calculate metrics
        self.calculate_student_gpa()
        self.calculate_class_rankings()
        self.calculate_attendance_metrics()
        self.calculate_course_statistics()
        self.calculate_faculty_performance()
        self.create_student_profile_view()
        
        # Write results
        self.write_batch_views()
        
        logger.info("=" * 70)
        logger.info("✓ BATCH PROCESSING COMPLETE")
        logger.info("=" * 70)
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution function"""
    
    import os
    HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")
    
    try:
        processor = BatchDataProcessor(hdfs_uri=HDFS_URI)
        processor.run_batch_pipeline()
    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}")
        raise
    finally:
        processor.cleanup()


if __name__ == "__main__":
    main()