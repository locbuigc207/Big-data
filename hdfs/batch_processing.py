"""
Batch Processing Layer - Complete Implementation
Performs advanced Spark transformations, aggregations, and analytics
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchProcessor:
    """Advanced Batch Processing with Spark"""
    
    def __init__(self, hdfs_namenode="hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"):
        """Initialize batch processor for Kubernetes"""
        
        self.hdfs_namenode = hdfs_namenode
        self.processing_date = datetime.now().strftime("%Y-%m-%d")
        
        # Initialize Spark with optimizations
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-BatchProcessing") \
            .config("spark.master", "k8s://https://kubernetes.default.svc:443") \
            .config("spark.kubernetes.namespace", "bigdata") \
            .config("spark.hadoop.fs.defaultFS", hdfs_namenode) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
            .config("spark.sql.shuffle.partitions", "20") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.instances", "4") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        logger.info(f"Spark version: {self.spark.version}")
        logger.info(f"Processing date: {self.processing_date}")
        
    def load_raw_data(self):
        """Load all raw data from HDFS with latest date"""
        
        logger.info("Loading raw data from HDFS...")
        
        base_path = f"{self.hdfs_namenode}/raw"
        
        # Find latest date for each table
        def get_latest_path(table_name):
            # In production, you'd query HDFS for latest date
            # For now, using processing_date
            return f"{base_path}/{table_name}/{self.processing_date}"
        
        # Load all tables
        self.students = self.spark.read.parquet(get_latest_path("students"))
        self.teachers = self.spark.read.parquet(get_latest_path("teachers"))
        self.courses = self.spark.read.parquet(get_latest_path("courses"))
        self.classes = self.spark.read.parquet(get_latest_path("classes"))
        self.enrollments = self.spark.read.parquet(get_latest_path("enrollments"))
        self.grades = self.spark.read.parquet(get_latest_path("grades"))
        self.sessions = self.spark.read.parquet(get_latest_path("sessions"))
        self.attendance = self.spark.read.parquet(get_latest_path("attendance"))
        
        # Cache frequently accessed dimension tables
        self.students.cache()
        self.teachers.cache()
        self.courses.cache()
        
        logger.info("Raw data loaded:")
        logger.info(f"  Students: {self.students.count():,}")
        logger.info(f"  Teachers: {self.teachers.count():,}")
        logger.info(f"  Courses: {self.courses.count():,}")
        logger.info(f"  Classes: {self.classes.count():,}")
        logger.info(f"  Enrollments: {self.enrollments.count():,}")
        logger.info(f"  Grades: {self.grades.count():,}")
        logger.info(f"  Sessions: {self.sessions.count():,}")
        logger.info(f"  Attendance: {self.attendance.count():,}")
    
    def clean_and_deduplicate(self):
        """
        Data cleaning and deduplication using window functions
        Demonstrates: Window functions, row_number, filtering
        """
        
        logger.info("=" * 70)
        logger.info("STEP 1: Data Cleaning & Deduplication")
        logger.info("=" * 70)
        
        # 1. Deduplicate grades using window function
        logger.info("Deduplicating grades...")
        
        window_grades = Window.partitionBy("student_id", "class_id", "semester") \
            .orderBy(col("ingestion_timestamp").desc())
        
        self.grades_clean = self.grades \
            .withColumn("row_num", row_number().over(window_grades)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        duplicates_removed = self.grades.count() - self.grades_clean.count()
        logger.info(f"  Removed {duplicates_removed} duplicate grade records")
        
        # 2. Validate grade ranges
        logger.info("Validating grade ranges...")
        
        self.grades_clean = self.grades_clean.filter(
            (col("midterm_score").isNotNull()) &
            (col("final_score").isNotNull()) &
            (col("midterm_score") >= 0) & (col("midterm_score") <= 10) &
            (col("final_score") >= 0) & (col("final_score") <= 10) &
            (col("total_score") >= 0) & (col("total_score") <= 10)
        )
        
        # 3. Deduplicate attendance
        logger.info("Deduplicating attendance...")
        
        window_attendance = Window.partitionBy("session_id", "student_id") \
            .orderBy(col("ingestion_timestamp").desc())
        
        self.attendance_clean = self.attendance \
            .withColumn("row_num", row_number().over(window_attendance)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        # 4. Add data quality flags
        logger.info("Adding data quality flags...")
        
        self.grades_clean = self.grades_clean \
            .withColumn(
                "score_consistency_flag",
                when(
                    abs(col("total_score") - 
                        (col("midterm_score") * col("weight_mid") + 
                         col("final_score") * col("weight_final"))) > 0.1,
                    "INCONSISTENT"
                ).otherwise("CONSISTENT")
            ) \
            .withColumn("processing_timestamp", current_timestamp())
        
        logger.info(f"✓ Data cleaning complete")
        logger.info(f"  Clean grades: {self.grades_clean.count():,}")
        logger.info(f"  Clean attendance: {self.attendance_clean.count():,}")
    
    def calculate_student_gpa(self):
        """
        Calculate semester and cumulative GPA
        Demonstrates: Complex joins, broadcast joins, custom aggregations, window functions
        """
        
        logger.info("=" * 70)
        logger.info("STEP 2: Calculate Student GPA")
        logger.info("=" * 70)
        
        # 1. Join grades with courses to get credits (BROADCAST JOIN)
        logger.info("Performing broadcast join: grades × courses...")
        
        grades_with_credits = self.grades_clean \
            .join(broadcast(self.classes.select("class_id", "course_id", "semester")), "class_id") \
            .join(broadcast(self.courses.select("course_id", "course_name", "credits")), "course_id") \
            .select(
                "student_id",
                "class_id", 
                "course_id",
                "course_name",
                "semester",
                "credits",
                "midterm_score",
                "final_score",
                "total_score",
                "passed",
                "weight_mid",
                "weight_final"
            )
        
        logger.info(f"  Joined {grades_with_credits.count():,} records")
        
        # 2. Calculate semester GPA using custom weighted average
        logger.info("Calculating semester GPA...")
        
        semester_gpa = grades_with_credits \
            .groupBy("student_id", "semester") \
            .agg(
                # Weighted GPA calculation
                (sum(col("total_score") * col("credits")) / sum("credits")).alias("semester_gpa"),
                
                # Credit statistics
                sum("credits").alias("total_credits"),
                sum(when(col("passed") == True, col("credits")).otherwise(0)).alias("passed_credits"),
                sum(when(col("passed") == False, col("credits")).otherwise(0)).alias("failed_credits"),
                
                # Course statistics
                count("*").alias("courses_taken"),
                sum(when(col("passed") == True, 1).otherwise(0)).alias("courses_passed"),
                sum(when(col("passed") == False, 1).otherwise(0)).alias("courses_failed"),
                
                # Score statistics
                avg("total_score").alias("avg_score"),
                min("total_score").alias("min_score"),
                max("total_score").alias("max_score"),
                stddev("total_score").alias("stddev_score")
            ) \
            .withColumn("pass_rate", col("courses_passed") / col("courses_taken") * 100) \
            .withColumn("credit_completion_rate", col("passed_credits") / col("total_credits") * 100)
        
        # 3. Calculate cumulative statistics using WINDOW FUNCTIONS
        logger.info("Calculating cumulative GPA with window functions...")
        
        window_cumulative = Window.partitionBy("student_id") \
            .orderBy("semester") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        window_previous = Window.partitionBy("student_id") \
            .orderBy("semester")
        
        cumulative_gpa = semester_gpa \
            .withColumn("cumulative_credits", sum("total_credits").over(window_cumulative)) \
            .withColumn("cumulative_passed_credits", sum("passed_credits").over(window_cumulative)) \
            .withColumn("cumulative_courses", sum("courses_taken").over(window_cumulative)) \
            .withColumn(
                "cumulative_gpa",
                sum(col("semester_gpa") * col("total_credits")).over(window_cumulative) / 
                sum("total_credits").over(window_cumulative)
            ) \
            .withColumn("prev_semester_gpa", lag("semester_gpa", 1).over(window_previous)) \
            .withColumn("gpa_trend", col("semester_gpa") - coalesce(col("prev_semester_gpa"), col("semester_gpa")))
        
        # 4. Classify students by performance (CASE WHEN)
        logger.info("Classifying student performance...")
        
        self.student_gpa = cumulative_gpa \
            .withColumn(
                "performance_tier",
                when(col("cumulative_gpa") >= 3.6, "Excellent")
                .when(col("cumulative_gpa") >= 3.2, "Very Good")
                .when(col("cumulative_gpa") >= 2.5, "Good")
                .when(col("cumulative_gpa") >= 2.0, "Average")
                .otherwise("At Risk")
            ) \
            .withColumn(
                "academic_standing",
                when(col("cumulative_gpa") >= 3.6, "Dean's List")
                .when(col("cumulative_gpa") >= 2.0, "Good Standing")
                .when(col("cumulative_gpa") >= 1.5, "Academic Warning")
                .otherwise("Academic Probation")
            ) \
            .withColumn(
                "gpa_trajectory",
                when(col("gpa_trend") > 0.3, "Improving")
                .when(col("gpa_trend") < -0.3, "Declining")
                .otherwise("Stable")
            )
        
        # Add join with student info
        self.student_gpa = self.student_gpa \
            .join(broadcast(self.students.select("student_id", "full_name", "faculty", "email")), 
                  "student_id", "left")
        
        logger.info(f"✓ GPA calculation complete: {self.student_gpa.count():,} records")
        
        # Show sample
        logger.info("\nSample GPA records:")
        self.student_gpa.select(
            "student_id", "full_name", "semester", "semester_gpa", 
            "cumulative_gpa", "performance_tier", "gpa_trajectory"
        ).show(5, truncate=False)
    
    def calculate_class_rankings(self):
        """
        Calculate student rankings within each class
        Demonstrates: rank, dense_rank, percent_rank, ntile
        """
        
        logger.info("=" * 70)
        logger.info("STEP 3: Calculate Class Rankings")
        logger.info("=" * 70)
        
        # Window for ranking within each class
        window_class = Window.partitionBy("class_id", "semester") \
            .orderBy(col("total_score").desc())
        
        window_course = Window.partitionBy("course_id", "semester") \
            .orderBy(col("total_score").desc())
        
        # Apply multiple ranking functions
        self.class_rankings = self.grades_clean \
            .join(broadcast(self.classes.select("class_id", "course_id", "semester")), "class_id") \
            .withColumn("class_rank", rank().over(window_class)) \
            .withColumn("class_dense_rank", dense_rank().over(window_class)) \
            .withColumn("class_percentile", percent_rank().over(window_class) * 100) \
            .withColumn("class_decile", ntile(10).over(window_class)) \
            .withColumn("course_rank", rank().over(window_course)) \
            .withColumn("course_percentile", percent_rank().over(window_course) * 100)
        
        # Add performance categories
        self.class_rankings = self.class_rankings \
            .withColumn(
                "class_performance_category",
                when(col("class_decile") <= 1, "Top 10%")
                .when(col("class_decile") <= 2, "Top 20%")
                .when(col("class_decile") <= 5, "Top 50%")
                .otherwise("Bottom 50%")
            ) \
            .withColumn(
                "grade_letter",
                when(col("total_score") >= 8.5, "A")
                .when(col("total_score") >= 8.0, "A-")
                .when(col("total_score") >= 7.0, "B+")
                .when(col("total_score") >= 6.5, "B")
                .when(col("total_score") >= 5.5, "B-")
                .when(col("total_score") >= 5.0, "C+")
                .when(col("total_score") >= 4.0, "C")
                .otherwise("F")
            )
        
        # Join with student names
        self.class_rankings = self.class_rankings \
            .join(broadcast(self.students.select("student_id", "full_name")), "student_id", "left")
        
        logger.info(f"✓ Rankings calculated: {self.class_rankings.count():,} records")
        
        # Show top performers
        logger.info("\nTop 5 performers per class (sample):")
        self.class_rankings \
            .filter(col("class_rank") <= 5) \
            .select("class_id", "student_id", "full_name", "total_score", 
                   "class_rank", "class_percentile", "grade_letter") \
            .orderBy("class_id", "class_rank") \
            .show(10, truncate=False)
    
    def calculate_attendance_metrics(self):
        """
        Calculate comprehensive attendance metrics
        Demonstrates: Aggregations, join strategies, complex transformations
        """
        
        logger.info("=" * 70)
        logger.info("STEP 4: Calculate Attendance Metrics")
        logger.info("=" * 70)
        
        # 1. Join attendance with sessions and enrollments
        logger.info("Joining attendance data...")
        
        attendance_full = self.attendance_clean \
            .join(self.sessions.select("session_id", "class_id", "date"), "session_id") \
            .join(self.enrollments.select("student_id", "class_id", "semester"), 
                  ["student_id", "class_id"])
        
        # 2. Calculate attendance by class
        logger.info("Calculating class-level attendance...")
        
        attendance_by_class = attendance_full \
            .groupBy("student_id", "class_id", "semester") \
            .agg(
                count("*").alias("total_sessions"),
                sum(when(col("status") == "present", 1).otherwise(0)).alias("present_count"),
                sum(when(col("status") == "absent", 1).otherwise(0)).alias("absent_count"),
                sum(when(col("status") == "excused", 1).otherwise(0)).alias("excused_count"),
                countDistinct("date").alias("unique_days")
            ) \
            .withColumn("attendance_rate", 
                       round(col("present_count") / col("total_sessions") * 100, 2)) \
            .withColumn("absence_rate", 
                       round(col("absent_count") / col("total_sessions") * 100, 2)) \
            .withColumn("excused_rate",
                       round(col("excused_count") / col("total_sessions") * 100, 2))
        
        # 3. Calculate semester-level attendance
        logger.info("Calculating semester-level attendance...")
        
        semester_attendance = attendance_by_class \
            .groupBy("student_id", "semester") \
            .agg(
                avg("attendance_rate").alias("avg_attendance_rate"),
                min("attendance_rate").alias("min_attendance_rate"),
                max("attendance_rate").alias("max_attendance_rate"),
                sum("absent_count").alias("total_absences"),
                sum("excused_count").alias("total_excused"),
                count("class_id").alias("classes_enrolled"),
                stddev("attendance_rate").alias("attendance_consistency")
            ) \
            .withColumn(
                "attendance_status",
                when(col("avg_attendance_rate") >= 95, "Excellent")
                .when(col("avg_attendance_rate") >= 85, "Very Good")
                .when(col("avg_attendance_rate") >= 75, "Good")
                .when(col("avg_attendance_rate") >= 60, "Acceptable")
                .otherwise("Poor")
            ) \
            .withColumn(
                "at_risk_attendance",
                when(col("avg_attendance_rate") < 70, True).otherwise(False)
            )
        
        # 4. Attendance patterns by day of week
        logger.info("Analyzing attendance patterns...")
        
        self.attendance_metrics = semester_attendance
        
        # Join with student info
        self.attendance_metrics = self.attendance_metrics \
            .join(broadcast(self.students.select("student_id", "full_name", "faculty")), 
                  "student_id", "left")
        
        logger.info(f"✓ Attendance metrics calculated: {self.attendance_metrics.count():,} records")
        
        # Show attendance distribution
        logger.info("\nAttendance status distribution:")
        self.attendance_metrics.groupBy("attendance_status") \
            .count() \
            .orderBy(col("count").desc()) \
            .show()
    
    def calculate_course_statistics(self):
        """
        Calculate comprehensive course statistics
        Demonstrates: CUBE, ROLLUP, percentile_approx, complex aggregations
        """
        
        logger.info("=" * 70)
        logger.info("STEP 5: Calculate Course Statistics")
        logger.info("=" * 70)
        
        # 1. Detailed course statistics
        logger.info("Calculating course statistics...")
        
        course_stats = self.grades_clean \
            .join(self.classes.select("class_id", "course_id", "semester", "teacher_id"), "class_id") \
            .groupBy("course_id", "semester") \
            .agg(
                # Enrollment metrics
                count("*").alias("enrollment_count"),
                countDistinct("student_id").alias("unique_students"),
                
                # Score statistics
                avg("total_score").alias("avg_score"),
                stddev("total_score").alias("stddev_score"),
                min("total_score").alias("min_score"),
                max("total_score").alias("max_score"),
                
                # Percentiles
                expr("percentile_approx(total_score, 0.25)").alias("q1_score"),
                expr("percentile_approx(total_score, 0.50)").alias("median_score"),
                expr("percentile_approx(total_score, 0.75)").alias("q3_score"),
                
                # Pass/Fail metrics
                sum(when(col("passed") == True, 1).otherwise(0)).alias("passed_count"),
                sum(when(col("passed") == False, 1).otherwise(0)).alias("failed_count"),
                
                # Score distribution
                sum(when(col("total_score") >= 8.0, 1).otherwise(0)).alias("score_8_plus"),
                sum(when((col("total_score") >= 7.0) & (col("total_score") < 8.0), 1).otherwise(0)).alias("score_7_8"),
                sum(when((col("total_score") >= 5.0) & (col("total_score") < 7.0), 1).otherwise(0)).alias("score_5_7"),
                sum(when(col("total_score") < 5.0, 1).otherwise(0)).alias("score_below_5")
            ) \
            .withColumn("pass_rate", round(col("passed_count") / col("enrollment_count") * 100, 2)) \
            .withColumn("fail_rate", round(col("failed_count") / col("enrollment_count") * 100, 2)) \
            .withColumn("iqr", col("q3_score") - col("q1_score"))
        
        # 2. Classify course difficulty
        logger.info("Classifying course difficulty...")
        
        course_stats = course_stats \
            .withColumn(
                "difficulty_level",
                when((col("pass_rate") >= 85) & (col("avg_score") >= 7.5), "Easy")
                .when((col("pass_rate") >= 75) & (col("avg_score") >= 6.5), "Moderate")
                .when((col("pass_rate") >= 60) & (col("avg_score") >= 5.5), "Challenging")
                .otherwise("Difficult")
            ) \
            .withColumn(
                "score_distribution_type",
                when(col("stddev_score") < 1.0, "Homogeneous")
                .when(col("stddev_score") < 1.5, "Normal")
                .otherwise("Heterogeneous")
            )
        
        # 3. Join with course details
        self.course_statistics = course_stats \
            .join(broadcast(self.courses.select("course_id", "course_name", "credits")), 
                  "course_id", "left")
        
        # 4. Calculate multi-dimensional aggregations with CUBE
        logger.info("Creating CUBE aggregation...")
        
        self.course_cube = self.grades_clean \
            .join(self.classes.select("class_id", "course_id", "semester"), "class_id") \
            .join(broadcast(self.students.select("student_id", "faculty")), "student_id") \
            .cube("semester", "faculty", "course_id") \
            .agg(
                count("*").alias("enrollment"),
                avg("total_score").alias("avg_score"),
                sum(when(col("passed") == True, 1).otherwise(0)).alias("passed")
            ) \
            .withColumn("pass_rate", 
                       when(col("enrollment") > 0, 
                            round(col("passed") / col("enrollment") * 100, 2))
                       .otherwise(0))
        
        logger.info(f"✓ Course statistics calculated: {self.course_statistics.count():,} records")
        logger.info(f"✓ CUBE aggregation created: {self.course_cube.count():,} combinations")
        
        # Show difficult courses
        logger.info("\nMost difficult courses:")
        self.course_statistics \
            .filter(col("difficulty_level") == "Difficult") \
            .select("course_id", "course_name", "semester", "avg_score", 
                   "pass_rate", "enrollment_count") \
            .orderBy(col("pass_rate").asc()) \
            .show(5, truncate=False)
    
    def calculate_faculty_performance(self):
        """
        Calculate teacher performance metrics
        Demonstrates: Complex joins, aggregations by multiple dimensions
        """
        
        logger.info("=" * 70)
        logger.info("STEP 6: Calculate Faculty Performance")
        logger.info("=" * 70)
        
        # 1. Aggregate by teacher and semester
        logger.info("Calculating faculty metrics...")
        
        faculty_metrics = self.classes \
            .join(self.grades_clean, "class_id") \
            .join(broadcast(self.courses.select("course_id", "course_name")), "course_id") \
            .groupBy("teacher_id", "semester") \
            .agg(
                countDistinct("class_id").alias("classes_taught"),
                countDistinct("course_id").alias("unique_courses"),
                count("student_id").alias("total_students"),
                avg("total_score").alias("avg_student_score"),
                stddev("total_score").alias("stddev_student_score"),
                sum(when(col("passed") == True, 1).otherwise(0)).alias("students_passed"),
                sum(when(col("passed") == False, 1).otherwise(0)).alias("students_failed"),
                min("total_score").alias("min_student_score"),
                max("total_score").alias("max_student_score")
            ) \
            .withColumn("pass_rate", 
                       round(col("students_passed") / col("total_students") * 100, 2)) \
            .withColumn("avg_class_size", 
                       round(col("total_students") / col("classes_taught"), 0))
        
        # 2. Calculate performance ranking
        logger.info("Ranking faculty performance...")
        
        window_semester = Window.partitionBy("semester") \
            .orderBy(col("avg_student_score").desc())
        
        faculty_metrics = faculty_metrics \
            .withColumn("faculty_rank", rank().over(window_semester)) \
            .withColumn("faculty_percentile", 
                       round(percent_rank().over(window_semester) * 100, 2))
        
        # 3. Classify performance
        self.faculty_performance = faculty_metrics \
            .withColumn(
                "performance_category",
                when(col("avg_student_score") >= 7.5, "Outstanding")
                .when(col("avg_student_score") >= 7.0, "Excellent")
                .when(col("avg_student_score") >= 6.0, "Very Good")
                .when(col("avg_student_score") >= 5.5, "Good")
                .otherwise("Needs Improvement")
            ) \
            .withColumn(
                "teaching_consistency",
                when(col("stddev_student_score") < 1.2, "Very Consistent")
                .when(col("stddev_student_score") < 1.8, "Consistent")
                .otherwise("Variable")
            )
        
        # 4. Join with teacher info
        self.faculty_performance = self.faculty_performance \
            .join(broadcast(self.teachers.select("teacher_id", "full_name", "specialty")), 
                  "teacher_id", "left")
        
        logger.info(f"✓ Faculty performance calculated: {self.faculty_performance.count():,} records")
        
        # Show top performers
        logger.info("\nTop performing faculty:")
        self.faculty_performance \
            .filter(col("faculty_rank") <= 5) \
            .select("teacher_id", "full_name", "semester", "classes_taught", 
                   "total_students", "avg_student_score", "pass_rate", "performance_category") \
            .orderBy("semester", "faculty_rank") \
            .show(10, truncate=False)
    
    def identify_at_risk_students(self):
        """
        Identify students at risk of academic failure
        Demonstrates: Complex business logic, multiple criteria evaluation
        """
        
        logger.info("=" * 70)
        logger.info("STEP 7: Identify At-Risk Students")
        logger.info("=" * 70)
        
        # Join GPA and attendance data
        at_risk_analysis = self.student_gpa \
            .join(self.attendance_metrics.select(
                "student_id", "semester", "avg_attendance_rate", "total_absences"
            ), ["student_id", "semester"], "left")
        
        # Define at-risk criteria
        at_risk_students = at_risk_analysis \
            .withColumn(
                "gpa_risk_flag",
                when(col("cumulative_gpa") < 2.0, "HIGH")
                .when(col("cumulative_gpa") < 2.5, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "attendance_risk_flag",
                when(col("avg_attendance_rate") < 60, "HIGH")
                .when(col("avg_attendance_rate") < 75, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "gpa_trend_risk_flag",
                when(col("gpa_trend") < -0.5, "HIGH")
                .when(col("gpa_trend") < -0.2, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "fail_rate_risk_flag",
                when(col("pass_rate") < 50, "HIGH")
                .when(col("pass_rate") < 70, "MEDIUM")
                .otherwise("LOW")
            )
        
        # Calculate overall risk score (0-100)
        at_risk_students = at_risk_students \
            .withColumn(
                "risk_score",
                (
                    # GPA component (40 points)
                    when(col("cumulative_gpa") >= 3.0, 0)
                    .when(col("cumulative_gpa") >= 2.5, 10)
                    .when(col("cumulative_gpa") >= 2.0, 20)
                    .when(col("cumulative_gpa") >= 1.5, 30)
                    .otherwise(40) +
                    
                    # Attendance component (30 points)
                    when(col("avg_attendance_rate") >= 85, 0)
                    .when(col("avg_attendance_rate") >= 75, 10)
                    .when(col("avg_attendance_rate") >= 60, 20)
                    .otherwise(30) +
                    
                    # GPA trend component (20 points)
                    when(col("gpa_trend") > 0, 0)
                    .when(col("gpa_trend") > -0.3, 10)
                    .otherwise(20) +
                    
                    # Pass rate component (10 points)
                    when(col("pass_rate") >= 80, 0)
                    .when(col("pass_rate") >= 60, 5)
                    .otherwise(10)
                )
            ) \
            .withColumn(
                "overall_risk_level",
                when(col("risk_score") >= 60, "CRITICAL")
                .when(col("risk_score") >= 40, "HIGH")
                .when(col("risk_score") >= 20, "MEDIUM")
                .otherwise("LOW")
            ) \
            .withColumn(
                "intervention_priority",
                when(col("overall_risk_level") == "CRITICAL", 1)
                .when(col("overall_risk_level") == "HIGH", 2)
                .when(col("overall_risk_level") == "MEDIUM", 3)
                .otherwise(4)
            )
        
        # Filter only at-risk students (risk_score > 20)
        self.at_risk_students = at_risk_students \
            .filter(col("risk_score") > 20) \
            .select(
                "student_id", "full_name", "faculty", "email", "semester",
                "cumulative_gpa", "gpa_trend", "avg_attendance_rate", "total_absences",
                "pass_rate", "risk_score", "overall_risk_level", "intervention_priority",
                "gpa_risk_flag", "attendance_risk_flag", "gpa_trend_risk_flag", "fail_rate_risk_flag"
            )
        
        logger.info(f"✓ At-risk analysis complete: {self.at_risk_students.count():,} students need intervention")
        
        # Show distribution by risk level
        logger.info("\nAt-risk students by level:")
        self.at_risk_students.groupBy("overall_risk_level") \
            .count() \
            .orderBy(col("count").desc()) \
            .show()
        
        # Show critical cases
        logger.info("\nCritical risk students (sample):")
        self.at_risk_students \
            .filter(col("overall_risk_level") == "CRITICAL") \
            .select("student_id", "full_name", "cumulative_gpa", "avg_attendance_rate", 
                   "risk_score", "intervention_priority") \
            .orderBy(col("risk_score").desc()) \
            .show(5, truncate=False)
    
    def create_pivot_tables(self):
        """
        Create pivot tables for cross-tabulation analysis
        Demonstrates: PIVOT operations
        """
        
        logger.info("=" * 70)
        logger.info("STEP 8: Create Pivot Tables")
        logger.info("=" * 70)
        
        # 1. Pivot: Students by faculty and performance tier
        logger.info("Creating student distribution pivot...")
        
        self.student_distribution_pivot = self.student_gpa \
            .groupBy("faculty", "semester") \
            .pivot("performance_tier", ["Excellent", "Very Good", "Good", "Average", "At Risk"]) \
            .count() \
            .fillna(0)
        
        logger.info("Student distribution by faculty:")
        self.student_distribution_pivot.show(truncate=False)
        
        # 2. Pivot: Course pass rates by semester
        logger.info("Creating course performance pivot...")
        
        self.course_performance_pivot = self.grades_clean \
            .join(self.classes.select("class_id", "course_id", "semester"), "class_id") \
            .join(broadcast(self.courses.select("course_id", "course_name")), "course_id") \
            .groupBy("course_name") \
            .pivot("semester") \
            .agg(
                round(
                    sum(when(col("passed") == True, 1).otherwise(0)) / count("*") * 100, 
                    2
                ).alias("pass_rate")
            )
        
        logger.info("Course pass rates by semester:")
        self.course_performance_pivot.show(10, truncate=False)
        
        # 3. Unpivot example (if needed for analysis)
        logger.info("Creating unpivoted semester scores...")
        
        # This would be used if we had pivoted data to unpivot
        # Example structure for reference
        
        logger.info("✓ Pivot tables created")
    
    def calculate_correlation_analysis(self):
        """
        Calculate correlations between different metrics
        Demonstrates: Statistical analysis, correlation computation
        """
        
        logger.info("=" * 70)
        logger.info("STEP 9: Correlation Analysis")
        logger.info("=" * 70)
        
        # Prepare data for correlation
        correlation_data = self.student_gpa \
            .join(
                self.attendance_metrics.select("student_id", "semester", "avg_attendance_rate"),
                ["student_id", "semester"],
                "left"
            ) \
            .select(
                "cumulative_gpa",
                "semester_gpa",
                "avg_attendance_rate",
                "pass_rate",
                "total_credits"
            ) \
            .na.drop()
        
        # Calculate correlations
        logger.info("Calculating correlation matrix...")
        
        metrics = ["cumulative_gpa", "avg_attendance_rate", "pass_rate"]
        
        for i, metric1 in enumerate(metrics):
            for metric2 in metrics[i+1:]:
                corr = correlation_data.stat.corr(metric1, metric2)
                logger.info(f"  Correlation {metric1} vs {metric2}: {corr:.4f}")
        
        logger.info("✓ Correlation analysis complete")
    
    def create_student_profile_view(self):
        """
        Create comprehensive student profile combining all metrics
        Demonstrates: Complex multi-way joins
        """
        
        logger.info("=" * 70)
        logger.info("STEP 10: Create Student Profile View")
        logger.info("=" * 70)
        
        # Combine all student metrics
        self.student_profile_view = self.students \
            .join(self.student_gpa, "student_id", "left") \
            .join(
                self.attendance_metrics.select(
                    "student_id", "semester", "avg_attendance_rate", 
                    "attendance_status", "total_absences"
                ),
                ["student_id", "semester"],
                "left"
            ) \
            .join(
                self.at_risk_students.select(
                    "student_id", "semester", "risk_score", "overall_risk_level"
                ),
                ["student_id", "semester"],
                "left"
            ) \
            .select(
                # Student info
                "student_id", "full_name", "dob", "faculty", "email",
                
                # Semester info
                "semester",
                
                # Academic metrics
                "semester_gpa", "cumulative_gpa", "cumulative_credits",
                "courses_taken", "courses_passed", "pass_rate",
                "performance_tier", "academic_standing", "gpa_trajectory",
                
                # Attendance metrics
                "avg_attendance_rate", "attendance_status", "total_absences",
                
                # Risk metrics
                coalesce(col("risk_score"), lit(0)).alias("risk_score"),
                coalesce(col("overall_risk_level"), lit("LOW")).alias("risk_level")
            ) \
            .withColumn("profile_generated_at", current_timestamp())
        
        logger.info(f"✓ Student profile view created: {self.student_profile_view.count():,} records")
        
        # Show sample profiles
        logger.info("\nSample student profiles:")
        self.student_profile_view \
            .select("student_id", "full_name", "semester", "cumulative_gpa", 
                   "performance_tier", "avg_attendance_rate", "risk_level") \
            .show(5, truncate=False)
    
    def write_batch_views(self):
        """
        Write all processed views to HDFS
        Demonstrates: Partitioning strategies, write modes
        """
        
        logger.info("=" * 70)
        logger.info("STEP 11: Write Batch Views to HDFS")
        logger.info("=" * 70)
        
        base_path = f"{self.hdfs_namenode}/views/batch/{self.processing_date}"
        
        # 1. Student GPA (partitioned by semester)
        logger.info("Writing student_gpa...")
        self.student_gpa.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/student_gpa")
        logger.info(f"  ✓ student_gpa written")
        
        # 2. Class rankings (partitioned by semester)
        logger.info("Writing class_rankings...")
        self.class_rankings.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/class_rankings")
        logger.info(f"  ✓ class_rankings written")
        
        # 3. Attendance metrics (partitioned by semester)
        logger.info("Writing attendance_metrics...")
        self.attendance_metrics.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/attendance_metrics")
        logger.info(f"  ✓ attendance_metrics written")
        
        # 4. Course statistics (partitioned by semester)
        logger.info("Writing course_statistics...")
        self.course_statistics.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/course_statistics")
        logger.info(f"  ✓ course_statistics written")
        
        # 5. Faculty performance (partitioned by semester)
        logger.info("Writing faculty_performance...")
        self.faculty_performance.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/faculty_performance")
        logger.info(f"  ✓ faculty_performance written")
        
        # 6. At-risk students (partitioned by semester and risk level)
        logger.info("Writing at_risk_students...")
        self.at_risk_students.write \
            .mode("overwrite") \
            .partitionBy("semester", "overall_risk_level") \
            .parquet(f"{base_path}/at_risk_students")
        logger.info(f"  ✓ at_risk_students written")
        
        # 7. Student profile view (partitioned by semester)
        logger.info("Writing student_profile_view...")
        self.student_profile_view.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{base_path}/student_profile_view")
        logger.info(f"  ✓ student_profile_view written")
        
        # 8. Pivot tables (no partitioning - small data)
        logger.info("Writing pivot tables...")
        self.student_distribution_pivot.write \
            .mode("overwrite") \
            .parquet(f"{base_path}/student_distribution_pivot")
        
        self.course_performance_pivot.write \
            .mode("overwrite") \
            .parquet(f"{base_path}/course_performance_pivot")
        logger.info(f"  ✓ pivot tables written")
        
        # 9. CUBE aggregation
        logger.info("Writing CUBE aggregation...")
        self.course_cube.write \
            .mode("overwrite") \
            .parquet(f"{base_path}/course_cube")
        logger.info(f"  ✓ course_cube written")
        
        logger.info("=" * 70)
        logger.info("✓ All batch views written to HDFS")
        logger.info("=" * 70)
    
    def create_summary_report(self):
        """
        Create executive summary report
        """
        
        logger.info("=" * 70)
        logger.info("BATCH PROCESSING SUMMARY REPORT")
        logger.info("=" * 70)
        
        # Overall statistics
        total_students = self.students.count()
        total_enrollments = self.enrollments.count()
        total_courses = self.courses.count()
        
        logger.info(f"\n1. Overall Metrics:")
        logger.info(f"   Total Students: {total_students:,}")
        logger.info(f"   Total Courses: {total_courses:,}")
        logger.info(f"   Total Enrollments: {total_enrollments:,}")
        
        # GPA distribution
        logger.info(f"\n2. GPA Distribution:")
        self.student_gpa \
            .groupBy("performance_tier") \
            .count() \
            .withColumn("percentage", round(col("count") / total_students * 100, 2)) \
            .orderBy(col("count").desc()) \
            .show(truncate=False)
        
        # At-risk students
        at_risk_count = self.at_risk_students.count()
        logger.info(f"\n3. At-Risk Students:")
        logger.info(f"   Total At-Risk: {at_risk_count:,} ({at_risk_count/total_students*100:.2f}%)")
        
        self.at_risk_students \
            .groupBy("overall_risk_level") \
            .count() \
            .orderBy(col("count").desc()) \
            .show(truncate=False)
        
        # Course difficulty
        logger.info(f"\n4. Course Difficulty Distribution:")
        self.course_statistics \
            .groupBy("difficulty_level") \
            .count() \
            .orderBy(col("count").desc()) \
            .show(truncate=False)
        
        # Faculty performance
        logger.info(f"\n5. Faculty Performance Summary:")
        self.faculty_performance \
            .groupBy("performance_category") \
            .count() \
            .orderBy(col("count").desc()) \
            .show(truncate=False)
        
        # Processing metadata
        logger.info(f"\n6. Processing Metadata:")
        logger.info(f"   Processing Date: {self.processing_date}")
        logger.info(f"   Spark Version: {self.spark.version}")
        logger.info(f"   Views Created: 10")
        logger.info(f"   Total Records Processed: {self.grades_clean.count():,}")
        
        logger.info("=" * 70)
    
    def run_batch_pipeline(self):
        """Execute complete batch processing pipeline"""
        
        logger.info("=" * 80)
        logger.info(" " * 20 + "BATCH PROCESSING PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # Load data
            self.load_raw_data()
            
            # Step 1: Clean data
            self.clean_and_deduplicate()
            
            # Step 2: Calculate GPA
            self.calculate_student_gpa()
            
            # Step 3: Calculate rankings
            self.calculate_class_rankings()
            
            # Step 4: Calculate attendance
            self.calculate_attendance_metrics()
            
            # Step 5: Calculate course statistics
            self.calculate_course_statistics()
            
            # Step 6: Calculate faculty performance
            self.calculate_faculty_performance()
            
            # Step 7: Identify at-risk students
            self.identify_at_risk_students()
            
            # Step 8: Create pivot tables
            self.create_pivot_tables()
            
            # Step 9: Correlation analysis
            self.calculate_correlation_analysis()
            
            # Step 10: Create student profiles
            self.create_student_profile_view()
            
            # Step 11: Write results
            self.write_batch_views()
            
            # Step 12: Summary report
            self.create_summary_report()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info(" " * 25 + "PIPELINE COMPLETE")
            logger.info("=" * 80)
            logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            return False
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution function"""
    
    import os
    
    # Get configuration from environment
    HDFS_NAMENODE = os.getenv(
        "HDFS_NAMENODE",
        "hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"
    )
    
    try:
        # Initialize processor
        processor = BatchProcessor(hdfs_namenode=HDFS_NAMENODE)
        
        # Run pipeline
        success = processor.run_batch_pipeline()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        processor.cleanup()


if __name__ == "__main__":
    main()