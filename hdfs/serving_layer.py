"""
Serving Layer - Export Batch Results to Serving Databases
Writes processed batch views to MongoDB and PostgreSQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ServingLayerExporter:
    """Exports batch processing results to serving layer databases"""
    
    def __init__(self, hdfs_uri="hdfs://namenode:9000",
                 mongo_uri="mongodb://admin:password@mongodb:27017/edu_analytics",
                 postgres_url="jdbc:postgresql://postgresql:5432/edu_analytics"):
        """
        Initialize serving layer exporter
        
        Args:
            hdfs_uri: HDFS namenode URI
            mongo_uri: MongoDB connection URI
            postgres_url: PostgreSQL JDBC URL
        """
        self.hdfs_uri = hdfs_uri
        self.mongo_uri = mongo_uri
        self.postgres_url = postgres_url
        
        # Initialize Spark with MongoDB and PostgreSQL connectors
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-ServingLayer") \
            .config("spark.mongodb.input.uri", mongo_uri) \
            .config("spark.mongodb.output.uri", mongo_uri) \
            .config("spark.jars.packages", 
                   "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
                   "org.postgresql:postgresql:42.6.0") \
            .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        
        logger.info("Serving layer exporter initialized")
    
    def load_batch_views(self):
        """Load all batch views from HDFS"""
        
        logger.info("Loading batch views from HDFS...")
        
        base_path = f"{self.hdfs_uri}/edu-analytics/views/batch"
        
        self.student_gpa = self.spark.read.parquet(f"{base_path}/student_gpa")
        self.class_rankings = self.spark.read.parquet(f"{base_path}/class_rankings")
        self.attendance_metrics = self.spark.read.parquet(f"{base_path}/attendance_metrics")
        self.course_statistics = self.spark.read.parquet(f"{base_path}/course_statistics")
        self.faculty_performance = self.spark.read.parquet(f"{base_path}/faculty_performance")
        self.student_profiles = self.spark.read.parquet(f"{base_path}/student_profiles")
        
        # Load ML predictions
        pred_path = f"{self.hdfs_uri}/edu-analytics/views/batch/ml_predictions"
        self.gpa_predictions = self.spark.read.parquet(f"{pred_path}/gpa_predictions")
        self.dropout_risk = self.spark.read.parquet(f"{pred_path}/dropout_risk")
        
        logger.info("✓ Batch views loaded")
    
    def export_to_mongodb(self):
        """
        Export operational data to MongoDB
        MongoDB is used for operational queries (current semester, real-time dashboards)
        """
        
        logger.info("Exporting to MongoDB...")
        
        # Get current semester (latest)
        current_semester = self.student_gpa \
            .select("semester").distinct() \
            .orderBy(col("semester").desc()) \
            .first()[0]
        
        logger.info(f"Current semester: {current_semester}")
        
        # 1. Current student profiles (latest semester only)
        current_profiles = self.student_profiles \
            .filter(col("semester") == current_semester) \
            .drop("ingestion_timestamp", "source_file", "processing_timestamp")
        
        current_profiles.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("database", "edu_analytics") \
            .option("collection", "current_student_profiles") \
            .save()
        
        logger.info(f"  ✓ current_student_profiles: {current_profiles.count()} docs")
        
        # 2. Current class rankings
        current_rankings = self.class_rankings \
            .filter(col("semester") == current_semester) \
            .select(
                "student_id", "class_id", "semester",
                "total_score", "class_rank", "performance_decile"
            )
        
        current_rankings.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("database", "edu_analytics") \
            .option("collection", "current_class_rankings") \
            .save()
        
        logger.info(f"  ✓ current_class_rankings: {current_rankings.count()} docs")
        
        # 3. At-risk students (current semester)
        at_risk_students = self.dropout_risk \
            .filter((col("semester") == current_semester) & (col("prediction") == 1)) \
            .join(self.student_profiles, ["student_id", "semester"]) \
            .select(
                "student_id", "full_name", "faculty", "semester",
                "cumulative_gpa", "avg_attendance_rate",
                col("risk_probability").alias("risk_score")
            ) \
            .orderBy(col("risk_score").desc())
        
        at_risk_students.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("database", "edu_analytics") \
            .option("collection", "at_risk_students") \
            .save()
        
        logger.info(f"  ✓ at_risk_students: {at_risk_students.count()} docs")
        
        # 4. Current course statistics
        current_courses = self.course_statistics \
            .filter(col("semester") == current_semester) \
            .select(
                "course_id", "course_name", "semester",
                "enrollment_count", "avg_score", "pass_rate",
                "difficulty_level"
            )
        
        current_courses.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("database", "edu_analytics") \
            .option("collection", "current_course_stats") \
            .save()
        
        logger.info(f"  ✓ current_course_stats: {current_courses.count()} docs")
        
        logger.info("✓ MongoDB export complete")
    
    def export_to_postgresql(self):
        """
        Export analytical data to PostgreSQL
        PostgreSQL is used for historical analytics and complex queries
        """
        
        logger.info("Exporting to PostgreSQL...")
        
        postgres_props = {
            "user": os.getenv("POSTGRES_USER", "admin"),
            "password": os.getenv("POSTGRES_PASSWORD", "password"),
            "driver": "org.postgresql.Driver"
        }
        
        # 1. Student GPA history (all semesters)
        self.student_gpa.select(
            "student_id", "semester",
            "semester_gpa", "cumulative_gpa",
            "total_credits", "cumulative_credits",
            "pass_rate", "performance_tier"
        ).write \
            .jdbc(self.postgres_url, "student_gpa_history",
                  mode="overwrite", properties=postgres_props)
        
        logger.info(f"  ✓ student_gpa_history: {self.student_gpa.count()} rows")
        
        # 2. Course statistics (all semesters)
        self.course_statistics.select(
            "course_id", "course_name", "semester",
            "enrollment_count", "avg_score", "stddev_score",
            "median_score", "pass_rate", "difficulty_level"
        ).write \
            .jdbc(self.postgres_url, "course_statistics",
                  mode="overwrite", properties=postgres_props)
        
        logger.info(f"  ✓ course_statistics: {self.course_statistics.count()} rows")
        
        # 3. Faculty performance (all semesters)
        self.faculty_performance.select(
            "teacher_id", "full_name", "semester",
            "classes_taught", "total_students",
            "avg_student_score", "pass_rate",
            "faculty_rank", "performance_category"
        ).write \
            .jdbc(self.postgres_url, "faculty_performance",
                  mode="overwrite", properties=postgres_props)
        
        logger.info(f"  ✓ faculty_performance: {self.faculty_performance.count()} rows")
        
        # 4. Attendance metrics (all semesters)
        self.attendance_metrics.select(
            "student_id", "semester",
            "avg_attendance_rate", "total_absences",
            "classes_enrolled", "attendance_status"
        ).write \
            .jdbc(self.postgres_url, "attendance_metrics",
                  mode="overwrite", properties=postgres_props)
        
        logger.info(f"  ✓ attendance_metrics: {self.attendance_metrics.count()} rows")
        
        # 5. GPA predictions
        self.gpa_predictions.select(
            "student_id", "semester",
            "target_gpa", "predicted_gpa", "prediction_error"
        ).write \
            .jdbc(self.postgres_url, "gpa_predictions",
                  mode="overwrite", properties=postgres_props)
        
        logger.info(f"  ✓ gpa_predictions: {self.gpa_predictions.count()} rows")
        
        # 6. Dropout risk predictions
        self.dropout_risk.select(
            "student_id", "semester",
            col("label").alias("actual_at_risk"),
            col("prediction").alias("predicted_at_risk"),
            "risk_probability"
        ).write \
            .jdbc(self.postgres_url, "dropout_risk_predictions",
                  mode="overwrite", properties=postgres_props)
        
        logger.info(f"  ✓ dropout_risk_predictions: {self.dropout_risk.count()} rows")
        
        logger.info("✓ PostgreSQL export complete")
    
    def create_aggregated_views(self):
        """Create pre-aggregated views for dashboards"""
        
        logger.info("Creating aggregated views...")
        
        postgres_props = {
            "user": os.getenv("POSTGRES_USER", "admin"),
            "password": os.getenv("POSTGRES_PASSWORD", "password"),
            "driver": "org.postgresql.Driver"
        }
        
        # 1. Semester summary statistics
        semester_summary = self.student_gpa \
            .groupBy("semester") \
            .agg(
                count("student_id").alias("total_students"),
                avg("semester_gpa").alias("avg_semester_gpa"),
                avg("cumulative_gpa").alias("avg_cumulative_gpa"),
                sum(when(col("performance_tier") == "Excellent", 1).otherwise(0)).alias("excellent_count"),
                sum(when(col("performance_tier") == "At Risk", 1).otherwise(0)).alias("at_risk_count")
            ) \
            .withColumn("excellent_pct", col("excellent_count") / col("total_students") * 100) \
            .withColumn("at_risk_pct", col("at_risk_count") / col("total_students") * 100)
        
        semester_summary.write \
            .jdbc(self.postgres_url, "semester_summary",
                  mode="overwrite", properties=postgres_props)
        
        logger.info(f"  ✓ semester_summary: {semester_summary.count()} rows")
        
        # 2. Faculty summary by semester
        faculty_summary = self.faculty_performance \
            .groupBy("semester") \
            .agg(
                count("teacher_id").alias("active_teachers"),
                avg("avg_student_score").alias("overall_avg_score"),
                avg("pass_rate").alias("overall_pass_rate")
            )
        
        faculty_summary.write \
            .jdbc(self.postgres_url, "faculty_summary",
                  mode="overwrite", properties=postgres_props)
        
        logger.info(f"  ✓ faculty_summary: {faculty_summary.count()} rows")
        
        logger.info("✓ Aggregated views created")
    
    def run_export_pipeline(self):
        """Execute complete serving layer export"""
        
        logger.info("=" * 70)
        logger.info("SERVING LAYER EXPORT - Starting")
        logger.info("=" * 70)
        
        # Load batch views
        self.load_batch_views()
        
        # Export to MongoDB (operational queries)
        logger.info("\n[1/3] Exporting to MongoDB")
        logger.info("-" * 70)
        self.export_to_mongodb()
        
        # Export to PostgreSQL (analytical queries)
        logger.info("\n[2/3] Exporting to PostgreSQL")
        logger.info("-" * 70)
        self.export_to_postgresql()
        
        # Create aggregated views
        logger.info("\n[3/3] Creating Aggregated Views")
        logger.info("-" * 70)
        self.create_aggregated_views()
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ SERVING LAYER EXPORT COMPLETE")
        logger.info("=" * 70)
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution"""
    
    HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")
    MONGO_URI = os.getenv("MONGO_URI", 
                          "mongodb://admin:password@mongodb:27017/edu_analytics")
    POSTGRES_URL = os.getenv("POSTGRES_URL",
                             "jdbc:postgresql://postgresql:5432/edu_analytics")
    
    try:
        exporter = ServingLayerExporter(
            hdfs_uri=HDFS_URI,
            mongo_uri=MONGO_URI,
            postgres_url=POSTGRES_URL
        )
        
        exporter.run_export_pipeline()
        
    except Exception as e:
        logger.error(f"Serving layer export failed: {str(e)}")
        raise
    finally:
        exporter.cleanup()


if __name__ == "__main__":
    main()