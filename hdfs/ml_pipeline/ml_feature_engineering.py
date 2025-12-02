"""
ML Feature Engineering Module
Extracts and transforms features from batch views for ML models
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.feature import Bucketizer, QuantileDiscretizer
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLFeatureEngineer:
    """
    Feature engineering for educational analytics ML models
    
    Features extracted:
    - Academic performance (GPA, pass rate, score trends)
    - Attendance behavior (rate, consistency, absences)
    - Engagement metrics (activities, video views, submissions)
    - Historical patterns (semester-over-semester changes)
    """
    
    def __init__(self, hdfs_uri="hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"):
        """
        Initialize feature engineer
        
        Args:
            hdfs_uri: HDFS namenode URI
        """
        self.hdfs_uri = hdfs_uri
        self.processing_date = datetime.now().strftime("%Y-%m-%d")
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-MLFeatureEngineering") \
            .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.instances", "3") \
            .getOrCreate()
        
        logger.info(f"Feature Engineer initialized for {self.processing_date}")
    
    def load_batch_views(self):
        """Load processed batch views from HDFS"""
        
        logger.info("Loading batch views from HDFS...")
        
        base_path = f"{self.hdfs_uri}/views/batch/{self.processing_date}"
        
        try:
            # Load main views
            self.student_gpa = self.spark.read.parquet(f"{base_path}/student_gpa")
            self.attendance_metrics = self.spark.read.parquet(f"{base_path}/attendance_metrics")
            self.class_rankings = self.spark.read.parquet(f"{base_path}/class_rankings")
            
            # Cache frequently accessed data
            self.student_gpa.cache()
            self.attendance_metrics.cache()
            
            logger.info(f"✓ Loaded batch views:")
            logger.info(f"  - Student GPA: {self.student_gpa.count():,} records")
            logger.info(f"  - Attendance: {self.attendance_metrics.count():,} records")
            logger.info(f"  - Rankings: {self.class_rankings.count():,} records")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load batch views: {e}")
            return False
    
    def create_gpa_prediction_features(self):
        """
        Create features for GPA prediction model
        
        Task: Predict next semester GPA based on:
        - Previous semester performance
        - Attendance patterns
        - Engagement metrics
        - Historical trends
        
        Returns:
            DataFrame with features and target variable
        """
        
        logger.info("=" * 70)
        logger.info("Creating GPA Prediction Features")
        logger.info("=" * 70)
        
        # Join GPA with attendance metrics
        logger.info("Joining GPA and attendance data...")
        
        features_df = self.student_gpa \
            .join(
                self.attendance_metrics,
                ["student_id", "semester"],
                "inner"
            ) \
            .select(
                # Identifiers
                "student_id",
                "semester",
                "faculty",
                
                # Target variable (what we want to predict)
                col("semester_gpa").alias("target_gpa"),
                
                # Academic features
                "total_credits",
                "courses_taken",
                "courses_passed",
                "courses_failed",
                "pass_rate",
                "avg_score",
                "min_score",
                "max_score",
                
                # Attendance features
                "avg_attendance_rate",
                "min_attendance_rate",
                "total_absences",
                "total_excused",
                "classes_enrolled",
                "attendance_consistency"
            )
        
        # Create window for previous semester features
        logger.info("Creating lag features (previous semester)...")
        
        window_prev = Window \
            .partitionBy("student_id") \
            .orderBy("semester") \
            .rowsBetween(-1, -1)
        
        # Add previous semester features
        features_df = features_df \
            .withColumn("prev_semester_gpa", lag("target_gpa", 1).over(window_prev)) \
            .withColumn("prev_pass_rate", lag("pass_rate", 1).over(window_prev)) \
            .withColumn("prev_attendance_rate", lag("avg_attendance_rate", 1).over(window_prev)) \
            .withColumn("prev_total_credits", lag("total_credits", 1).over(window_prev)) \
            .withColumn("prev_avg_score", lag("avg_score", 1).over(window_prev))
        
        # Calculate trend features (change from previous semester)
        logger.info("Calculating trend features...")
        
        features_df = features_df \
            .withColumn(
                "gpa_trend",
                when(col("prev_semester_gpa").isNotNull(),
                     col("target_gpa") - col("prev_semester_gpa"))
                .otherwise(0.0)
            ) \
            .withColumn(
                "attendance_trend",
                when(col("prev_attendance_rate").isNotNull(),
                     col("avg_attendance_rate") - col("prev_attendance_rate"))
                .otherwise(0.0)
            ) \
            .withColumn(
                "score_trend",
                when(col("prev_avg_score").isNotNull(),
                     col("avg_score") - col("prev_avg_score"))
                .otherwise(0.0)
            )
        
        # Create derived features
        logger.info("Creating derived features...")
        
        features_df = features_df \
            .withColumn(
                "course_completion_rate",
                when(col("courses_taken") > 0,
                     col("courses_passed") / col("courses_taken"))
                .otherwise(0.0)
            ) \
            .withColumn(
                "absence_rate",
                when(col("classes_enrolled") > 0,
                     col("total_absences") / col("classes_enrolled"))
                .otherwise(0.0)
            ) \
            .withColumn(
                "score_range",
                col("max_score") - col("min_score")
            ) \
            .withColumn(
                "is_improving",
                when(col("gpa_trend") > 0.1, 1).otherwise(0)
            ) \
            .withColumn(
                "is_declining",
                when(col("gpa_trend") < -0.1, 1).otherwise(0)
            )
        
        # Filter: Remove first semester (no previous data)
        features_df = features_df.filter(col("prev_semester_gpa").isNotNull())
        
        # Drop rows with null target
        features_df = features_df.filter(col("target_gpa").isNotNull())
        
        # Final feature selection
        self.gpa_features = features_df.select(
            # Identifiers
            "student_id",
            "semester",
            "faculty",
            
            # Target
            "target_gpa",
            
            # Previous semester features
            "prev_semester_gpa",
            "prev_pass_rate",
            "prev_attendance_rate",
            "prev_total_credits",
            "prev_avg_score",
            
            # Current semester features
            "total_credits",
            "courses_taken",
            "pass_rate",
            "avg_attendance_rate",
            "total_absences",
            "attendance_consistency",
            
            # Derived features
            "gpa_trend",
            "attendance_trend",
            "score_trend",
            "course_completion_rate",
            "absence_rate",
            "score_range",
            "is_improving",
            "is_declining"
        )
        
        # Show feature statistics
        logger.info("\nFeature Statistics:")
        self.gpa_features.describe([
            "target_gpa",
            "prev_semester_gpa",
            "prev_attendance_rate",
            "gpa_trend"
        ]).show()
        
        logger.info(f"✓ GPA prediction features created: {self.gpa_features.count():,} records")
        
        return self.gpa_features
    
    def create_dropout_risk_features(self):
        """
        Create features for dropout risk classification
        
        Task: Classify students as at-risk or not based on:
        - Cumulative GPA
        - Attendance patterns
        - Academic trajectory
        - Engagement levels
        
        Returns:
            DataFrame with features and binary target (0=not at risk, 1=at risk)
        """
        
        logger.info("=" * 70)
        logger.info("Creating Dropout Risk Features")
        logger.info("=" * 70)
        
        # Join all relevant data
        logger.info("Joining multiple data sources...")
        
        features_df = self.student_gpa \
            .join(
                self.attendance_metrics,
                ["student_id", "semester"],
                "inner"
            ) \
            .select(
                # Identifiers
                "student_id",
                "semester",
                "faculty",
                
                # Academic performance
                "semester_gpa",
                "cumulative_gpa",
                "cumulative_credits",
                "pass_rate",
                "courses_failed",
                "performance_tier",
                "gpa_trajectory",
                
                # Attendance
                "avg_attendance_rate",
                "min_attendance_rate",
                "total_absences",
                "attendance_status",
                
                # Scores
                "avg_score",
                "min_score",
                "stddev_score"
            )
        
        # Define at-risk criteria
        logger.info("Defining at-risk criteria...")
        
        features_df = features_df \
            .withColumn(
                "at_risk_label",
                when(
                    # Multiple risk factors
                    (col("cumulative_gpa") < 2.0) |
                    (col("avg_attendance_rate") < 70) |
                    (col("pass_rate") < 60) |
                    (col("courses_failed") >= 2),
                    1  # At risk
                ).otherwise(0)  # Not at risk
            )
        
        # Create risk score components
        logger.info("Creating risk score components...")
        
        features_df = features_df \
            .withColumn(
                "gpa_risk_score",
                when(col("cumulative_gpa") >= 3.0, 0)
                .when(col("cumulative_gpa") >= 2.5, 20)
                .when(col("cumulative_gpa") >= 2.0, 40)
                .when(col("cumulative_gpa") >= 1.5, 60)
                .otherwise(80)
            ) \
            .withColumn(
                "attendance_risk_score",
                when(col("avg_attendance_rate") >= 85, 0)
                .when(col("avg_attendance_rate") >= 75, 20)
                .when(col("avg_attendance_rate") >= 60, 40)
                .otherwise(60)
            ) \
            .withColumn(
                "performance_risk_score",
                when(col("pass_rate") >= 80, 0)
                .when(col("pass_rate") >= 60, 20)
                .when(col("pass_rate") >= 40, 40)
                .otherwise(60)
            ) \
            .withColumn(
                "overall_risk_score",
                col("gpa_risk_score") + 
                col("attendance_risk_score") + 
                col("performance_risk_score")
            )
        
        # Create categorical features (for StringIndexer)
        features_df = features_df \
            .withColumn(
                "gpa_category",
                when(col("cumulative_gpa") >= 3.5, "Excellent")
                .when(col("cumulative_gpa") >= 3.0, "Good")
                .when(col("cumulative_gpa") >= 2.5, "Average")
                .when(col("cumulative_gpa") >= 2.0, "Below Average")
                .otherwise("At Risk")
            ) \
            .withColumn(
                "attendance_category",
                when(col("avg_attendance_rate") >= 90, "Excellent")
                .when(col("avg_attendance_rate") >= 80, "Good")
                .when(col("avg_attendance_rate") >= 70, "Average")
                .otherwise("Poor")
            )
        
        # Final feature selection
        self.dropout_features = features_df.select(
            # Identifiers
            "student_id",
            "semester",
            "faculty",
            
            # Target
            col("at_risk_label").alias("label"),
            
            # Numerical features
            "cumulative_gpa",
            "semester_gpa",
            "cumulative_credits",
            "pass_rate",
            "courses_failed",
            "avg_attendance_rate",
            "min_attendance_rate",
            "total_absences",
            "avg_score",
            "min_score",
            "stddev_score",
            
            # Risk scores
            "gpa_risk_score",
            "attendance_risk_score",
            "performance_risk_score",
            "overall_risk_score",
            
            # Categorical features
            "gpa_category",
            "attendance_category",
            "performance_tier",
            "gpa_trajectory"
        )
        
        # Check class balance
        logger.info("\nClass Distribution:")
        class_dist = self.dropout_features \
            .groupBy("label") \
            .count() \
            .withColumn("percentage", round(col("count") / self.dropout_features.count() * 100, 2))
        
        class_dist.show()
        
        # Show feature statistics by class
        logger.info("\nFeature Statistics by Class:")
        self.dropout_features \
            .groupBy("label") \
            .agg(
                avg("cumulative_gpa").alias("avg_gpa"),
                avg("avg_attendance_rate").alias("avg_attendance"),
                avg("pass_rate").alias("avg_pass_rate"),
                avg("overall_risk_score").alias("avg_risk_score")
            ) \
            .show()
        
        logger.info(f"✓ Dropout risk features created: {self.dropout_features.count():,} records")
        
        return self.dropout_features
    
    def save_features(self, output_path=None):
        """
        Save engineered features to HDFS
        
        Args:
            output_path: Optional custom output path
        """
        
        if output_path is None:
            output_path = f"{self.hdfs_uri}/ml_features/{self.processing_date}"
        
        logger.info(f"Saving features to {output_path}...")
        
        # Save GPA prediction features
        gpa_path = f"{output_path}/gpa_prediction"
        self.gpa_features.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(gpa_path)
        
        logger.info(f"✓ GPA features saved to {gpa_path}")
        
        # Save dropout risk features
        dropout_path = f"{output_path}/dropout_risk"
        self.dropout_features.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(dropout_path)
        
        logger.info(f"✓ Dropout features saved to {dropout_path}")
        
        # Save feature metadata
        metadata = {
            "processing_date": self.processing_date,
            "gpa_feature_count": self.gpa_features.count(),
            "dropout_feature_count": self.dropout_features.count(),
            "gpa_feature_names": self.gpa_features.columns,
            "dropout_feature_names": self.dropout_features.columns
        }
        
        metadata_df = self.spark.createDataFrame([metadata])
        metadata_df.write \
            .mode("overwrite") \
            .json(f"{output_path}/metadata")
        
        logger.info("✓ Feature metadata saved")
    
    def run_feature_engineering(self):
        """Execute complete feature engineering pipeline"""
        
        logger.info("=" * 80)
        logger.info(" " * 25 + "FEATURE ENGINEERING PIPELINE")
        logger.info("=" * 80)
        
        try:
            # Load data
            if not self.load_batch_views():
                raise Exception("Failed to load batch views")
            
            # Create GPA prediction features
            logger.info("\n[1/2] GPA Prediction Features")
            logger.info("-" * 70)
            self.create_gpa_prediction_features()
            
            # Create dropout risk features
            logger.info("\n[2/2] Dropout Risk Features")
            logger.info("-" * 70)
            self.create_dropout_risk_features()
            
            # Save features
            logger.info("\nSaving Features")
            logger.info("-" * 70)
            self.save_features()
            
            logger.info("\n" + "=" * 80)
            logger.info(" " * 25 + "FEATURE ENGINEERING COMPLETE")
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"Feature engineering failed: {e}", exc_info=True)
            return False
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution"""
    import os
    import sys
    
    HDFS_URI = os.getenv(
        "HDFS_NAMENODE",
        "hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"
    )
    
    try:
        engineer = MLFeatureEngineer(hdfs_uri=HDFS_URI)
        success = engineer.run_feature_engineering()
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Feature engineering failed: {e}")
        sys.exit(1)
    finally:
        engineer.cleanup()


if __name__ == "__main__":
    main()