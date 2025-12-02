"""
ML Model Serving Module
Loads trained models and generates batch predictions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import PipelineModel
import logging
from datetime import datetime
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLModelServer:
    """
    Serves ML models for batch prediction
    
    Features:
    - Load trained models from HDFS
    - Generate predictions on new data
    - Export predictions to serving databases
    """
    
    def __init__(self, hdfs_uri="hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"):
        """Initialize model server"""
        
        self.hdfs_uri = hdfs_uri
        self.processing_date = datetime.now().strftime("%Y-%m-%d")
        
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-MLServing") \
            .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.instances", "3") \
            .getOrCreate()
        
        logger.info(f"ML Model Server initialized for {self.processing_date}")
    
    def load_gpa_model(self, model_date=None):
        """Load trained GPA prediction model"""
        
        if model_date is None:
            model_date = self.processing_date
        
        model_path = f"{self.hdfs_uri}/models/gpa_predictor/{model_date}"
        
        logger.info(f"Loading GPA model from {model_path}...")
        
        try:
            self.gpa_model = PipelineModel.load(model_path)
            logger.info("✓ GPA model loaded successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to load GPA model: {e}")
            return False
    
    def load_dropout_model(self, model_date=None):
        """Load trained dropout risk model"""
        
        if model_date is None:
            model_date = self.processing_date
        
        model_path = f"{self.hdfs_uri}/models/dropout_risk/{model_date}"
        
        logger.info(f"Loading dropout model from {model_path}...")
        
        try:
            self.dropout_model = PipelineModel.load(model_path)
            logger.info("✓ Dropout model loaded successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to load dropout model: {e}")
            return False
    
    def generate_gpa_predictions(self):
        """Generate GPA predictions for all students"""
        
        logger.info("Generating GPA predictions...")
        
        # Load features
        features_path = f"{self.hdfs_uri}/ml_features/{self.processing_date}/gpa_prediction"
        features_df = self.spark.read.parquet(features_path)
        
        # Generate predictions
        predictions = self.gpa_model.transform(features_df)
        
        # Select relevant columns
        predictions_clean = predictions.select(
            "student_id",
            "semester",
            "faculty",
            col("target_gpa").alias("actual_gpa"),
            col("predicted_gpa"),
            (col("predicted_gpa") - col("target_gpa")).alias("prediction_error"),
            abs(col("predicted_gpa") - col("target_gpa")).alias("abs_error")
        )
        
        logger.info(f"✓ Generated {predictions_clean.count():,} GPA predictions")
        
        # Show accuracy metrics
        predictions_clean.selectExpr(
            "avg(abs_error) as mae",
            "sqrt(avg(prediction_error * prediction_error)) as rmse"
        ).show()
        
        self.gpa_predictions = predictions_clean
        return predictions_clean
    
    def generate_dropout_predictions(self):
        """Generate dropout risk predictions"""
        
        logger.info("Generating dropout risk predictions...")
        
        # Load features
        features_path = f"{self.hdfs_uri}/ml_features/{self.processing_date}/dropout_risk"
        features_df = self.spark.read.parquet(features_path)
        
        # Generate predictions
        predictions = self.dropout_model.transform(features_df)
        
        # Extract probability of being at-risk (class 1)
        predictions_clean = predictions.select(
            "student_id",
            "semester",
            "faculty",
            col("label").alias("actual_at_risk"),
            col("prediction").alias("predicted_at_risk"),
            col("probability")[1].alias("risk_probability"),
            "cumulative_gpa",
            "avg_attendance_rate",
            "pass_rate"
        )
        
        # Add risk category
        predictions_clean = predictions_clean.withColumn(
            "risk_category",
            when(col("risk_probability") >= 0.8, "CRITICAL")
            .when(col("risk_probability") >= 0.6, "HIGH")
            .when(col("risk_probability") >= 0.4, "MEDIUM")
            .otherwise("LOW")
        )
        
        logger.info(f"✓ Generated {predictions_clean.count():,} dropout predictions")
        
        # Show risk distribution
        logger.info("\nRisk Distribution:")
        predictions_clean.groupBy("risk_category").count().orderBy("count", ascending=False).show()
        
        self.dropout_predictions = predictions_clean
        return predictions_clean
    
    def save_predictions_to_hdfs(self):
        """Save predictions to HDFS"""
        
        output_path = f"{self.hdfs_uri}/ml_predictions/{self.processing_date}"
        
        logger.info(f"Saving predictions to {output_path}...")
        
        # Save GPA predictions
        gpa_path = f"{output_path}/gpa_predictions"
        self.gpa_predictions.write \
            .mode("overwrite") \
            .partitionBy("semester") \
            .parquet(gpa_path)
        
        logger.info(f"✓ GPA predictions saved to {gpa_path}")
        
        # Save dropout predictions
        dropout_path = f"{output_path}/dropout_risk"
        self.dropout_predictions.write \
            .mode("overwrite") \
            .partitionBy("semester", "risk_category") \
            .parquet(dropout_path)
        
        logger.info(f"✓ Dropout predictions saved to {dropout_path}")
    
    def export_to_mongodb(self, mongo_uri=None):
        """Export predictions to MongoDB for serving"""
        
        if mongo_uri is None:
            mongo_uri = os.getenv(
                "MONGO_URI",
                "mongodb://admin:password@mongodb.bigdata.svc.cluster.local:27017/edu_analytics"
            )
        
        logger.info("Exporting predictions to MongoDB...")
        
        # Critical risk students only
        critical_students = self.dropout_predictions \
            .filter(col("risk_category").isin(["CRITICAL", "HIGH"])) \
            .select(
                "student_id",
                "semester",
                "faculty",
                "risk_probability",
                "risk_category",
                "cumulative_gpa",
                "avg_attendance_rate",
                "pass_rate"
            )
        
        critical_students.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("spark.mongodb.output.uri", mongo_uri) \
            .option("database", "edu_analytics") \
            .option("collection", "ml_at_risk_predictions") \
            .save()
        
        logger.info(f"✓ Exported {critical_students.count():,} at-risk predictions to MongoDB")
    
    def run_serving_pipeline(self):
        """Execute complete model serving pipeline"""
        
        logger.info("=" * 80)
        logger.info(" " * 25 + "ML MODEL SERVING")
        logger.info("=" * 80)
        
        try:
            # Load models
            logger.info("\n[1/5] Loading Models")
            logger.info("-" * 70)
            if not self.load_gpa_model():
                raise Exception("Failed to load GPA model")
            if not self.load_dropout_model():
                raise Exception("Failed to load dropout model")
            
            # Generate predictions
            logger.info("\n[2/5] Generating GPA Predictions")
            logger.info("-" * 70)
            self.generate_gpa_predictions()
            
            logger.info("\n[3/5] Generating Dropout Predictions")
            logger.info("-" * 70)
            self.generate_dropout_predictions()
            
            # Save to HDFS
            logger.info("\n[4/5] Saving to HDFS")
            logger.info("-" * 70)
            self.save_predictions_to_hdfs()
            
            # Export to MongoDB
            logger.info("\n[5/5] Exporting to MongoDB")
            logger.info("-" * 70)
            self.export_to_mongodb()
            
            logger.info("\n" + "=" * 80)
            logger.info(" " * 25 + "SERVING COMPLETE")
            logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            logger.error(f"Model serving failed: {e}", exc_info=True)
            return False
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution"""
    import sys
    
    HDFS_URI = os.getenv(
        "HDFS_NAMENODE",
        "hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"
    )
    
    try:
        server = MLModelServer(hdfs_uri=HDFS_URI)
        success = server.run_serving_pipeline()
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Model serving failed: {e}")
        sys.exit(1)
    finally:
        server.cleanup()


if __name__ == "__main__":
    main()