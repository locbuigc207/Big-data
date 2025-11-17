"""
Batch Layer - Machine Learning Pipeline
Trains predictive models for GPA prediction and dropout risk
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MLPipeline:
    """Machine Learning pipeline for educational analytics"""
    
    def __init__(self, hdfs_uri="hdfs://namenode:9000"):
        """Initialize ML pipeline"""
        
        self.hdfs_uri = hdfs_uri
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-MLPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .getOrCreate()
        
        logger.info("ML Pipeline initialized")
    
    def load_training_data(self):
        """Load processed data for ML training"""
        
        logger.info("Loading training data...")
        
        base_path = f"{self.hdfs_uri}/edu-analytics/views/batch"
        
        # Load student GPA history
        self.student_gpa = self.spark.read.parquet(f"{base_path}/student_gpa")
        
        # Load attendance metrics
        self.attendance = self.spark.read.parquet(f"{base_path}/attendance_metrics")
        
        # Load class rankings
        self.rankings = self.spark.read.parquet(f"{base_path}/class_rankings")
        
        logger.info("✓ Training data loaded")
    
    def prepare_gpa_prediction_features(self):
        """
        Prepare features for GPA prediction
        Predict final GPA based on midterm scores and behavior
        """
        
        logger.info("Preparing GPA prediction features...")
        
        # Join GPA with attendance
        features_df = self.student_gpa \
            .join(self.attendance, ["student_id", "semester"], "inner") \
            .select(
                "student_id",
                "semester",
                "semester_gpa",
                "total_credits",
                "pass_rate",
                "avg_attendance_rate",
                "total_absences",
                "classes_enrolled"
            )
        
        # Calculate previous semester GPA (for sequential prediction)
        from pyspark.sql.window import Window
        
        window_prev = Window.partitionBy("student_id") \
            .orderBy("semester") \
            .rowsBetween(-1, -1)
        
        features_df = features_df \
            .withColumn("prev_semester_gpa", lag("semester_gpa", 1).over(window_prev)) \
            .withColumn("prev_pass_rate", lag("pass_rate", 1).over(window_prev))
        
        # Remove first semester (no previous data)
        features_df = features_df.filter(col("prev_semester_gpa").isNotNull())
        
        # Target: current semester GPA
        # Features: previous GPA, attendance, credits, etc.
        self.gpa_features = features_df.select(
            "student_id",
            "semester",
            col("semester_gpa").alias("target_gpa"),
            "prev_semester_gpa",
            "prev_pass_rate",
            "total_credits",
            "avg_attendance_rate",
            "total_absences",
            "classes_enrolled"
        ).na.drop()
        
        logger.info(f"✓ GPA prediction features prepared: {self.gpa_features.count()} records")
        
        return self.gpa_features
    
    def train_gpa_prediction_model(self):
        """Train Random Forest model to predict GPA"""
        
        logger.info("Training GPA prediction model...")
        
        # Feature columns
        feature_cols = [
            "prev_semester_gpa",
            "prev_pass_rate",
            "total_credits",
            "avg_attendance_rate",
            "total_absences",
            "classes_enrolled"
        ]
        
        # Assemble features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Random Forest Regressor
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="target_gpa",
            predictionCol="predicted_gpa",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, rf])
        
        # Split data
        train_data, test_data = self.gpa_features.randomSplit([0.8, 0.2], seed=42)
        
        logger.info(f"Training set: {train_data.count()} | Test set: {test_data.count()}")
        
        # Train model
        self.gpa_model = pipeline.fit(train_data)
        
        # Evaluate
        predictions = self.gpa_model.transform(test_data)
        
        evaluator = RegressionEvaluator(
            labelCol="target_gpa",
            predictionCol="predicted_gpa",
            metricName="rmse"
        )
        
        rmse = evaluator.evaluate(predictions)
        
        evaluator.setMetricName("r2")
        r2 = evaluator.evaluate(predictions)
        
        evaluator.setMetricName("mae")
        mae = evaluator.evaluate(predictions)
        
        logger.info(f"✓ GPA Model Performance:")
        logger.info(f"  RMSE: {rmse:.4f}")
        logger.info(f"  R²: {r2:.4f}")
        logger.info(f"  MAE: {mae:.4f}")
        
        # Save model
        model_path = f"{self.hdfs_uri}/edu-analytics/models/gpa_predictor"
        self.gpa_model.write().overwrite().save(model_path)
        logger.info(f"✓ Model saved to {model_path}")
        
        # Feature importance
        rf_model = self.gpa_model.stages[-1]
        feature_importance = list(zip(feature_cols, rf_model.featureImportances.toArray()))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        logger.info("Feature Importance:")
        for feature, importance in feature_importance:
            logger.info(f"  {feature:25s}: {importance:.4f}")
        
        return self.gpa_model, {"rmse": rmse, "r2": r2, "mae": mae}
    
    def prepare_dropout_risk_features(self):
        """
        Prepare features for dropout risk classification
        Predict if student is at risk of dropping out
        """
        
        logger.info("Preparing dropout risk features...")
        
        # Define "at risk" criteria
        # At risk if: GPA < 2.0 OR attendance < 70% OR fail rate > 40%
        features_df = self.student_gpa \
            .join(self.attendance, ["student_id", "semester"], "inner") \
            .withColumn(
                "at_risk",
                when(
                    (col("cumulative_gpa") < 2.0) | 
                    (col("avg_attendance_rate") < 70) |
                    (col("pass_rate") < 60),
                    1
                ).otherwise(0)
            )
        
        # Select features
        self.dropout_features = features_df.select(
            "student_id",
            "semester",
            col("at_risk").alias("label"),
            "cumulative_gpa",
            "semester_gpa",
            "pass_rate",
            "avg_attendance_rate",
            "total_absences",
            "cumulative_credits",
            "classes_enrolled"
        ).na.drop()
        
        # Check class balance
        class_dist = self.dropout_features.groupBy("label").count()
        class_dist.show()
        
        logger.info(f"✓ Dropout features prepared: {self.dropout_features.count()} records")
        
        return self.dropout_features
    
    def train_dropout_risk_model(self):
        """Train classifier to predict dropout risk"""
        
        logger.info("Training dropout risk model...")
        
        feature_cols = [
            "cumulative_gpa",
            "semester_gpa",
            "pass_rate",
            "avg_attendance_rate",
            "total_absences",
            "cumulative_credits",
            "classes_enrolled"
        ]
        
        # Assemble features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Gradient Boosted Trees Classifier
        gbt = GBTClassifier(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            probabilityCol="probability",
            maxDepth=6,
            maxIter=100,
            seed=42
        )
        
        # Pipeline
        pipeline = Pipeline(stages=[assembler, scaler, gbt])
        
        # Split data (stratified by label if possible)
        train_data, test_data = self.dropout_features.randomSplit([0.8, 0.2], seed=42)
        
        logger.info(f"Training set: {train_data.count()} | Test set: {test_data.count()}")
        
        # Train
        self.dropout_model = pipeline.fit(train_data)
        
        # Evaluate
        predictions = self.dropout_model.transform(test_data)
        
        # Metrics
        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        
        auc = evaluator.evaluate(predictions)
        
        evaluator.setMetricName("areaUnderPR")
        auprc = evaluator.evaluate(predictions)
        
        # Confusion matrix
        cm = predictions.groupBy("label", "prediction").count().toPandas()
        
        logger.info(f"✓ Dropout Risk Model Performance:")
        logger.info(f"  AUC-ROC: {auc:.4f}")
        logger.info(f"  AUC-PR: {auprc:.4f}")
        logger.info("\nConfusion Matrix:")
        logger.info(cm.to_string())
        
        # Save model
        model_path = f"{self.hdfs_uri}/edu-analytics/models/dropout_risk"
        self.dropout_model.write().overwrite().save(model_path)
        logger.info(f"✓ Model saved to {model_path}")
        
        # Feature importance
        gbt_model = self.dropout_model.stages[-1]
        feature_importance = list(zip(feature_cols, gbt_model.featureImportances.toArray()))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        logger.info("Feature Importance:")
        for feature, importance in feature_importance:
            logger.info(f"  {feature:25s}: {importance:.4f}")
        
        return self.dropout_model, {"auc": auc, "auprc": auprc}
    
    def generate_predictions(self):
        """Generate predictions for all students"""
        
        logger.info("Generating predictions for all students...")
        
        # GPA predictions
        gpa_predictions = self.gpa_model.transform(self.gpa_features) \
            .select(
                "student_id",
                "semester",
                "target_gpa",
                "predicted_gpa",
                abs(col("target_gpa") - col("predicted_gpa")).alias("prediction_error")
            )
        
        # Dropout risk predictions
        dropout_predictions = self.dropout_model.transform(self.dropout_features) \
            .select(
                "student_id",
                "semester",
                "label",
                "prediction",
                "probability"
            ) \
            .withColumn("risk_probability", col("probability").getItem(1))
        
        # Save predictions
        pred_path = f"{self.hdfs_uri}/edu-analytics/views/batch/ml_predictions"
        
        gpa_predictions.write.mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{pred_path}/gpa_predictions")
        
        dropout_predictions.write.mode("overwrite") \
            .partitionBy("semester") \
            .parquet(f"{pred_path}/dropout_risk")
        
        logger.info("✓ Predictions generated and saved")
        
        return gpa_predictions, dropout_predictions
    
    def run_ml_pipeline(self):
        """Execute complete ML pipeline"""
        
        logger.info("=" * 70)
        logger.info("MACHINE LEARNING PIPELINE - Starting")
        logger.info("=" * 70)
        
        # Load data
        self.load_training_data()
        
        # GPA Prediction
        logger.info("\n[1/2] GPA PREDICTION MODEL")
        logger.info("-" * 70)
        self.prepare_gpa_prediction_features()
        gpa_model, gpa_metrics = self.train_gpa_prediction_model()
        
        # Dropout Risk
        logger.info("\n[2/2] DROPOUT RISK MODEL")
        logger.info("-" * 70)
        self.prepare_dropout_risk_features()
        dropout_model, dropout_metrics = self.train_dropout_risk_model()
        
        # Generate predictions
        logger.info("\nGenerating Predictions")
        logger.info("-" * 70)
        gpa_pred, dropout_pred = self.generate_predictions()
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ ML PIPELINE COMPLETE")
        logger.info("=" * 70)
        
        return {
            "gpa_model": gpa_model,
            "gpa_metrics": gpa_metrics,
            "dropout_model": dropout_model,
            "dropout_metrics": dropout_metrics
        }
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution"""
    
    import os
    HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")
    
    try:
        ml_pipeline = MLPipeline(hdfs_uri=HDFS_URI)
        results = ml_pipeline.run_ml_pipeline()
        
        logger.info("\nModel Performance Summary:")
        logger.info(f"GPA Prediction - RMSE: {results['gpa_metrics']['rmse']:.4f}, R²: {results['gpa_metrics']['r2']:.4f}")
        logger.info(f"Dropout Risk - AUC: {results['dropout_metrics']['auc']:.4f}")
        
    except Exception as e:
        logger.error(f"ML pipeline failed: {str(e)}")
        raise
    finally:
        ml_pipeline.cleanup()


if __name__ == "__main__":
    main()