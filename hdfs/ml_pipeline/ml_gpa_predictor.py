"""
GPA Prediction Model Training Module
Uses Spark MLlib to train regression models for predicting student GPA
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, TrainValidationSplit
import logging
from datetime import datetime
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GPAPredictorTrainer:
    """
    Trains regression models to predict next semester GPA
    
    Models:
    - Random Forest Regressor (primary)
    - Gradient Boosted Trees (alternative)
    - Linear Regression (baseline)
    
    Evaluation Metrics:
    - RMSE (Root Mean Squared Error)
    - MAE (Mean Absolute Error)
    - R² (R-squared)
    """
    
    def __init__(self, hdfs_uri="hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"):
        """
        Initialize GPA predictor trainer
        
        Args:
            hdfs_uri: HDFS namenode URI
        """
        self.hdfs_uri = hdfs_uri
        self.processing_date = datetime.now().strftime("%Y-%m-%d")
        
        # Initialize Spark with optimizations for ML
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-GPAPredictor") \
            .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .config("spark.executor.instances", "4") \
            .config("spark.executor.cores", "2") \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "20") \
            .getOrCreate()
        
        logger.info(f"GPA Predictor initialized for {self.processing_date}")
        
        # Feature columns (numerical features)
        self.feature_cols = [
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
        ]
        
        self.target_col = "target_gpa"
    
    def load_features(self):
        """Load engineered features from HDFS"""
        
        logger.info("Loading GPA prediction features from HDFS...")
        
        features_path = f"{self.hdfs_uri}/ml_features/{self.processing_date}/gpa_prediction"
        
        try:
            self.features_df = self.spark.read.parquet(features_path)
            
            # Remove any null values in features or target
            self.features_df = self.features_df.na.drop(
                subset=self.feature_cols + [self.target_col]
            )
            
            logger.info(f"✓ Loaded {self.features_df.count():,} training examples")
            
            # Show basic statistics
            logger.info("\nTarget Variable Statistics:")
            self.features_df.select(self.target_col).describe().show()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load features: {e}")
            return False
    
    def prepare_data(self, test_size=0.2, validation_size=0.1):
        """
        Prepare data for training
        
        Args:
            test_size: Proportion of data for testing
            validation_size: Proportion of training data for validation
            
        Returns:
            train_df, validation_df, test_df
        """
        
        logger.info("=" * 70)
        logger.info("Preparing Training Data")
        logger.info("=" * 70)
        
        # Assemble features into vector
        logger.info("Assembling feature vectors...")
        
        assembler = VectorAssembler(
            inputCols=self.feature_cols,
            outputCol="features_raw",
            handleInvalid="skip"  # Skip rows with invalid values
        )
        
        assembled_df = assembler.transform(self.features_df)
        
        # Scale features for better model performance
        logger.info("Scaling features...")
        
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        scaler_model = scaler.fit(assembled_df)
        scaled_df = scaler_model.transform(assembled_df)
        
        # Save scaler for later use in predictions
        self.scaler_model = scaler_model
        self.assembler = assembler
        
        # Split data: train/test
        logger.info(f"Splitting data: {1-test_size:.0%} train, {test_size:.0%} test...")
        
        train_df, test_df = scaled_df.randomSplit(
            [1 - test_size, test_size],
            seed=42
        )
        
        # Further split train into train/validation
        logger.info(f"Creating validation set: {validation_size:.0%} of training data...")
        
        train_df, validation_df = train_df.randomSplit(
            [1 - validation_size, validation_size],
            seed=42
        )
        
        # Cache datasets
        train_df.cache()
        validation_df.cache()
        test_df.cache()
        
        logger.info(f"\nDataset Sizes:")
        logger.info(f"  Training:   {train_df.count():,} examples")
        logger.info(f"  Validation: {validation_df.count():,} examples")
        logger.info(f"  Test:       {test_df.count():,} examples")
        
        self.train_df = train_df
        self.validation_df = validation_df
        self.test_df = test_df
        
        return train_df, validation_df, test_df
    
    def train_random_forest(self, use_cross_validation=False):
        """
        Train Random Forest regression model
        
        Args:
            use_cross_validation: Whether to use cross-validation for hyperparameter tuning
            
        Returns:
            Trained model and evaluation metrics
        """
        
        logger.info("=" * 70)
        logger.info("Training Random Forest Regressor")
        logger.info("=" * 70)
        
        # Initialize Random Forest
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol=self.target_col,
            predictionCol="predicted_gpa",
            seed=42
        )
        
        if use_cross_validation:
            logger.info("Using Cross-Validation for hyperparameter tuning...")
            
            # Parameter grid
            param_grid = ParamGridBuilder() \
                .addGrid(rf.numTrees, [50, 100, 150]) \
                .addGrid(rf.maxDepth, [5, 10, 15]) \
                .addGrid(rf.minInstancesPerNode, [1, 5]) \
                .build()
            
            # Evaluator
            evaluator = RegressionEvaluator(
                labelCol=self.target_col,
                predictionCol="predicted_gpa",
                metricName="rmse"
            )
            
            # Cross-validator
            cv = CrossValidator(
                estimator=rf,
                estimatorParamMaps=param_grid,
                evaluator=evaluator,
                numFolds=5,
                parallelism=4,
                seed=42
            )
            
            logger.info("Starting cross-validation (this may take a while)...")
            cv_model = cv.fit(self.train_df)
            
            # Get best model
            best_model = cv_model.bestModel
            
            logger.info("\nBest Hyperparameters:")
            logger.info(f"  Number of Trees: {best_model.getNumTrees}")
            logger.info(f"  Max Depth: {best_model.getMaxDepth()}")
            logger.info(f"  Min Instances Per Node: {best_model.getMinInstancesPerNode()}")
            
            self.rf_model = cv_model
            
        else:
            logger.info("Training with default hyperparameters...")
            
            # Set reasonable defaults
            rf.setNumTrees(100)
            rf.setMaxDepth(10)
            rf.setMinInstancesPerNode(5)
            rf.setMaxBins(32)
            rf.setSubsamplingRate(0.8)
            
            logger.info(f"Hyperparameters:")
            logger.info(f"  Number of Trees: {rf.getNumTrees()}")
            logger.info(f"  Max Depth: {rf.getMaxDepth()}")
            logger.info(f"  Min Instances Per Node: {rf.getMinInstancesPerNode()}")
            logger.info(f"  Subsampling Rate: {rf.getSubsamplingRate()}")
            
            # Train
            self.rf_model = rf.fit(self.train_df)
        
        # Evaluate on validation set
        logger.info("\nEvaluating on validation set...")
        val_predictions = self.rf_model.transform(self.validation_df)
        
        val_metrics = self._evaluate_predictions(
            val_predictions,
            self.target_col,
            "predicted_gpa"
        )
        
        logger.info("\nValidation Metrics:")
        logger.info(f"  RMSE: {val_metrics['rmse']:.4f}")
        logger.info(f"  MAE:  {val_metrics['mae']:.4f}")
        logger.info(f"  R²:   {val_metrics['r2']:.4f}")
        
        # Feature importance
        if not use_cross_validation:
            self._show_feature_importance(self.rf_model, self.feature_cols)
        
        return self.rf_model, val_metrics
    
    def train_gbt(self):
        """
        Train Gradient Boosted Trees regression model
        
        Returns:
            Trained model and evaluation metrics
        """
        
        logger.info("=" * 70)
        logger.info("Training Gradient Boosted Trees Regressor")
        logger.info("=" * 70)
        
        # Initialize GBT
        gbt = GBTRegressor(
            featuresCol="features",
            labelCol=self.target_col,
            predictionCol="predicted_gpa",
            maxIter=100,
            maxDepth=5,
            stepSize=0.1,
            seed=42
        )
        
        logger.info(f"Hyperparameters:")
        logger.info(f"  Max Iterations: {gbt.getMaxIter()}")
        logger.info(f"  Max Depth: {gbt.getMaxDepth()}")
        logger.info(f"  Step Size: {gbt.getStepSize()}")
        
        # Train
        logger.info("Training GBT model...")
        self.gbt_model = gbt.fit(self.train_df)
        
        # Evaluate on validation set
        logger.info("\nEvaluating on validation set...")
        val_predictions = self.gbt_model.transform(self.validation_df)
        
        val_metrics = self._evaluate_predictions(
            val_predictions,
            self.target_col,
            "predicted_gpa"
        )
        
        logger.info("\nValidation Metrics:")
        logger.info(f"  RMSE: {val_metrics['rmse']:.4f}")
        logger.info(f"  MAE:  {val_metrics['mae']:.4f}")
        logger.info(f"  R²:   {val_metrics['r2']:.4f}")
        
        # Feature importance
        self._show_feature_importance(self.gbt_model, self.feature_cols)
        
        return self.gbt_model, val_metrics
    
    def train_linear_regression(self):
        """
        Train Linear Regression model (baseline)
        
        Returns:
            Trained model and evaluation metrics
        """
        
        logger.info("=" * 70)
        logger.info("Training Linear Regression (Baseline)")
        logger.info("=" * 70)
        
        # Initialize Linear Regression
        lr = LinearRegression(
            featuresCol="features",
            labelCol=self.target_col,
            predictionCol="predicted_gpa",
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.5
        )
        
        logger.info(f"Hyperparameters:")
        logger.info(f"  Max Iterations: {lr.getMaxIter()}")
        logger.info(f"  Regularization: {lr.getRegParam()}")
        logger.info(f"  ElasticNet Mix: {lr.getElasticNetParam()}")
        
        # Train
        logger.info("Training Linear Regression model...")
        self.lr_model = lr.fit(self.train_df)
        
        # Evaluate on validation set
        logger.info("\nEvaluating on validation set...")
        val_predictions = self.lr_model.transform(self.validation_df)
        
        val_metrics = self._evaluate_predictions(
            val_predictions,
            self.target_col,
            "predicted_gpa"
        )
        
        logger.info("\nValidation Metrics:")
        logger.info(f"  RMSE: {val_metrics['rmse']:.4f}")
        logger.info(f"  MAE:  {val_metrics['mae']:.4f}")
        logger.info(f"  R²:   {val_metrics['r2']:.4f}")
        
        # Show coefficients
        logger.info("\nModel Coefficients (top 10 by magnitude):")
        coefficients = list(zip(self.feature_cols, self.lr_model.coefficients.toArray()))
        coefficients.sort(key=lambda x: abs(x[1]), reverse=True)
        
        for i, (feature, coef) in enumerate(coefficients[:10], 1):
            logger.info(f"  {i:2d}. {feature:30s}: {coef:+.4f}")
        
        return self.lr_model, val_metrics
    
    def _evaluate_predictions(self, predictions_df, label_col, prediction_col):
        """
        Evaluate regression predictions
        
        Args:
            predictions_df: DataFrame with predictions
            label_col: Actual values column
            prediction_col: Predicted values column
            
        Returns:
            Dictionary with evaluation metrics
        """
        
        evaluator_rmse = RegressionEvaluator(
            labelCol=label_col,
            predictionCol=prediction_col,
            metricName="rmse"
        )
        
        evaluator_mae = RegressionEvaluator(
            labelCol=label_col,
            predictionCol=prediction_col,
            metricName="mae"
        )
        
        evaluator_r2 = RegressionEvaluator(
            labelCol=label_col,
            predictionCol=prediction_col,
            metricName="r2"
        )
        
        rmse = evaluator_rmse.evaluate(predictions_df)
        mae = evaluator_mae.evaluate(predictions_df)
        r2 = evaluator_r2.evaluate(predictions_df)
        
        return {
            "rmse": rmse,
            "mae": mae,
            "r2": r2
        }
    
    def _show_feature_importance(self, model, feature_names):
        """Display feature importance from tree-based models"""
        
        try:
            if hasattr(model, 'featureImportances'):
                importances = model.featureImportances.toArray()
            elif hasattr(model, 'bestModel') and hasattr(model.bestModel, 'featureImportances'):
                importances = model.bestModel.featureImportances.toArray()
            else:
                logger.warning("Model does not have feature importances")
                return
            
            feature_importance = list(zip(feature_names, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)
            
            logger.info("\nFeature Importance (top 10):")
            for i, (feature, importance) in enumerate(feature_importance[:10], 1):
                logger.info(f"  {i:2d}. {feature:30s}: {importance:.4f}")
                
        except Exception as e:
            logger.warning(f"Could not extract feature importance: {e}")
    
    def evaluate_on_test_set(self, model, model_name="Model"):
        """
        Final evaluation on held-out test set
        
        Args:
            model: Trained model
            model_name: Name for logging
            
        Returns:
            Test metrics and predictions
        """
        
        logger.info("=" * 70)
        logger.info(f"Final Evaluation: {model_name} on Test Set")
        logger.info("=" * 70)
        
        # Make predictions
        test_predictions = model.transform(self.test_df)
        
        # Evaluate
        test_metrics = self._evaluate_predictions(
            test_predictions,
            self.target_col,
            "predicted_gpa"
        )
        
        logger.info(f"\nTest Set Metrics:")
        logger.info(f"  RMSE: {test_metrics['rmse']:.4f}")
        logger.info(f"  MAE:  {test_metrics['mae']:.4f}")
        logger.info(f"  R²:   {test_metrics['r2']:.4f}")
        
        # Show sample predictions
        logger.info("\nSample Predictions:")
        test_predictions.select(
            "student_id",
            "semester",
            col(self.target_col).alias("actual_gpa"),
            col("predicted_gpa"),
            (col("predicted_gpa") - col(self.target_col)).alias("error")
        ).show(10, truncate=False)
        
        # Calculate error distribution
        logger.info("\nError Distribution:")
        test_predictions \
            .withColumn("abs_error", abs(col("predicted_gpa") - col(self.target_col))) \
            .selectExpr(
                "percentile_approx(abs_error, 0.25) as q1_error",
                "percentile_approx(abs_error, 0.50) as median_error",
                "percentile_approx(abs_error, 0.75) as q3_error",
                "max(abs_error) as max_error"
            ).show()
        
        return test_metrics, test_predictions
    
    def compare_models(self):
        """Compare all trained models"""
        
        logger.info("=" * 70)
        logger.info("Model Comparison on Test Set")
        logger.info("=" * 70)
        
        models = []
        
        if hasattr(self, 'rf_model'):
            rf_metrics, _ = self.evaluate_on_test_set(self.rf_model, "Random Forest")
            models.append(("Random Forest", rf_metrics))
        
        if hasattr(self, 'gbt_model'):
            gbt_metrics, _ = self.evaluate_on_test_set(self.gbt_model, "Gradient Boosted Trees")
            models.append(("Gradient Boosted Trees", gbt_metrics))
        
        if hasattr(self, 'lr_model'):
            lr_metrics, _ = self.evaluate_on_test_set(self.lr_model, "Linear Regression")
            models.append(("Linear Regression", lr_metrics))
        
        # Create comparison DataFrame
        logger.info("\n" + "=" * 70)
        logger.info("Summary Comparison")
        logger.info("=" * 70)
        
        comparison_data = []
        for model_name, metrics in models:
            comparison_data.append({
                "Model": model_name,
                "RMSE": round(metrics['rmse'], 4),
                "MAE": round(metrics['mae'], 4),
                "R²": round(metrics['r2'], 4)
            })
        
        comparison_df = self.spark.createDataFrame(comparison_data)
        comparison_df.show(truncate=False)
        
        # Determine best model
        best_model_name = min(models, key=lambda x: x[1]['rmse'])[0]
        logger.info(f"\n🏆 Best Model (by RMSE): {best_model_name}")
        
        return comparison_df
    
    def save_model(self, model, model_name="gpa_predictor"):
        """
        Save trained model to HDFS
        
        Args:
            model: Trained model to save
            model_name: Name for the saved model
        """
        
        model_path = f"{self.hdfs_uri}/models/{model_name}/{self.processing_date}"
        
        logger.info(f"Saving model to {model_path}...")
        
        try:
            # Create full pipeline with preprocessing
            pipeline = Pipeline(stages=[
                self.assembler,
                self.scaler_model,
                model
            ])
            
            # Fit pipeline (already fitted stages will be reused)
            pipeline_model = pipeline.fit(self.features_df.limit(1))
            
            # Save pipeline
            pipeline_model.write().overwrite().save(model_path)
            
            logger.info(f"✓ Model saved successfully to {model_path}")
            
            # Save feature names for reference
            feature_metadata = {
                "model_name": model_name,
                "processing_date": self.processing_date,
                "feature_columns": self.feature_cols,
                "target_column": self.target_col,
                "model_type": type(model).__name__
            }
            
            metadata_df = self.spark.createDataFrame([feature_metadata])
            metadata_df.write \
                .mode("overwrite") \
                .json(f"{model_path}/metadata")
            
            logger.info("✓ Model metadata saved")
            
            return model_path
            
        except Exception as e:
            logger.error(f"Failed to save model: {e}")
            raise
    
    def run_training_pipeline(self, use_cv=False):
        """
        Execute complete GPA prediction training pipeline
        
        Args:
            use_cv: Whether to use cross-validation for Random Forest
        """
        
        logger.info("=" * 80)
        logger.info(" " * 20 + "GPA PREDICTION TRAINING PIPELINE")
        logger.info("=" * 80)
        
        try:
            # Load features
            if not self.load_features():
                raise Exception("Failed to load features")
            
            # Prepare data
            self.prepare_data(test_size=0.2, validation_size=0.1)
            
            # Train models
            logger.info("\n[1/3] Training Random Forest")
            logger.info("-" * 70)
            self.train_random_forest(use_cross_validation=use_cv)
            
            logger.info("\n[2/3] Training Gradient Boosted Trees")
            logger.info("-" * 70)
            self.train_gbt()
            
            logger.info("\n[3/3] Training Linear Regression (Baseline)")
            logger.info("-" * 70)
            self.train_linear_regression()
            
            # Compare models
            logger.info("\n")
            self.compare_models()
            
            # Save best model (Random Forest)
            logger.info("\nSaving Best Model")
            logger.info("-" * 70)
            model_path = self.save_model(self.rf_model, "gpa_predictor")
            
            logger.info("\n" + "=" * 80)
            logger.info(" " * 20 + "TRAINING PIPELINE COMPLETE")
            logger.info("=" * 80)
            logger.info(f"\nModel saved to: {model_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Training pipeline failed: {e}", exc_info=True)
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
    
    # Parse command line arguments
    use_cv = "--cross-validation" in sys.argv
    
    try:
        trainer = GPAPredictorTrainer(hdfs_uri=HDFS_URI)
        success = trainer.run_training_pipeline(use_cv=use_cv)
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"GPA prediction training failed: {e}")
        sys.exit(1)
    finally:
        trainer.cleanup()


if __name__ == "__main__":
    main()