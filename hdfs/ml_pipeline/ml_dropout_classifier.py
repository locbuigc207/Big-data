"""
ml_dropout_classifier.py
Uses Spark MLlib to train binary classification models for dropout risk prediction.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import logging
from datetime import datetime
import os
import sys

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Configuration ---
HDFS_URI = os.getenv(
    "HDFS_NAMENODE",
    "hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"
)

class DropoutRiskClassifier:
    """
    Trains binary classification models to predict dropout risk 
    using Spark MLlib.

    Models:
    - Random Forest Classifier (primary)
    - Gradient Boosted Trees Classifier
    - Logistic Regression (baseline)

    Evaluation Metrics:
    - AUC-ROC (Area Under ROC Curve)
    - AUC-PR (Area Under Precision-Recall Curve)
    - Accuracy, Precision, Recall, F1-Score
    - Confusion Matrix
    """

    def __init__(self, hdfs_uri: str = HDFS_URI):
        """
        Initialize dropout risk classifier trainer

        Args:
            hdfs_uri: HDFS namenode URI
        """
        self.hdfs_uri = hdfs_uri
        self.processing_date = datetime.now().strftime("%Y-%m-%d")

        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-DropoutClassifier") \
            .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "6g") \
            .config("spark.executor.instances", "4") \
            .config("spark.executor.cores", "2") \
            .getOrCreate()

        logger.info(f"Dropout Classifier initialized for {self.processing_date}")

        # Numerical features
        self.numerical_features = [
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
            "gpa_risk_score",
            "attendance_risk_score",
            "performance_risk_score",
            "overall_risk_score"
        ]

        # Categorical features
        self.categorical_features = [
            "gpa_category",
            "attendance_category",
            "performance_tier",
            "gpa_trajectory"
        ]

        self.target_col = "label"
        self.features_df = None
        self.train_df = None
        self.test_df = None
        
        self.indexers = None
        self.encoders = None
        self.assembler = None
        self.scaler_model = None
        
        self.rf_model = None
        self.gbt_model = None
        self.lr_model = None

    def load_features(self) -> bool:
        """Load engineered features from HDFS"""

        logger.info("Loading dropout risk features from HDFS...")

        features_path = f"{self.hdfs_uri}/ml_features/{self.processing_date}/dropout_risk"

        try:
            self.features_df = self.spark.read.parquet(features_path)

            # Remove nulls
            self.features_df = self.features_df.na.drop(
                subset=self.numerical_features + self.categorical_features + [self.target_col]
            )

            logger.info(f"✓ Loaded {self.features_df.count():,} training examples")

            # Check class distribution
            logger.info("\nClass Distribution:")
            class_dist = self.features_df.groupBy(self.target_col).count()
            class_dist.show()

            # Calculate class balance ratio
            counts = class_dist.collect()
            if len(counts) == 2:
                # Ensure the labels 0 and 1 are handled correctly
                count_0 = next((c['count'] for c in counts if c[self.target_col] == 0), 0)
                count_1 = next((c['count'] for c in counts if c[self.target_col] == 1), 0)
                
                if count_0 > 0 and count_1 > 0:
                    minority_count = min(count_0, count_1)
                    majority_count = max(count_0, count_1)
                    imbalance_ratio = majority_count / minority_count
                    logger.info(f"Class imbalance ratio: {imbalance_ratio:.2f}:1")

                    if imbalance_ratio > 3:
                        logger.warning("⚠️  Significant class imbalance detected!")
                        logger.warning("    Consider using class weights or resampling")
                else:
                    logger.warning("⚠️ Only one class found after loading and cleaning data.")

            return True

        except Exception as e:
            logger.error(f"Failed to load features: {e}")
            return False

    def prepare_data(self, test_size: float = 0.2, balance_classes: bool = False):
        """
        Prepare data for training

        Args:
            test_size: Proportion of data for testing
            balance_classes: Whether to balance classes using undersampling

        Returns:
            train_df, test_df
        """

        logger.info("=" * 70)
        logger.info("Preparing Training Data")
        logger.info("=" * 70)

        df = self.features_df

        # Balance classes if requested
        if balance_classes:
            logger.info("Balancing classes using undersampling...")

            # Get class counts
            class_counts = df.groupBy(self.target_col).count().collect()
            
            count_0 = next((c['count'] for c in class_counts if c[self.target_col] == 0), 0)
            count_1 = next((c['count'] for c in class_counts if c[self.target_col] == 1), 0)

            if count_0 == 0 or count_1 == 0:
                logger.warning("Cannot balance: One class is missing or empty.")
            else:
                minority_count = min(count_0, count_1)

                # Sample majority class
                df_minority = df.filter(col(self.target_col) == 1)
                
                # Calculate fraction to sample from majority class to match minority count
                majority_count = df.filter(col(self.target_col) == 0).count()
                
                # Use a small epsilon to avoid division by zero if majority_count is 0, though checked above
                fraction = minority_count / (majority_count if majority_count > 0 else 1) 
                
                df_majority = df.filter(col(self.target_col) == 0).sample(
                    fraction=fraction,
                    seed=42
                )

                df = df_minority.union(df_majority)

                logger.info(f"Balanced dataset size: {df.count():,}")

        # Process categorical features
        logger.info("Processing categorical features...")

        indexers = []
        encoders = []
        df_temp = df # Use a temporary DF to apply transformations sequentially

        for cat_col in self.categorical_features:
            # String Indexer
            indexer = StringIndexer(
                inputCol=cat_col,
                outputCol=f"{cat_col}_index",
                handleInvalid="keep"
            )
            indexer_model = indexer.fit(df_temp)
            df_temp = indexer_model.transform(df_temp)
            indexers.append(indexer_model)

            # One-Hot Encoder
            encoder = OneHotEncoder(
                inputCol=f"{cat_col}_index",
                outputCol=f"{cat_col}_encoded",
                dropLast=True
            )
            encoder_model = encoder.fit(df_temp)
            df_temp = encoder_model.transform(df_temp)
            encoders.append(encoder_model)
            
        df = df_temp # Final DataFrame after indexing/encoding

        # Assemble all features
        logger.info("Assembling feature vectors...")

        encoded_cat_features = [f"{cat}_encoded" for cat in self.categorical_features]
        all_features = self.numerical_features + encoded_cat_features

        assembler = VectorAssembler(
            inputCols=all_features,
            outputCol="features_raw",
            handleInvalid="skip"
        )

        df = assembler.transform(df)

        # Scale features
        logger.info("Scaling features...")

        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=False  # Don't center for sparse vectors
        )

        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)

        # Save preprocessing objects
        self.indexers = indexers
        self.encoders = encoders
        self.assembler = assembler
        self.scaler_model = scaler_model

        # Split data
        logger.info(f"Splitting data: {1-test_size:.0%} train, {test_size:.0%} test...")

        train_df, test_df = df.randomSplit([1 - test_size, test_size], seed=42)

        # Cache datasets
        train_df.cache()
        test_df.cache()

        logger.info(f"\nDataset Sizes:")
        logger.info(f"  Training: {train_df.count():,} examples")
        logger.info(f"  Test:     {test_df.count():,} examples")

        # Verify class distribution in splits
        logger.info("\nClass distribution in training set:")
        train_df.groupBy(self.target_col).count().show()

        logger.info("Class distribution in test set:")
        test_df.groupBy(self.target_col).count().show()

        self.train_df = train_df
        self.test_df = test_df

        return train_df, test_df

    def train_random_forest(self, use_class_weights: bool = True):
        """
        Train Random Forest classification model

        Args:
            use_class_weights: Whether to use class weights for imbalanced data (Note: 
                                class weighting logic needs implementation based on 
                                data size/Spark version if desired). Currently only 
                                hyperparameters are set.

        Returns:
            Trained model and validation metrics
        """

        logger.info("=" * 70)
        logger.info("Training Random Forest Classifier")
        logger.info("=" * 70)

        # Initialize Random Forest
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol=self.target_col,
            predictionCol="prediction",
            probabilityCol="probability",
            rawPredictionCol="rawPrediction",
            numTrees=100,
            maxDepth=10,
            minInstancesPerNode=5,
            maxBins=32,
            seed=42
        )

        logger.info(f"Hyperparameters:")
        logger.info(f"  Number of Trees: {rf.getNumTrees()}")
        logger.info(f"  Max Depth: {rf.getMaxDepth()}")
        logger.info(f"  Min Instances Per Node: {rf.getMinInstancesPerNode()}")

        # Train
        logger.info("Training Random Forest model...")
        self.rf_model = rf.fit(self.train_df)

        # Evaluate
        logger.info("\nEvaluating on test set...")
        test_predictions = self.rf_model.transform(self.test_df)

        metrics = self._evaluate_classification(test_predictions)

        logger.info("\nTest Set Metrics:")
        logger.info(f"  AUC-ROC:   {metrics['auc_roc']:.4f}")
        logger.info(f"  AUC-PR:    {metrics['auc_pr']:.4f}")
        logger.info(f"  Accuracy:  {metrics['accuracy']:.4f}")
        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall:    {metrics['recall']:.4f}")
        logger.info(f"  F1-Score:  {metrics['f1']:.4f}")

        # Feature importance
        self._show_feature_importance(self.rf_model)

        # Confusion matrix
        self._show_confusion_matrix(test_predictions)

        return self.rf_model, metrics

    def train_gbt(self):
        """Train Gradient Boosted Trees classifier"""

        logger.info("=" * 70)
        logger.info("Training Gradient Boosted Trees Classifier")
        logger.info("=" * 70)

        gbt = GBTClassifier(
            featuresCol="features",
            labelCol=self.target_col,
            predictionCol="prediction",
            maxIter=100,
            maxDepth=5,
            stepSize=0.1,
            seed=42
        )

        logger.info(f"Hyperparameters:")
        logger.info(f"  Max Iterations: {gbt.getMaxIter()}")
        logger.info(f"  Max Depth: {gbt.getMaxDepth()}")
        logger.info(f"  Step Size: {gbt.getStepSize()}")

        logger.info("Training GBT model...")
        self.gbt_model = gbt.fit(self.train_df)

        test_predictions = self.gbt_model.transform(self.test_df)
        metrics = self._evaluate_classification(test_predictions)

        logger.info("\nTest Set Metrics:")
        logger.info(f"  AUC-ROC:   {metrics['auc_roc']:.4f}")
        logger.info(f"  AUC-PR:    {metrics['auc_pr']:.4f}")
        logger.info(f"  Accuracy:  {metrics['accuracy']:.4f}")
        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall:    {metrics['recall']:.4f}")
        logger.info(f"  F1-Score:  {metrics['f1']:.4f}")

        self._show_feature_importance(self.gbt_model)
        self._show_confusion_matrix(test_predictions)

        return self.gbt_model, metrics

    def train_logistic_regression(self):
        """Train Logistic Regression (baseline)"""

        logger.info("=" * 70)
        logger.info("Training Logistic Regression (Baseline)")
        logger.info("=" * 70)

        lr = LogisticRegression(
            featuresCol="features",
            labelCol=self.target_col,
            predictionCol="prediction",
            probabilityCol="probability",
            maxIter=100,
            regParam=0.1,
            elasticNetParam=0.5
        )

        logger.info(f"Hyperparameters:")
        logger.info(f"  Max Iterations: {lr.getMaxIter()}")
        logger.info(f"  Regularization: {lr.getRegParam()}")
        logger.info(f"  ElasticNet Mix: {lr.getElasticNetParam()}")

        logger.info("Training Logistic Regression model...")
        self.lr_model = lr.fit(self.train_df)

        test_predictions = self.lr_model.transform(self.test_df)
        metrics = self._evaluate_classification(test_predictions)

        logger.info("\nTest Set Metrics:")
        logger.info(f"  AUC-ROC:   {metrics['auc_roc']:.4f}")
        logger.info(f"  AUC-PR:    {metrics['auc_pr']:.4f}")
        logger.info(f"  Accuracy:  {metrics['accuracy']:.4f}")
        logger.info(f"  Precision: {metrics['precision']:.4f}")
        logger.info(f"  Recall:    {metrics['recall']:.4f}")
        logger.info(f"  F1-Score:  {metrics['f1']:.4f}")

        self._show_confusion_matrix(test_predictions)

        return self.lr_model, metrics

    def _evaluate_classification(self, predictions_df) -> dict:
        """Evaluate binary classification predictions"""

        # AUC-ROC
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol=self.target_col,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc_roc = evaluator_auc.evaluate(predictions_df)

        # AUC-PR
        evaluator_pr = BinaryClassificationEvaluator(
            labelCol=self.target_col,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderPR"
        )
        auc_pr = evaluator_pr.evaluate(predictions_df)

        # Multi-class metrics
        evaluator_mc = MulticlassClassificationEvaluator(
            labelCol=self.target_col,
            predictionCol="prediction"
        )

        accuracy = evaluator_mc.evaluate(predictions_df, {evaluator_mc.metricName: "accuracy"})
        precision = evaluator_mc.evaluate(predictions_df, {evaluator_mc.metricName: "weightedPrecision"})
        recall = evaluator_mc.evaluate(predictions_df, {evaluator_mc.metricName: "weightedRecall"})
        f1 = evaluator_mc.evaluate(predictions_df, {evaluator_mc.metricName: "f1"})

        return {
            "auc_roc": auc_roc,
            "auc_pr": auc_pr,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1
        }

    def _show_feature_importance(self, model):
        """Display feature importance"""

        try:
            if hasattr(model, 'featureImportances'):
                importances = model.featureImportances.toArray()
            else:
                return

            all_features = self.numerical_features + \
                          [f"{cat}_encoded" for cat in self.categorical_features]

            feature_importance = list(zip(all_features, importances))
            feature_importance.sort(key=lambda x: x[1], reverse=True)

            logger.info("\nFeature Importance (top 10):")
            for i, (feature, importance) in enumerate(feature_importance[:10], 1):
                logger.info(f"  {i:2d}. {feature:30s}: {importance:.4f}")

        except Exception as e:
            logger.warning(f"Could not extract feature importance: {e}")

    def _show_confusion_matrix(self, predictions_df):
        """Display confusion matrix"""

        logger.info("\nConfusion Matrix:")

        # The confusion matrix table itself
        cm = predictions_df \
            .groupBy(self.target_col, "prediction") \
            .count() \
            .orderBy(self.target_col, "prediction")
        
        cm.show()

        # Calculate detailed metrics
        tp = predictions_df.filter((col(self.target_col) == 1) & (col("prediction") == 1)).count()
        tn = predictions_df.filter((col(self.target_col) == 0) & (col("prediction") == 0)).count()
        fp = predictions_df.filter((col(self.target_col) == 0) & (col("prediction") == 1)).count()
        fn = predictions_df.filter((col(self.target_col) == 1) & (col("prediction") == 0)).count()

        logger.info(f"\nDetailed Metrics:")
        logger.info(f"  True Positives:  {tp}")
        logger.info(f"  True Negatives:  {tn}")
        logger.info(f"  False Positives: {fp}")
        logger.info(f"  False Negatives: {fn}")

        if tp + fp > 0:
            precision_pos = tp / (tp + fp)
            logger.info(f"  Precision (At-Risk): {precision_pos:.4f}")
        else:
            logger.info("  Precision (At-Risk): N/A (No positive predictions)")
            
        if tp + fn > 0:
            recall_pos = tp / (tp + fn)
            logger.info(f"  Recall (At-Risk):    {recall_pos:.4f}")
        else:
            logger.info("  Recall (At-Risk):    N/A (No true positive cases)")

    def save_model(self, model, model_name: str = "dropout_risk") -> str | None:
        """Save trained model pipeline to HDFS"""

        model_path = f"{self.hdfs_uri}/models/{model_name}/{self.processing_date}"

        logger.info(f"Saving model to {model_path}...")

        try:
            # Create pipeline with all preprocessing steps and the final model
            # Note: scaler_model, indexers, encoders are already fitted objects (models)
            stages = self.indexers + self.encoders + [self.assembler, self.scaler_model, model]
            
            # Create a dummy pipeline to write the stages, fitting it to a minimal DF 
            # as Spark ML requires the pipeline structure to be fitted before saving.
            pipeline = Pipeline(stages=stages)
            
            # Use a dummy fit on a single row (since all models in stages are already fitted)
            # This is often needed when saving a full pipeline model in Spark ML.
            pipeline_model = pipeline.fit(self.features_df.limit(1)) 

            # Save the fitted pipeline model
            pipeline_model.write().overwrite().save(model_path)

            logger.info(f"✓ Model saved to {model_path}")

            return model_path

        except Exception as e:
            logger.error(f"Failed to save model: {e}")
            return None

    def run_training_pipeline(self, balance_classes: bool = False) -> bool:
        """Execute complete dropout risk training pipeline"""

        logger.info("=" * 80)
        logger.info(" " * 15 + "DROPOUT RISK CLASSIFICATION TRAINING")
        logger.info("=" * 80)

        try:
            if not self.load_features():
                raise Exception("Failed to load features")

            self.prepare_data(test_size=0.2, balance_classes=balance_classes)

            # --- 1. Random Forest ---
            logger.info("\n[1/3] Training Random Forest")
            logger.info("-" * 70)
            self.train_random_forest()

            # --- 2. Gradient Boosted Trees ---
            logger.info("\n[2/3] Training Gradient Boosted Trees")
            logger.info("-" * 70)
            self.train_gbt()

            # --- 3. Logistic Regression ---
            logger.info("\n[3/3] Training Logistic Regression")
            logger.info("-" * 70)
            self.train_logistic_regression()

            # --- Save Best Model (Assuming Random Forest is the target best model) ---
            logger.info("\nSaving Best Model (Random Forest)")
            logger.info("-" * 70)
            model_path = self.save_model(self.rf_model, "dropout_risk")

            logger.info("\n" + "=" * 80)
            logger.info(" " * 20 + "TRAINING COMPLETE")
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

# --- Main Execution ---
def main():
    """Main execution entry point"""
    
    # Determine if class balancing is requested
    balance = "--balance-classes" in sys.argv
    trainer = None
    
    try:
        trainer = DropoutRiskClassifier(hdfs_uri=HDFS_URI)
        success = trainer.run_training_pipeline(balance_classes=balance)
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Dropout classification training failed: {e}")
        sys.exit(1)
        
    finally:
        # Ensure Spark session is stopped, even if training fails
        if trainer:
            trainer.cleanup()

if __name__ == "__main__":
    main()