from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, unix_timestamp, from_unixtime
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import logging

from config import serving_config, get_spark_config, get_batch_view_path, get_realtime_view_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ViewMerger:
    """
    Merges batch and real-time views to provide complete, up-to-date results
    
    Strategy:
    - Batch views: Historical data (everything up to ~1 hour ago)
    - Real-time views: Recent data (last 60 minutes)
    - Merged view: Union of both, with real-time taking precedence
    """
    
    def __init__(self):
        """Initialize Spark session and configuration"""
        self.spark = self._create_spark_session()
        self.config = serving_config
        logger.info("ViewMerger initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        builder = SparkSession.builder \
            .appName(serving_config["spark"]["app_name"])
        
        # Apply configurations
        for key, value in get_spark_config().items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def read_batch_view(self, view_name: str) -> Optional[DataFrame]:
        """
        Read batch view from MinIO
        
        Args:
            view_name: Name of the batch view
            
        Returns:
            DataFrame or None if view doesn't exist
        """
        try:
            path = get_batch_view_path(view_name)
            logger.info(f"Reading batch view: {view_name} from {path}")
            df = self.spark.read.parquet(path)
            logger.info(f"Batch view {view_name}: {df.count()} records")
            return df
        except Exception as e:
            logger.error(f"Error reading batch view {view_name}: {str(e)}")
            return None
    
    def read_realtime_view(self, view_name: str, window_minutes: int = 60) -> Optional[DataFrame]:
        """
        Read real-time view from MinIO, filtered to recent window
        
        Args:
            view_name: Name of the real-time view
            window_minutes: How many minutes of recent data to include
            
        Returns:
            DataFrame or None if view doesn't exist
        """
        try:
            path = get_realtime_view_path(view_name)
            logger.info(f"Reading realtime view: {view_name} from {path}")
            df = self.spark.read.parquet(path)
            
            # Filter to recent window
            cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
            
            # Determine timestamp column (varies by view)
            timestamp_col = None
            for col_name in df.columns:
                if 'window_start' in col_name or 'timestamp' in col_name.lower():
                    timestamp_col = col_name
                    break
            
            if timestamp_col:
                df_filtered = df.filter(col(timestamp_col) >= lit(cutoff_time))
                logger.info(f"Realtime view {view_name}: {df_filtered.count()} records (last {window_minutes} min)")
                return df_filtered
            else:
                logger.warning(f"No timestamp column found in {view_name}, returning all data")
                return df
                
        except Exception as e:
            logger.error(f"Error reading realtime view {view_name}: {str(e)}")
            return None
    
    def merge_views(
        self, 
        batch_df: Optional[DataFrame], 
        realtime_df: Optional[DataFrame],
        merge_key: str = None
    ) -> Optional[DataFrame]:
        """
        Merge batch and realtime views
        
        Strategy:
        - If only batch exists: return batch
        - If only realtime exists: return realtime
        - If both exist: union them (realtime supplements batch)
        
        Args:
            batch_df: Batch view DataFrame
            realtime_df: Realtime view DataFrame
            merge_key: Column to use for deduplication (optional)
            
        Returns:
            Merged DataFrame
        """
        if batch_df is None and realtime_df is None:
            logger.warning("Both batch and realtime views are None")
            return None
        
        if batch_df is None:
            logger.info("Only realtime view available")
            return realtime_df.withColumn("source", lit("realtime"))
        
        if realtime_df is None:
            logger.info("Only batch view available")
            return batch_df.withColumn("source", lit("batch"))
        
        # Both views exist - merge them
        logger.info("Merging batch and realtime views")
        
        # Add source column
        batch_with_source = batch_df.withColumn("source", lit("batch"))
        realtime_with_source = realtime_df.withColumn("source", lit("realtime"))
        
        # Union
        merged = batch_with_source.union(realtime_with_source)
        
        # Deduplicate if merge key provided
        if merge_key and merge_key in merged.columns:
            # Keep realtime version if duplicates exist
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number, when
            
            window = Window.partitionBy(merge_key).orderBy(
                when(col("source") == "realtime", 0).otherwise(1)
            )
            
            merged = merged.withColumn("row_num", row_number().over(window)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")
        
        logger.info(f"Merged view: {merged.count()} records")
        return merged
    
    def get_active_users(self, minutes: int = 60) -> Optional[DataFrame]:
        """
        Get active users combining batch daily stats + realtime 1-min windows
        
        Example of a common query pattern
        """
        logger.info(f"Getting active users (last {minutes} minutes)")
        
        # Get batch view (daily aggregates)
        batch_df = self.read_batch_view("auth_daily_active_users")
        
        # Get realtime view (1-minute windows)
        realtime_df = self.read_realtime_view("auth_realtime_active_users_1min", window_minutes=minutes)
        
        # For active users, we just want to show both sources
        # Batch gives historical context, realtime gives current status
        return self.merge_views(batch_df, realtime_df)
    
    def get_video_engagement(self, video_id: str = None) -> Optional[DataFrame]:
        """
        Get video engagement metrics
        
        Combines:
        - Batch: Total watch time (all history)
        - Realtime: Current viewers and recent watch time
        """
        logger.info(f"Getting video engagement for video_id={video_id}")
        
        # Batch view
        batch_df = self.read_batch_view("video_total_watch_time")
        if video_id and batch_df:
            batch_df = batch_df.filter(col("video_id") == video_id)
        
        # Realtime view
        realtime_df = self.read_realtime_view("video_realtime_watch_time_5min", window_minutes=60)
        if video_id and realtime_df:
            realtime_df = realtime_df.filter(col("video_id") == video_id)
        
        # Merge
        merged = self.merge_views(batch_df, realtime_df, merge_key="video_id")
        
        if merged:
            # Aggregate total watch time
            from pyspark.sql.functions import sum as spark_sum, count, max as spark_max
            
            result = merged.groupBy("video_id").agg(
                spark_sum("total_watch_seconds").alias("total_watch_seconds"),
                count("*").alias("data_points"),
                spark_max("source").alias("latest_source")
            )
            return result
        
        return None
    
    def get_student_performance(self, student_id: str) -> Optional[DataFrame]:
        """
        Get comprehensive student performance metrics
        
        Combines:
        - Batch: Historical grades, quiz performance
        - Realtime: Recent submission activity
        """
        logger.info(f"Getting performance for student_id={student_id}")
        
        # Batch views
        batch_grades = self.read_batch_view("assessment_grading_stats")
        batch_quiz = self.read_batch_view("assessment_quiz_performance")
        
        # Filter to student
        if batch_grades:
            batch_grades = batch_grades.filter(col("student_id") == student_id)
        if batch_quiz:
            batch_quiz = batch_quiz.filter(col("user_id") == student_id)
        
        # Realtime views
        realtime_submissions = self.read_realtime_view("assessment_realtime_submissions_1min", window_minutes=60)
        
        # Combine all sources
        result = {
            "batch_grades": batch_grades,
            "batch_quiz": batch_quiz,
            "realtime_submissions": realtime_submissions
        }
        
        return result
    
    def get_course_metrics(self, course_id: str) -> Dict[str, Any]:
        """
        Get comprehensive course metrics
        
        Returns dictionary with multiple DataFrames
        """
        logger.info(f"Getting metrics for course_id={course_id}")
        
        # Batch views
        batch_enrollment = self.read_batch_view("course_enrollment_stats")
        batch_materials = self.read_batch_view("course_material_popularity")
        
        # Filter
        if batch_enrollment:
            batch_enrollment = batch_enrollment.filter(col("course_id") == course_id)
        if batch_materials:
            batch_materials = batch_materials.filter(col("course_id") == course_id)
        
        # Realtime views
        realtime_active = self.read_realtime_view("course_realtime_active_courses_1min", window_minutes=60)
        realtime_enrollment = self.read_realtime_view("course_realtime_enrollment_rate", window_minutes=60)
        
        if realtime_enrollment:
            realtime_enrollment = realtime_enrollment.filter(col("course_id") == course_id)
        
        return {
            "batch_enrollment": batch_enrollment,
            "batch_materials": batch_materials,
            "realtime_active": realtime_active,
            "realtime_enrollment": realtime_enrollment
        }
    
    def cache_view(self, df: DataFrame, cache_key: str):
        """Cache a DataFrame for faster subsequent access"""
        if serving_config["cache"]["enabled"]:
            df.cache()
            logger.info(f"Cached view: {cache_key}")
    
    def clear_cache(self):
        """Clear all cached DataFrames"""
        self.spark.catalog.clearCache()
        logger.info("Cache cleared")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        logger.info("ViewMerger stopped")


# Example usage
if __name__ == "__main__":
    merger = ViewMerger()
    
    # Example 1: Get active users
    print("\n=== Active Users ===")
    active_users = merger.get_active_users(minutes=60)
    if active_users:
        active_users.show(10)
    
    # Example 2: Get video engagement
    print("\n=== Video Engagement ===")
    video_engagement = merger.get_video_engagement(video_id="VID001")
    if video_engagement:
        video_engagement.show()
    
    merger.stop()