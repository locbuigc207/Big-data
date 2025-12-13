"""
Profile & Notification Batch Job - User Profile and Notification Analytics
Precomputes profile and notification-related batch views from raw events in MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, date_format, 
    sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, expr
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys

def create_spark_session():
    """Initialize Spark Session with MinIO configuration"""
    return SparkSession.builder \
        .appName("Profile_Notification_Batch_Job") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

# ============ PROFILE ANALYTICS ============

def compute_profile_update_frequency(df_profile):
    """Track how often users update their profiles"""
    updates = df_profile.filter(col("event_type") == "UPDATE_PROFILE")
    
    return updates.groupBy("user_id") \
        .agg(
            count("user_id").alias("total_profile_updates"),
            spark_min("timestamp_parsed").alias("first_update"),
            spark_max("timestamp_parsed").alias("last_update")
        ) \
        .orderBy(col("total_profile_updates").desc())

def compute_profile_field_changes(df_profile):
    """Analyze which profile fields are updated most frequently"""
    updates = df_profile.filter(col("event_type") == "UPDATE_PROFILE")
    
    return updates.groupBy("field_name") \
        .agg(
            count("user_id").alias("total_updates"),
            countDistinct("user_id").alias("unique_users_updating")
        ) \
        .orderBy(col("total_updates").desc())

def compute_avatar_change_stats(df_profile):
    """Track avatar change patterns"""
    avatar_changes = df_profile.filter(col("event_type") == "CHANGE_AVATAR")
    
    return avatar_changes.groupBy("user_id") \
        .agg(
            count("user_id").alias("avatar_changes_count"),
            spark_min("timestamp_parsed").alias("first_change"),
            spark_max("timestamp_parsed").alias("last_change")
        ) \
        .orderBy(col("avatar_changes_count").desc())

def compute_daily_profile_activity(df_profile):
    """Track daily profile management activity"""
    return df_profile.withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "event_type") \
        .agg(
            countDistinct("user_id").alias("unique_users"),
            count("user_id").alias("event_count")
        ) \
        .orderBy("date", "event_type")

# ============ NOTIFICATION ANALYTICS ============

def compute_notification_delivery_stats(df_notif):
    """Compute notification delivery statistics"""
    delivered = df_notif.filter(col("event_type") == "NOTIFICATION_SENT")
    
    return delivered.groupBy("notification_type") \
        .agg(
            count("user_id").alias("total_sent"),
            countDistinct("user_id").alias("unique_recipients")
        ) \
        .orderBy(col("total_sent").desc())

def compute_notification_engagement(df_notif):
    """Analyze notification engagement (clicks)"""
    clicks = df_notif.filter(col("event_type") == "NOTIFICATION_CLICKED")
    
    return clicks.groupBy("user_id", "notification_type") \
        .agg(
            count("notification_id").alias("notifications_clicked")
        ) \
        .orderBy(col("notifications_clicked").desc())

def compute_notification_click_through_rate(df_notif):
    """Compute click-through rate per notification type"""
    # Count sent
    sent = df_notif.filter(col("event_type") == "NOTIFICATION_SENT") \
        .groupBy("notification_type") \
        .agg(count("notification_id").alias("total_sent"))
    
    # Count clicked
    clicked = df_notif.filter(col("event_type") == "NOTIFICATION_CLICKED") \
        .groupBy("notification_type") \
        .agg(count("notification_id").alias("total_clicked"))
    
    # Join and compute CTR
    return sent.join(clicked, "notification_type", "left") \
        .na.fill(0, ["total_clicked"]) \
        .withColumn(
            "click_through_rate",
            (col("total_clicked") / col("total_sent") * 100).cast("decimal(5,2)")
        ) \
        .orderBy(col("click_through_rate").desc())

def compute_user_notification_preferences(df_notif):
    """Analyze user engagement with different notification types"""
    # Received notifications
    received = df_notif.filter(col("event_type") == "NOTIFICATION_SENT") \
        .groupBy("user_id", "notification_type") \
        .agg(count("notification_id").alias("notifications_received"))
    
    # Clicked notifications
    clicked = df_notif.filter(col("event_type") == "NOTIFICATION_CLICKED") \
        .groupBy("user_id", "notification_type") \
        .agg(count("notification_id").alias("notifications_clicked"))
    
    # Combine
    return received.join(clicked, ["user_id", "notification_type"], "left") \
        .na.fill(0, ["notifications_clicked"]) \
        .withColumn(
            "engagement_rate",
            (col("notifications_clicked") / col("notifications_received") * 100).cast("decimal(5,2)")
        ) \
        .orderBy("user_id", col("engagement_rate").desc())

def compute_daily_notification_activity(df_notif):
    """Track daily notification delivery and engagement"""
    return df_notif.withColumn("date", date_format(col("timestamp_parsed"), "yyyy-MM-dd")) \
        .groupBy("date", "event_type", "notification_type") \
        .agg(
            countDistinct("user_id").alias("unique_users"),
            count("notification_id").alias("event_count")
        ) \
        .orderBy("date", "event_type", "notification_type")

def compute_user_notification_summary(df_notif):
    """Overall notification summary per user"""
    return df_notif.groupBy("user_id") \
        .agg(
            count(when(col("event_type") == "NOTIFICATION_SENT", 1)).alias("total_received"),
            count(when(col("event_type") == "NOTIFICATION_CLICKED", 1)).alias("total_clicked")
        ) \
        .withColumn(
            "overall_engagement_rate",
            (col("total_clicked") / col("total_received") * 100).cast("decimal(5,2)")
        ) \
        .orderBy(col("overall_engagement_rate").desc())

def main(input_path, output_path):
    """Main batch job execution"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # ========== PROFILE PROCESSING ==========
    print(f"[PROFILE BATCH] Reading profile events from: {input_path}")
    df_profile_raw = spark.read.parquet(f"{input_path}/topic=profile_topic")
    df_profile = df_profile_raw.withColumn("timestamp_parsed", col("timestamp").cast(TimestampType()))
    
    print(f"[PROFILE BATCH] Total profile events: {df_profile.count()}")
    
    print("[PROFILE BATCH] Computing profile update frequency...")
    update_freq = compute_profile_update_frequency(df_profile)
    update_freq.write.mode("overwrite").parquet(f"{output_path}/profile_update_frequency")
    
    print("[PROFILE BATCH] Computing profile field changes...")
    field_changes = compute_profile_field_changes(df_profile)
    field_changes.write.mode("overwrite").parquet(f"{output_path}/profile_field_changes")
    
    print("[PROFILE BATCH] Computing avatar change stats...")
    avatar_stats = compute_avatar_change_stats(df_profile)
    avatar_stats.write.mode("overwrite").parquet(f"{output_path}/profile_avatar_changes")
    
    print("[PROFILE BATCH] Computing daily profile activity...")
    daily_profile = compute_daily_profile_activity(df_profile)
    daily_profile.write.mode("overwrite").parquet(f"{output_path}/profile_daily_activity")
    
    # ========== NOTIFICATION PROCESSING ==========
    print(f"[NOTIFICATION BATCH] Reading notification events from: {input_path}")
    df_notif_raw = spark.read.parquet(f"{input_path}/topic=notification_topic")
    df_notif = df_notif_raw.withColumn("timestamp_parsed", col("timestamp").cast(TimestampType()))
    
    print(f"[NOTIFICATION BATCH] Total notification events: {df_notif.count()}")
    
    print("[NOTIFICATION BATCH] Computing delivery statistics...")
    delivery_stats = compute_notification_delivery_stats(df_notif)
    delivery_stats.write.mode("overwrite").parquet(f"{output_path}/notification_delivery_stats")
    
    print("[NOTIFICATION BATCH] Computing engagement metrics...")
    engagement = compute_notification_engagement(df_notif)
    engagement.write.mode("overwrite").parquet(f"{output_path}/notification_engagement")
    
    print("[NOTIFICATION BATCH] Computing click-through rates...")
    ctr = compute_notification_click_through_rate(df_notif)
    ctr.write.mode("overwrite").parquet(f"{output_path}/notification_click_through_rate")
    
    print("[NOTIFICATION BATCH] Computing user notification preferences...")
    preferences = compute_user_notification_preferences(df_notif)
    preferences.write.mode("overwrite").parquet(f"{output_path}/notification_user_preferences")
    
    print("[NOTIFICATION BATCH] Computing daily notification activity...")
    daily_notif = compute_daily_notification_activity(df_notif)
    daily_notif.write.mode("overwrite").parquet(f"{output_path}/notification_daily_activity")
    
    print("[NOTIFICATION BATCH] Computing user notification summary...")
    summary = compute_user_notification_summary(df_notif)
    summary.write.mode("overwrite").parquet(f"{output_path}/notification_user_summary")
    
    print("[PROFILE & NOTIFICATION BATCH] Batch job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: profile_notification_batch_job.py <input_path> <output_path>")
        print("Example: profile_notification_batch_job.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
