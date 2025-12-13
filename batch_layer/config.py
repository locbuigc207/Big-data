# Batch Layer Configuration
# Central configuration for all batch processing jobs

batch_config = {
    # MinIO / S3 Configuration
    "minio": {
        "endpoint": "http://minio:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "ssl_enabled": False,
        "path_style_access": True
    },
    
    # Data Paths
    "paths": {
        "input": "s3a://bucket-0/master_dataset",
        "output": "s3a://bucket-0/batch_views",
        "checkpoint": "s3a://bucket-0/checkpoints/batch"
    },
    
    # Kafka Topics
    "topics": {
        "auth": "auth_topic",
        "assessment": "assessment_topic",
        "video": "video_topic",
        "course": "course_topic",
        "profile": "profile_topic",
        "notification": "notification_topic"
    },
    
    # Spark Configuration
    "spark": {
        "executor_memory": "4g",
        "executor_cores": 2,
        "num_executors": 3,
        "driver_memory": "2g",
        "shuffle_partitions": 200,
        "default_parallelism": 100
    },
    
    # Batch Processing Schedule
    "schedule": {
        "frequency": "daily",  # daily, hourly, weekly
        "time": "00:00",  # UTC time for daily runs
        "timezone": "UTC"
    },
    
    # Batch Views Output Configuration
    "output": {
        "format": "parquet",
        "mode": "overwrite",
        "compression": "snappy",
        "partition_by": None  # Can add partitioning if needed
    },
    
    # Oozie Configuration
    "oozie": {
        "workflow_path": "/user/batch_layer/oozie",
        "coordinator_frequency": "24h",
        "job_tracker": "resourcemanager:8032",
        "name_node": "hdfs://namenode:9000"
    },
    
    # Batch Views Metadata
    "batch_views": {
        "auth": [
            "auth_daily_active_users",
            "auth_hourly_login_patterns",
            "auth_user_session_metrics",
            "auth_user_activity_summary",
            "auth_registration_analytics"
        ],
        "assessment": [
            "assessment_student_submissions",
            "assessment_engagement_timeline",
            "assessment_quiz_performance",
            "assessment_grading_stats",
            "assessment_teacher_workload",
            "assessment_submission_distribution",
            "assessment_student_overall_performance"
        ],
        "video": [
            "video_total_watch_time",
            "video_student_engagement",
            "video_popularity",
            "video_daily_engagement",
            "video_course_metrics",
            "video_student_course_summary",
            "video_drop_off_indicators"
        ],
        "course": [
            "course_enrollment_stats",
            "course_material_access",
            "course_material_popularity",
            "course_download_analytics",
            "course_resource_download_stats",
            "course_activity_summary",
            "course_daily_engagement",
            "course_overall_metrics"
        ],
        "profile_notification": [
            "profile_update_frequency",
            "profile_field_changes",
            "profile_avatar_changes",
            "profile_daily_activity",
            "notification_delivery_stats",
            "notification_engagement",
            "notification_click_through_rate",
            "notification_user_preferences",
            "notification_daily_activity",
            "notification_user_summary"
        ]
    },
    
    # Logging
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    }
}

def get_spark_config():
    """Get Spark configuration dictionary"""
    return {
        "spark.hadoop.fs.s3a.endpoint": batch_config["minio"]["endpoint"],
        "spark.hadoop.fs.s3a.access.key": batch_config["minio"]["access_key"],
        "spark.hadoop.fs.s3a.secret.key": batch_config["minio"]["secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": str(batch_config["minio"]["path_style_access"]).lower(),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(batch_config["minio"]["ssl_enabled"]).lower(),
        "spark.executor.memory": batch_config["spark"]["executor_memory"],
        "spark.executor.cores": str(batch_config["spark"]["executor_cores"]),
        "spark.sql.shuffle.partitions": str(batch_config["spark"]["shuffle_partitions"]),
        "spark.default.parallelism": str(batch_config["spark"]["default_parallelism"])
    }

def get_input_path(topic=None):
    """Get input path for batch processing"""
    base_path = batch_config["paths"]["input"]
    if topic:
        return f"{base_path}/topic={topic}"
    return base_path

def get_output_path(view_name=None):
    """Get output path for batch views"""
    base_path = batch_config["paths"]["output"]
    if view_name:
        return f"{base_path}/{view_name}"
    return base_path

def get_all_batch_views():
    """Get list of all batch view names"""
    all_views = []
    for category, views in batch_config["batch_views"].items():
        all_views.extend(views)
    return all_views
