# Speed Layer Configuration
# Central configuration for all real-time stream processing jobs

speed_config = {
    # Kafka Configuration
    "kafka": {
        "bootstrap_servers": "kafka:29092",  # Use "localhost:9092" for local testing
        "topics": {
            "auth": "auth_topic",
            "assessment": "assessment_topic",
            "video": "video_topic",
            "course": "course_topic",
            "profile": "profile_topic",
            "notification": "notification_topic"
        },
        "starting_offsets": "latest",  # or "earliest" for replay
        "max_offsets_per_trigger": 10000,
        "fetch_min_bytes": 1024,
        "group_id": "speed_layer_consumer_group"
    },
    
    # MinIO / S3 Configuration for real-time views
    "minio": {
        "endpoint": "http://minio:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "ssl_enabled": False,
        "path_style_access": True
    },
    
    # Data Paths
    "paths": {
        "output": "s3a://bucket-0/realtime_views",
        "checkpoint": "s3a://bucket-0/checkpoints/speed"
    },
    
    # Window Configurations
    "windows": {
        "micro": "1 minute",          # Ultra-short term metrics
        "short": "5 minutes",         # Short term metrics
        "medium": "15 minutes",       # Medium term metrics
        "long": "1 hour",             # Longer term metrics
        "watermark": "2 minutes"      # Late data tolerance
    },
    
    # Spark Structured Streaming Configuration
    "spark": {
        "app_name_prefix": "SpeedLayer",
        "executor_memory": "2g",
        "executor_cores": 2,
        "num_executors": 2,
        "driver_memory": "1g",
        "shuffle_partitions": 50,
        "default_parallelism": 50,
        "streaming_checkpoint_interval": "10 seconds"
    },
    
    # Output Configuration
    "output": {
        "format": "parquet",
        "mode": "append",  # For streaming, use append
        "compression": "snappy",
        "trigger_interval": "30 seconds",  # How often to write output
        "output_mode": "append"  # append, complete, or update
    },
    
    # Stateful Processing Configuration
    "stateful": {
        "state_timeout": "1 hour",
        "enable_state_store": True,
        "state_store_provider": "hdfs"  # or "rocksdb" for production
    },
    
    # Real-time Views Metadata
    "realtime_views": {
        "auth": [
            "auth_realtime_active_users_1min",
            "auth_realtime_active_users_5min",
            "auth_realtime_login_rate",
            "auth_realtime_session_activity",
            "auth_realtime_failed_logins"
        ],
        "assessment": [
            "assessment_realtime_submissions_1min",
            "assessment_realtime_active_students",
            "assessment_realtime_quiz_attempts",
            "assessment_realtime_grading_queue",
            "assessment_realtime_submission_rate"
        ],
        "video": [
            "video_realtime_viewers_1min",
            "video_realtime_watch_time_5min",
            "video_realtime_engagement_rate",
            "video_realtime_popular_videos",
            "video_realtime_concurrent_viewers"
        ],
        "course": [
            "course_realtime_active_courses_1min",
            "course_realtime_enrollment_rate",
            "course_realtime_material_access_5min",
            "course_realtime_download_activity",
            "course_realtime_engagement_metrics"
        ],
        "profile_notification": [
            "profile_realtime_updates_1min",
            "notification_realtime_sent_1min",
            "notification_realtime_click_rate_5min",
            "notification_realtime_delivery_status",
            "notification_realtime_engagement_metrics"
        ]
    },
    
    # Alert Thresholds (for monitoring)
    "alerts": {
        "auth": {
            "failed_login_threshold": 10,  # per minute
            "max_concurrent_sessions": 1000
        },
        "assessment": {
            "submission_spike_threshold": 50,  # per minute
            "grading_queue_max": 100
        },
        "video": {
            "concurrent_viewer_max": 500,
            "engagement_rate_min": 0.3  # 30%
        },
        "course": {
            "enrollment_spike_threshold": 20,  # per minute
            "material_access_spike": 100
        }
    }
}


def get_kafka_connection_options(topic_name):
    """Get Kafka connection options for Spark Structured Streaming"""
    return {
        "kafka.bootstrap.servers": speed_config["kafka"]["bootstrap_servers"],
        "subscribe": topic_name,
        "startingOffsets": speed_config["kafka"]["starting_offsets"],
        "maxOffsetsPerTrigger": speed_config["kafka"]["max_offsets_per_trigger"],
        "failOnDataLoss": "false"
    }


def get_output_path(view_name):
    """Get full output path for a real-time view"""
    return f"{speed_config['paths']['output']}/{view_name}"


def get_checkpoint_path(job_name, view_name):
    """Get checkpoint path for a specific streaming query"""
    return f"{speed_config['paths']['checkpoint']}/{job_name}/{view_name}"


def get_spark_config():
    """Get Spark configuration as dict"""
    return {
        "spark.executor.memory": speed_config["spark"]["executor_memory"],
        "spark.executor.cores": str(speed_config["spark"]["executor_cores"]),
        "spark.executor.instances": str(speed_config["spark"]["num_executors"]),
        "spark.driver.memory": speed_config["spark"]["driver_memory"],
        "spark.sql.shuffle.partitions": str(speed_config["spark"]["shuffle_partitions"]),
        "spark.default.parallelism": str(speed_config["spark"]["default_parallelism"]),
        "spark.streaming.checkpoint.interval": speed_config["spark"]["streaming_checkpoint_interval"],
        "spark.hadoop.fs.s3a.endpoint": speed_config["minio"]["endpoint"],
        "spark.hadoop.fs.s3a.access.key": speed_config["minio"]["access_key"],
        "spark.hadoop.fs.s3a.secret.key": speed_config["minio"]["secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    }
