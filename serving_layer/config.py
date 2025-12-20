serving_config = {
    # Spark Configuration for Reading Views
    "spark": {
        "app_name": "ServingLayer_QueryService",
        "executor_memory": "2g",
        "executor_cores": 2,
        "driver_memory": "2g"
    },
    
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
        "batch_views": "s3a://bucket-0/batch_views",
        "realtime_views": "s3a://bucket-0/realtime_views"
    },
    
    # Cache Configuration
    "cache": {
        "enabled": True,
        "ttl_seconds": 300,  # 5 minutes
        "max_size": 1000
    },
    
    # Query Service Configuration
    "api": {
        "host": "0.0.0.0",
        "port": 8000,
        "reload": True,
        "log_level": "info"
    },
    
    # Time Windows for Merging Batch + Realtime
    "merge": {
        "realtime_window_minutes": 60,  # Last 60 minutes from realtime
        "batch_cutoff_hours": 1  # Batch data up to 1 hour ago
    },
    
    # Available Views (Batch + Realtime)
    "views": {
        "auth": {
            "batch": [
                "auth_daily_active_users",
                "auth_hourly_login_patterns",
                "auth_user_session_metrics",
                "auth_user_activity_summary",
                "auth_registration_analytics"
            ],
            "realtime": [
                "auth_realtime_active_users_1min",
                "auth_realtime_active_users_5min",
                "auth_realtime_login_rate",
                "auth_realtime_session_activity",
                "auth_realtime_failed_logins"
            ]
        },
        "assessment": {
            "batch": [
                "assessment_student_submissions",
                "assessment_engagement_timeline",
                "assessment_quiz_performance",
                "assessment_grading_stats",
                "assessment_teacher_workload",
                "assessment_submission_distribution",
                "assessment_student_overall_performance"
            ],
            "realtime": [
                "assessment_realtime_submissions_1min",
                "assessment_realtime_active_students",
                "assessment_realtime_quiz_attempts",
                "assessment_realtime_grading_queue",
                "assessment_realtime_submission_rate"
            ]
        },
        "video": {
            "batch": [
                "video_total_watch_time",
                "video_student_engagement",
                "video_popularity",
                "video_daily_engagement",
                "video_course_metrics",
                "video_student_course_summary",
                "video_drop_off_indicators"
            ],
            "realtime": [
                "video_realtime_viewers_1min",
                "video_realtime_watch_time_5min",
                "video_realtime_engagement_rate",
                "video_realtime_popular_videos",
                "video_realtime_concurrent_viewers"
            ]
        },
        "course": {
            "batch": [
                "course_enrollment_stats",
                "course_material_access",
                "course_material_popularity",
                "course_download_analytics",
                "course_resource_download_stats",
                "course_activity_summary",
                "course_daily_engagement",
                "course_overall_metrics"
            ],
            "realtime": [
                "course_realtime_active_courses_1min",
                "course_realtime_enrollment_rate",
                "course_realtime_material_access_5min",
                "course_realtime_download_activity",
                "course_realtime_engagement_metrics"
            ]
        },
        "profile_notification": {
            "batch": [
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
            ],
            "realtime": [
                "profile_realtime_updates_1min",
                "notification_realtime_sent_1min",
                "notification_realtime_click_rate_5min",
                "notification_realtime_delivery_status",
                "notification_realtime_engagement_metrics"
            ]
        }
    }
}


def get_spark_config():
    """Get Spark configuration for reading views"""
    return {
        "spark.executor.memory": serving_config["spark"]["executor_memory"],
        "spark.executor.cores": str(serving_config["spark"]["executor_cores"]),
        "spark.driver.memory": serving_config["spark"]["driver_memory"],
        "spark.hadoop.fs.s3a.endpoint": serving_config["minio"]["endpoint"],
        "spark.hadoop.fs.s3a.access.key": serving_config["minio"]["access_key"],
        "spark.hadoop.fs.s3a.secret.key": serving_config["minio"]["secret_key"],
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
    }


def get_batch_view_path(view_name):
    """Get full path to batch view"""
    return f"{serving_config['paths']['batch_views']}/{view_name}"


def get_realtime_view_path(view_name):
    """Get full path to realtime view"""
    return f"{serving_config['paths']['realtime_views']}/{view_name}"