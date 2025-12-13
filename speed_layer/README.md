# Speed Layer - Real-Time Stream Processing

## Overview

The Speed Layer processes events in real-time from Kafka using Spark Structured Streaming to generate low-latency views that complement the batch layer's precomputed views.

## Architecture

```
Kafka Topics → Spark Structured Streaming → Real-Time Views (MinIO)
     ↓                                              ↓
6 Event Topics              25 Real-Time Views (1min, 5min windows)
```

## Components

### Streaming Jobs (5 Jobs)

1. **auth_stream_job.py** - Authentication events
   - Real-time active users (1min, 5min windows)
   - Login rate tracking
   - Session activity monitoring
   - Failed login detection

2. **assessment_stream_job.py** - Assessment/assignment events
   - Submission rate monitoring
   - Active student tracking
   - Quiz attempt analytics
   - Grading queue monitoring

3. **video_stream_job.py** - Video interaction events
   - Active viewer counts
   - Watch time aggregation
   - Engagement rate calculation
   - Concurrent viewer tracking

4. **course_stream_job.py** - Course interaction events
   - Active course tracking
   - Enrollment rate monitoring
   - Material access patterns
   - Download activity tracking

5. **profile_notification_stream_job.py** - Profile & notification events
   - Profile update monitoring
   - Notification delivery tracking
   - Click-through rate calculation
   - Engagement metrics

### Real-Time Views (25 Views)

#### Authentication (5 views)
- `auth_realtime_active_users_1min` - Active users per minute
- `auth_realtime_active_users_5min` - Active users per 5 minutes
- `auth_realtime_login_rate` - Login success/failure rate
- `auth_realtime_session_activity` - Session activity metrics
- `auth_realtime_failed_logins` - Failed login attempts

#### Assessment (5 views)
- `assessment_realtime_submissions_1min` - Submissions per minute
- `assessment_realtime_active_students` - Students working on assessments
- `assessment_realtime_quiz_attempts` - Quiz attempt tracking
- `assessment_realtime_grading_queue` - Pending grading items
- `assessment_realtime_submission_rate` - Submission trends

#### Video (5 views)
- `video_realtime_viewers_1min` - Viewers per minute
- `video_realtime_watch_time_5min` - Watch time aggregation
- `video_realtime_engagement_rate` - Engagement metrics
- `video_realtime_popular_videos` - Popular video ranking
- `video_realtime_concurrent_viewers` - Concurrent viewer counts

#### Course (5 views)
- `course_realtime_active_courses_1min` - Active courses per minute
- `course_realtime_enrollment_rate` - Enrollment tracking
- `course_realtime_material_access_5min` - Material access patterns
- `course_realtime_download_activity` - Download monitoring
- `course_realtime_engagement_metrics` - Overall engagement

#### Profile & Notification (5 views)
- `profile_realtime_updates_1min` - Profile updates per minute
- `notification_realtime_sent_1min` - Notifications sent per minute
- `notification_realtime_click_rate_5min` - Click-through rate
- `notification_realtime_delivery_status` - Delivery monitoring
- `notification_realtime_engagement_metrics` - User engagement

## Configuration

All configuration is centralized in [config.py](config.py):

```python
speed_config = {
    "kafka": {
        "bootstrap_servers": "kafka:29092",
        "topics": {...},
        "starting_offsets": "latest"
    },
    "windows": {
        "micro": "1 minute",
        "short": "5 minutes",
        "watermark": "2 minutes"
    },
    "paths": {
        "output": "s3a://bucket-0/realtime_views",
        "checkpoint": "s3a://bucket-0/checkpoints/speed"
    }
}
```

### Key Configuration Options

- **Windows**: 1-minute and 5-minute tumbling/sliding windows
- **Watermark**: 2-minute late data tolerance
- **Trigger Interval**: 30 seconds between micro-batches
- **Output Mode**: Append mode for streaming aggregations

## Usage

### Run All Streaming Jobs

```bash
python run_stream_jobs.py
```

### Run Specific Job

```bash
# Run single job
python run_stream_jobs.py auth

# Run multiple jobs
python run_stream_jobs.py auth video course
```

### List Available Jobs

```bash
python run_stream_jobs.py --list
```

### Individual Job Execution

```bash
# Auth streaming
python jobs/auth_stream_job.py

# Assessment streaming
python jobs/assessment_stream_job.py

# Video streaming
python jobs/video_stream_job.py

# Course streaming
python jobs/course_stream_job.py

# Profile/Notification streaming
python jobs/profile_notification_stream_job.py
```

## Spark Submit (Production)

For production deployment, use spark-submit:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name SpeedLayer_Auth \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=2 \
  jobs/auth_stream_job.py
```

## Stream Processing Features

### Windowing

- **Tumbling Windows**: Non-overlapping fixed-size windows (1min, 5min)
- **Sliding Windows**: Overlapping windows for trend analysis
- **Watermarking**: 2-minute tolerance for late-arriving events

### Stateful Processing

- Session tracking with state management
- Rate calculation with windowed aggregations
- Join operations for queue monitoring

### Fault Tolerance

- Checkpointing to MinIO for exactly-once semantics
- Automatic recovery from failures
- State store for stateful operations

## Output Structure

Real-time views are written to MinIO:

```
s3a://bucket-0/realtime_views/
├── auth_realtime_active_users_1min/
│   └── part-*.parquet
├── assessment_realtime_submissions_1min/
│   └── part-*.parquet
├── video_realtime_viewers_1min/
│   └── part-*.parquet
└── ...
```

Checkpoint locations:

```
s3a://bucket-0/checkpoints/speed/
├── auth_stream/
│   ├── auth_realtime_active_users_1min/
│   └── ...
└── assessment_stream/
    └── ...
```

## Monitoring

### Check Job Status

```bash
# View running Spark streaming jobs
spark-submit --status <application-id>

# Check Spark UI
# http://localhost:4040
```

### View Streaming Metrics

The runner script provides real-time monitoring:
- Job health status
- Number of running jobs
- Process IDs for each job

### Common Issues

1. **Kafka Connection Errors**
   - Verify Kafka broker address in config.py
   - Check network connectivity to Kafka

2. **Checkpoint Errors**
   - Ensure MinIO is accessible
   - Verify checkpoint paths exist

3. **Memory Issues**
   - Adjust executor/driver memory in config.py
   - Reduce shuffle.partitions for smaller datasets

## Integration with Lambda Architecture

The Speed Layer:
1. **Complements Batch Layer**: Provides recent data (last few minutes/hours)
2. **Low Latency**: Sub-minute to 5-minute windows
3. **Eventually Consistent**: Batch layer will recompute and fix any issues

The Serving Layer merges:
- **Batch Views**: Historical, accurate, recomputed daily
- **Real-Time Views**: Recent, approximate, updated every 30 seconds

Query pattern:
```
Result = BatchView(t=0 to t=now-1hour) + RealtimeView(t=now-1hour to t=now)
```

## Performance Tuning

### For High Throughput

```python
speed_config["spark"]["shuffle_partitions"] = 100
speed_config["output"]["trigger_interval"] = "10 seconds"
speed_config["kafka"]["max_offsets_per_trigger"] = 50000
```

### For Low Latency

```python
speed_config["spark"]["shuffle_partitions"] = 20
speed_config["output"]["trigger_interval"] = "5 seconds"
speed_config["windows"]["micro"] = "30 seconds"
```

## Dependencies

- PySpark 3.5.0
- Kafka 0.10+ (via spark-sql-kafka package)
- Hadoop AWS 3.3.4 (for MinIO/S3 support)
- Python 3.8+

## Next Steps

1. **Deploy to Production**: Use spark-submit with YARN
2. **Implement Serving Layer**: Merge batch and real-time views
3. **Add Alerting**: Monitor thresholds defined in config
4. **Scale Up**: Adjust executor counts and memory based on load
