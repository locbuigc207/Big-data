# Batch Layer - Lambda Architecture

## Overview

The Batch Layer is responsible for precomputing views from the immutable master dataset stored in MinIO. It processes historical data in batch mode using Apache Spark and schedules jobs using Apache Oozie.

### Architecture

```
Raw Events (MinIO) → Batch Jobs (PySpark) → Batch Views (MinIO) → Serving Layer
                           ↑
                      Oozie Scheduler
                      (Daily at midnight)
```

### Key Components

1. **Batch Jobs**: PySpark jobs that process raw events and create precomputed views
2. **Oozie Workflow**: Orchestrates parallel execution of all batch jobs
3. **Oozie Coordinator**: Schedules daily batch processing
4. **MinIO Storage**: Stores both raw events and precomputed batch views

---

## Directory Structure

```
batch_layer/
├── jobs/                           # PySpark batch processing jobs
│   ├── auth_batch_job.py          # Authentication analytics
│   ├── assessment_batch_job.py    # Assignment & quiz analytics
│   ├── video_batch_job.py         # Video engagement analytics
│   ├── course_batch_job.py        # Course interaction analytics
│   └── profile_notification_batch_job.py  # Profile & notification analytics
├── oozie/                          # Oozie workflow definitions
│   ├── workflow.xml               # Workflow definition (parallel jobs)
│   ├── coordinator.xml            # Daily schedule coordinator
│   └── job.properties             # Oozie job configuration
├── config.py                       # Centralized configuration
├── run_batch_jobs.py              # Manual batch job runner
├── deploy_oozie.sh                # Deployment script (Linux)
├── deploy_oozie.ps1               # Deployment script (Windows)
└── README.md                       # This file
```

---

## Batch Jobs

### 1. Auth Batch Job (`auth_batch_job.py`)

**Input**: `s3a://bucket-0/master_dataset/topic=auth_topic`

**Batch Views Created**:
- `auth_daily_active_users` - Daily active users (DAU) and session counts
- `auth_hourly_login_patterns` - Hourly login distribution for peak usage analysis
- `auth_user_session_metrics` - Session duration metrics per user
- `auth_user_activity_summary` - Overall user activity summary
- `auth_registration_analytics` - New user registration trends

### 2. Assessment Batch Job (`assessment_batch_job.py`)

**Input**: `s3a://bucket-0/master_dataset/topic=assessment_topic`

**Batch Views Created**:
- `assessment_student_submissions` - Submission statistics per student
- `assessment_engagement_timeline` - Time from viewing to submitting assignments
- `assessment_quiz_performance` - Quiz performance metrics
- `assessment_grading_stats` - Grading statistics (student perspective)
- `assessment_teacher_workload` - Teacher grading workload analytics
- `assessment_submission_distribution` - Daily submission patterns
- `assessment_student_overall_performance` - Combined quiz + assignment performance

### 3. Video Batch Job (`video_batch_job.py`)

**Input**: `s3a://bucket-0/master_dataset/topic=video_topic`

**Batch Views Created**:
- `video_total_watch_time` - Cumulative watch time per student per video
- `video_student_engagement` - Overall video engagement per student per course
- `video_popularity` - Most-watched videos
- `video_daily_engagement` - Daily video watching patterns
- `video_course_metrics` - Course-level video metrics
- `video_student_course_summary` - Student x course x video comprehensive summary
- `video_drop_off_indicators` - Videos with low engagement (potential issues)

### 4. Course Batch Job (`course_batch_job.py`)

**Input**: `s3a://bucket-0/master_dataset/topic=course_topic`

**Batch Views Created**:
- `course_enrollment_stats` - Enrollment trends
- `course_material_access` - Material access patterns per student
- `course_material_popularity` - Most accessed materials
- `course_download_analytics` - Download behavior per student
- `course_resource_download_stats` - Download statistics per resource
- `course_activity_summary` - Comprehensive course activity per student
- `course_daily_engagement` - Daily course engagement metrics
- `course_overall_metrics` - Overall course-level metrics

### 5. Profile & Notification Batch Job (`profile_notification_batch_job.py`)

**Input**: 
- `s3a://bucket-0/master_dataset/topic=profile_topic`
- `s3a://bucket-0/master_dataset/topic=notification_topic`

**Batch Views Created**:
- `profile_update_frequency` - Profile update patterns
- `profile_field_changes` - Which fields are updated most
- `profile_avatar_changes` - Avatar change tracking
- `profile_daily_activity` - Daily profile management activity
- `notification_delivery_stats` - Notification delivery statistics
- `notification_engagement` - Notification click patterns
- `notification_click_through_rate` - CTR per notification type
- `notification_user_preferences` - User engagement with different notification types
- `notification_daily_activity` - Daily notification metrics
- `notification_user_summary` - Overall notification engagement per user

---

## Running Batch Jobs

### Option 1: Manual Execution (Development/Testing)

Run all batch jobs:
```bash
python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
```

Run a specific batch job:
```bash
python batch_layer/run_batch_jobs.py video s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
```

### Option 2: Oozie Orchestration (Production)

#### Step 1: Deploy to Oozie

**Linux/Mac**:
```bash
cd batch_layer
chmod +x deploy_oozie.sh
./deploy_oozie.sh
```

**Windows**:
```powershell
cd batch_layer
.\deploy_oozie.ps1
```

#### Step 2: Submit Oozie Coordinator

```bash
oozie job -oozie http://localhost:11000/oozie \
    -config batch_layer/oozie/job.properties \
    -run
```

#### Step 3: Monitor Job

Check job status:
```bash
oozie job -oozie http://localhost:11000/oozie -info <job-id>
```

View job logs:
```bash
oozie job -oozie http://localhost:11000/oozie -log <job-id>
```

List all jobs:
```bash
oozie jobs -oozie http://localhost:11000/oozie
```

#### Step 4: Manage Coordinator

Kill coordinator:
```bash
oozie job -oozie http://localhost:11000/oozie -kill <coordinator-id>
```

Suspend coordinator:
```bash
oozie job -oozie http://localhost:11000/oozie -suspend <coordinator-id>
```

Resume coordinator:
```bash
oozie job -oozie http://localhost:11000/oozie -resume <coordinator-id>
```

---

## Configuration

### MinIO Configuration (`config.py`)

```python
"minio": {
    "endpoint": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "ssl_enabled": False,
    "path_style_access": True
}
```

### Data Paths

- **Input**: `s3a://bucket-0/master_dataset` (raw events from Kafka ingestion)
- **Output**: `s3a://bucket-0/batch_views` (precomputed batch views)
- **Checkpoint**: `s3a://bucket-0/checkpoints/batch` (job checkpoints)

### Spark Configuration

```python
"spark": {
    "executor_memory": "4g",
    "executor_cores": 2,
    "num_executors": 3,
    "driver_memory": "2g",
    "shuffle_partitions": 200,
    "default_parallelism": 100
}
```

Adjust these based on your cluster resources.

### Schedule Configuration

**Default**: Daily at midnight UTC

To change frequency, edit `batch_layer/oozie/coordinator.xml`:
```xml
<coordinator-app frequency="${coord:hours(24)}" ...>
```

Options:
- Hourly: `${coord:hours(1)}`
- Every 6 hours: `${coord:hours(6)}`
- Daily: `${coord:hours(24)}`
- Weekly: `${coord:days(7)}`

---

## Oozie Workflow Execution

The Oozie workflow uses a **fork-join** pattern to run all batch jobs in parallel:

```
START
  ↓
FORK
  ├─→ auth_batch_job ────────────┐
  ├─→ assessment_batch_job ──────┤
  ├─→ video_batch_job ───────────┼─→ JOIN → END
  ├─→ course_batch_job ──────────┤
  └─→ profile_notification_batch ┘
```

This maximizes resource utilization and minimizes total processing time.

---

## Data Flow

### Input Format (Parquet partitioned by topic)

```
s3a://bucket-0/master_dataset/
└── topic=auth_topic/
    ├── part-00000.parquet
    ├── part-00001.parquet
    └── ...
```

Each Parquet file contains:
```
{
  "event_category": "AUTH",
  "event_type": "LOGIN",
  "user_id": "SV001",
  "timestamp": "2025-12-10 14:30:00",
  ...
}
```

### Output Format (Parquet batch views)

```
s3a://bucket-0/batch_views/
├── auth_daily_active_users/
│   └── part-00000.parquet
├── video_total_watch_time/
│   └── part-00000.parquet
└── ...
```

---

## Performance Optimization

### 1. Partitioning

Input data is partitioned by topic for efficient filtering:
```python
df = spark.read.parquet(f"{input_path}/topic=auth_topic")
```

### 2. Broadcast Joins

For small lookup tables, use broadcast joins:
```python
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

### 3. Caching

Cache DataFrames used multiple times:
```python
df.cache()
# Use df multiple times
df.unpersist()
```

### 4. Repartitioning

Adjust partitions for optimal parallelism:
```python
df = df.repartition(100)  # Increase parallelism
df = df.coalesce(1)        # Reduce output files
```

---

## Monitoring & Debugging

### View Spark UI

Access Spark UI at: `http://<spark-driver>:4040`

### Check MinIO Output

```bash
# Using MinIO client (mc)
mc ls minio/bucket-0/batch_views/

# Or AWS CLI
aws --endpoint-url http://localhost:9000 s3 ls s3://bucket-0/batch_views/
```

### View Batch Views

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ViewBatchView").getOrCreate()

# Read a batch view
df = spark.read.parquet("s3a://bucket-0/batch_views/video_total_watch_time")
df.show()
df.printSchema()
```

### Common Issues

**Issue**: `java.io.FileNotFoundException: s3a://bucket-0/master_dataset`
- **Fix**: Ensure ingestion layer has run and created data in MinIO

**Issue**: `AccessDeniedException: s3a://bucket-0`
- **Fix**: Check MinIO credentials in `config.py`

**Issue**: Oozie job stuck in PREP state
- **Fix**: Check HDFS paths and permissions

**Issue**: Out of memory errors
- **Fix**: Increase `executor_memory` in `config.py`

---

## Testing

### 1. Test Individual Job Locally

```bash
# Run video batch job
python batch_layer/jobs/video_batch_job.py \
    s3a://bucket-0/master_dataset \
    s3a://bucket-0/batch_views_test
```

### 2. Test All Jobs

```bash
python batch_layer/run_batch_jobs.py \
    s3a://bucket-0/master_dataset \
    s3a://bucket-0/batch_views_test
```

### 3. Validate Output

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ValidateBatchViews").getOrCreate()

# Check video watch time
video_df = spark.read.parquet("s3a://bucket-0/batch_views_test/video_total_watch_time")
print(f"Total records: {video_df.count()}")
video_df.show(10)

# Check for nulls
video_df.select([count(when(col(c).isNull(), c)).alias(c) for c in video_df.columns]).show()
```

---

## Maintenance

### Reprocessing Historical Data

To reprocess data for a specific date range:

1. Modify input path to specific partition:
```python
input_path = "s3a://bucket-0/master_dataset/year=2025/month=12/day=10"
```

2. Run batch job:
```bash
python batch_layer/run_batch_jobs.py <modified_input_path> s3a://bucket-0/batch_views
```

### Cleaning Old Batch Views

```bash
# Delete old batch views
aws --endpoint-url http://localhost:9000 s3 rm s3://bucket-0/batch_views/ --recursive
```

### Updating Batch Jobs

1. Modify Python job files
2. Redeploy to Oozie:
```bash
./batch_layer/deploy_oozie.sh
```
3. Kill and resubmit coordinator

---

## Integration with Serving Layer

The Serving Layer reads precomputed batch views:

```python
# In serving_layer.py
batch_video_df = spark.read.parquet("s3a://bucket-0/batch_views/video_total_watch_time")

# Merge with speed layer real-time views
result = batch_video_df.union(speed_video_df)
```

---

## Next Steps

1. **Speed Layer**: Implement real-time processing with Spark Streaming
2. **Serving Layer**: Create unified query interface combining batch + speed views
3. **Monitoring**: Set up Grafana dashboards for batch job metrics
4. **Alerting**: Configure Oozie notifications for job failures

---

## References

- [Apache Oozie Documentation](https://oozie.apache.org/docs/5.2.1/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Lambda Architecture](http://lambda-architecture.net/)
