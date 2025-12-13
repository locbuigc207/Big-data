# Batch Layer Quick Start Guide

## Prerequisites

- ✅ Kafka ingestion layer running and writing to MinIO
- ✅ MinIO accessible at `http://minio:9000`
- ✅ PySpark 3.5.0 installed
- ✅ Apache Oozie installed (for production scheduling)

---

## Quick Start (Manual Testing)

### Step 1: Verify Raw Data Exists

```bash
# Check MinIO has ingested data
mc ls minio/bucket-0/master_dataset/
```

You should see partitions like:
```
topic=auth_topic/
topic=assessment_topic/
topic=video_topic/
...
```

### Step 2: Run All Batch Jobs

```bash
cd d:\2025.1\big_data\btl\BTLbigdata-group30

python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
```

**Expected Output**:
```
########################################################################
# MASTER BATCH JOB RUNNER
# Running 5 batch jobs
########################################################################

Starting Batch Job: auth
[AUTH BATCH] Total auth events: 15234
[AUTH BATCH] Computing daily active users...
...
✅ Batch Job 'auth' completed successfully!

...

########################################################################
# BATCH JOB EXECUTION SUMMARY
########################################################################
Job Results:
  auth                      : ✅ SUCCESS
  assessment                : ✅ SUCCESS
  video                     : ✅ SUCCESS
  course                    : ✅ SUCCESS
  profile_notification      : ✅ SUCCESS
```

### Step 3: Verify Batch Views Created

```bash
# List batch views
mc ls minio/bucket-0/batch_views/
```

You should see 33 batch views:
```
auth_daily_active_users/
auth_hourly_login_patterns/
auth_user_session_metrics/
assessment_student_submissions/
video_total_watch_time/
...
```

### Step 4: Query Batch Views

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QueryBatchViews") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read daily active users
dau = spark.read.parquet("s3a://bucket-0/batch_views/auth_daily_active_users")
dau.show()

# Read video engagement
video = spark.read.parquet("s3a://bucket-0/batch_views/video_total_watch_time")
video.orderBy("total_watch_hours", ascending=False).show(10)
```

---

## Production Deployment with Oozie

### Step 1: Install Oozie (if not installed)

```bash
# Download Oozie
wget https://archive.apache.org/dist/oozie/5.2.1/oozie-5.2.1.tar.gz
tar -xzf oozie-5.2.1.tar.gz
cd oozie-5.2.1

# Build Oozie
bin/mkdistro.sh -DskipTests

# Setup database
bin/ooziedb.sh create -sqlfile oozie.sql -run

# Start Oozie server
bin/oozied.sh start
```

Verify Oozie is running:
```bash
curl http://localhost:11000/oozie/v2/admin/status
```

### Step 2: Deploy Batch Layer

**Windows**:
```powershell
cd d:\2025.1\big_data\btl\BTLbigdata-group30\batch_layer
.\deploy_oozie.ps1
```

**Linux/Mac**:
```bash
cd batch_layer
chmod +x deploy_oozie.sh
./deploy_oozie.sh
```

### Step 3: Submit Oozie Coordinator

```bash
cd batch_layer
oozie job -oozie http://localhost:11000/oozie \
    -config oozie/job.properties \
    -run
```

**Output**:
```
job: 0000001-YYYYMMDD-oozie-oozi-C
```

Save this job ID!

### Step 4: Monitor Job

```bash
# Check status
oozie job -oozie http://localhost:11000/oozie -info 0000001-YYYYMMDD-oozie-oozi-C

# View logs
oozie job -oozie http://localhost:11000/oozie -log 0000001-YYYYMMDD-oozie-oozi-C
```

### Step 5: Verify Daily Execution

The coordinator will run daily at midnight UTC. Check next run time:

```bash
oozie job -oozie http://localhost:11000/oozie -info 0000001-YYYYMMDD-oozie-oozi-C | grep "Next Materialized Time"
```

---

## Running Individual Jobs

### Run Only Video Batch Job

```bash
python batch_layer/run_batch_jobs.py video s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
```

### Run Only Assessment Batch Job

```bash
python batch_layer/run_batch_jobs.py assessment s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
```

---

## Configuration

### Change Schedule Frequency

Edit `batch_layer/oozie/coordinator.xml`:

```xml
<!-- Every 6 hours -->
<coordinator-app frequency="${coord:hours(6)}" ...>

<!-- Every hour -->
<coordinator-app frequency="${coord:hours(1)}" ...>

<!-- Weekly -->
<coordinator-app frequency="${coord:days(7)}" ...>
```

### Adjust Spark Resources

Edit `batch_layer/config.py`:

```python
"spark": {
    "executor_memory": "8g",      # Increase for larger datasets
    "executor_cores": 4,           # More cores per executor
    "num_executors": 10,           # More executors
    "shuffle_partitions": 400      # More partitions for larger data
}
```

Or edit `batch_layer/oozie/job.properties`:

```properties
executorMemory=8G
executorCores=4
numExecutors=10
```

### Change MinIO Credentials

Edit `batch_layer/config.py`:

```python
"minio": {
    "endpoint": "http://minio:9000",
    "access_key": "your_access_key",
    "secret_key": "your_secret_key"
}
```

---

## Troubleshooting

### Problem: "No such file or directory: s3a://bucket-0/master_dataset"

**Solution**: Ensure ingestion layer has run first:
```bash
# Check if data exists
mc ls minio/bucket-0/master_dataset/

# If empty, run ingestion
python minio_ingest.py
```

### Problem: "Connection refused: minio:9000"

**Solution**: Check MinIO is running:
```bash
docker ps | grep minio

# If not running, start MinIO
docker-compose up -d minio
```

### Problem: Out of memory errors

**Solution**: Increase executor memory:
```bash
# Edit batch_layer/config.py
"executor_memory": "8g"  # Increase from 4g
```

### Problem: Jobs take too long

**Solution**: Increase parallelism:
```bash
# Edit batch_layer/config.py
"num_executors": 10       # More executors
"shuffle_partitions": 400  # More partitions
```

### Problem: Oozie job stuck in PREP

**Solution**: Check HDFS permissions:
```bash
hdfs dfs -ls -R /user/batch_layer/
hdfs dfs -chmod -R 755 /user/batch_layer/
```

---

## Batch Views Reference

### Authentication (5 views)
1. `auth_daily_active_users` - DAU metrics
2. `auth_hourly_login_patterns` - Peak usage times
3. `auth_user_session_metrics` - Session durations
4. `auth_user_activity_summary` - Overall activity
5. `auth_registration_analytics` - Signup trends

### Assessment (7 views)
1. `assessment_student_submissions` - Submission stats
2. `assessment_engagement_timeline` - Time to submit
3. `assessment_quiz_performance` - Quiz metrics
4. `assessment_grading_stats` - Grade statistics
5. `assessment_teacher_workload` - Teacher analytics
6. `assessment_submission_distribution` - Daily patterns
7. `assessment_student_overall_performance` - Combined metrics

### Video (7 views)
1. `video_total_watch_time` - Cumulative watch time
2. `video_student_engagement` - Per-student metrics
3. `video_popularity` - Most watched
4. `video_daily_engagement` - Daily patterns
5. `video_course_metrics` - Course-level metrics
6. `video_student_course_summary` - Comprehensive view
7. `video_drop_off_indicators` - Engagement issues

### Course (8 views)
1. `course_enrollment_stats` - Enrollment trends
2. `course_material_access` - Access patterns
3. `course_material_popularity` - Popular materials
4. `course_download_analytics` - Download behavior
5. `course_resource_download_stats` - Resource stats
6. `course_activity_summary` - Student activity
7. `course_daily_engagement` - Daily metrics
8. `course_overall_metrics` - Course-level metrics

### Profile & Notification (10 views)
1. `profile_update_frequency` - Update patterns
2. `profile_field_changes` - Field-level stats
3. `profile_avatar_changes` - Avatar tracking
4. `profile_daily_activity` - Daily profile activity
5. `notification_delivery_stats` - Delivery metrics
6. `notification_engagement` - Click patterns
7. `notification_click_through_rate` - CTR
8. `notification_user_preferences` - User preferences
9. `notification_daily_activity` - Daily metrics
10. `notification_user_summary` - Overall engagement

**Total: 37 batch views**

---

## Next Steps

✅ **Batch Layer Complete!**

Now proceed to:
1. **Speed Layer**: Implement real-time stream processing
2. **Serving Layer**: Create unified query interface
3. **Monitoring**: Set up metrics and alerting
4. **Visualization**: Create dashboards (Grafana/Superset)

---

## Support

For issues or questions:
- Check logs: `oozie job -log <job-id>`
- Review Spark UI: `http://<spark-driver>:4040`
- Inspect MinIO: `mc ls minio/bucket-0/batch_views/`
