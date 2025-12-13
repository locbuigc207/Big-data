# Speed Layer Quick Start Guide

Get the speed layer running in 5 minutes!

## Prerequisites

✅ Kafka cluster running with 6 topics
✅ MinIO accessible at http://minio:9000
✅ PySpark 3.5.0 installed
✅ Required Spark packages available

## Quick Start Steps

### 1. Verify Configuration

Check [config.py](config.py) settings:

```python
# Kafka broker
"bootstrap_servers": "kafka:29092"  # Change to "localhost:9092" for local testing

# MinIO endpoint
"endpoint": "http://minio:9000"
```

### 2. Start All Streaming Jobs

```bash
cd speed_layer
python run_stream_jobs.py
```

This will start all 5 streaming jobs:
- Auth streaming (5 real-time views)
- Assessment streaming (5 real-time views)
- Video streaming (5 real-time views)
- Course streaming (5 real-time views)
- Profile/Notification streaming (5 real-time views)

### 3. Start Specific Jobs Only

```bash
# Start only auth and video streaming
python run_stream_jobs.py auth video
```

### 4. Monitor Jobs

The runner automatically displays:
- Job start confirmation with PID
- Running job count every 10 seconds
- Job termination alerts

Press `Ctrl+C` to stop all jobs gracefully.

### 5. Verify Output

Check MinIO for real-time views:

```bash
# Using AWS CLI with MinIO
aws s3 ls s3://bucket-0/realtime_views/ --endpoint-url http://localhost:9000

# Or use MinIO console
# http://localhost:9001
```

You should see directories like:
- `auth_realtime_active_users_1min/`
- `assessment_realtime_submissions_1min/`
- `video_realtime_viewers_1min/`
- etc.

## Production Deployment

### Using Spark Submit

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name SpeedLayer_All \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  run_stream_jobs.py
```

### Individual Job Submission

```bash
# Submit auth streaming job
spark-submit \
  --master yarn \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.executor.memory=2g \
  jobs/auth_stream_job.py
```

## Testing

### 1. Generate Test Events

Start the producer to generate events:

```bash
cd ../ingestion_layer
python producer.py
```

### 2. Check Real-Time Views

After 1-2 minutes, query the real-time views:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("QueryRealtimeViews").getOrCreate()

# Read active users view
df = spark.read.parquet("s3a://bucket-0/realtime_views/auth_realtime_active_users_1min")
df.show()
```

### 3. Monitor Streaming UI

Access Spark Streaming UI at:
```
http://localhost:4040/StreamingQuery/
```

## Configuration for Different Scenarios

### Local Development

```python
# In config.py
"kafka": {
    "bootstrap_servers": "localhost:9092"
},
"spark": {
    "executor_memory": "1g",
    "num_executors": 1
}
```

### Production

```python
# In config.py
"kafka": {
    "bootstrap_servers": "kafka1:9092,kafka2:9092,kafka3:9092"
},
"spark": {
    "executor_memory": "4g",
    "num_executors": 5
}
```

### High Throughput

```python
# In config.py
"kafka": {
    "max_offsets_per_trigger": 50000
},
"output": {
    "trigger_interval": "10 seconds"
},
"spark": {
    "shuffle_partitions": 100
}
```

### Low Latency

```python
# In config.py
"windows": {
    "micro": "30 seconds",
    "short": "2 minutes"
},
"output": {
    "trigger_interval": "5 seconds"
}
```

## Troubleshooting

### Kafka Connection Failed

```bash
# Test Kafka connectivity
telnet kafka 29092

# Or use kafkacat
kafkacat -b kafka:29092 -L
```

**Solution**: Update bootstrap_servers in config.py

### MinIO Access Denied

```bash
# Test MinIO credentials
aws s3 ls s3://bucket-0/ --endpoint-url http://localhost:9000
```

**Solution**: Verify access_key/secret_key in config.py

### Out of Memory

```bash
# Check executor memory usage in Spark UI
# Increase memory in config.py
"executor_memory": "4g"
"driver_memory": "2g"
```

### Checkpointing Errors

```bash
# Clear old checkpoints
aws s3 rm s3://bucket-0/checkpoints/speed/ --recursive --endpoint-url http://localhost:9000
```

## Common Commands

```bash
# List available jobs
python run_stream_jobs.py --list

# Run all jobs
python run_stream_jobs.py

# Run specific jobs
python run_stream_jobs.py auth assessment

# Run single job directly
python jobs/video_stream_job.py

# Check Spark streaming jobs
spark-submit --status <app-id>
```

## What's Next?

After speed layer is running:

1. **Implement Serving Layer** - Merge batch + real-time views
2. **Add Monitoring** - Set up alerts for thresholds
3. **Performance Tuning** - Optimize based on load
4. **Scale Out** - Add more executors for higher throughput

## Real-Time Views Generated

25 views across 5 categories:
- **Auth**: 5 views (active users, login rate, sessions, failed logins)
- **Assessment**: 5 views (submissions, active students, quizzes, grading queue)
- **Video**: 5 views (viewers, watch time, engagement, popularity)
- **Course**: 5 views (enrollments, material access, downloads, engagement)
- **Profile/Notification**: 5 views (updates, sent, click rate, delivery, engagement)

Each view updates every 30 seconds with 1-5 minute windows!

## Support

- Check [README.md](README.md) for detailed documentation
- Review job logs for errors
- Monitor Spark UI for streaming metrics
- Verify Kafka topic has messages: `kafka-console-consumer --bootstrap-server kafka:29092 --topic auth_topic`
