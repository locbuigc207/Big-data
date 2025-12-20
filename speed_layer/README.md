# Speed Layer

The Speed Layer processes real-time events from Kafka using Spark Structured Streaming.
It computes low-latency metrics that complement the Batch Layer.

## Components

- **`stream_layer.py`**: Main Spark Streaming application.
- **`Dockerfile`**: Container definition for K8s.
- **`deployment.yaml`**: Kubernetes deployment manifest.

## Implemented Views

The current implementation provides 3 key real-time metrics:
1. **Real-time Active Users**: Unique users active in the last minute.
2. **Course Popularity**: Interaction counts per course (1-minute window).
3. **Video Engagement**: Video view counts (1-minute window).

## Output

Data is written to MinIO in Parquet format:
- `s3a://bucket-0/speed_views/active_users`
- `s3a://bucket-0/speed_views/course_popularity`
- `s3a://bucket-0/speed_views/video_engagement`

Checkpoints are stored in `s3a://bucket-0/checkpoints/`.

## Running Locally

```bash
# Set env vars if not default
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export MINIO_ENDPOINT=http://localhost:9000

python speed_layer/stream_layer.py
```

## Running on Kubernetes

```bash
kubectl apply -f speed_layer/deployment.yaml
```
