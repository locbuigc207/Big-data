# Ingestion Layer (Kafka → MinIO)

## Overview

The ingestion layer captures raw events from Kafka topics and stores them immutably in MinIO (S3-compatible storage) for batch processing.

## Components

### 1. Event Producer (`producer.py`)
- Generates simulated student learning events
- Publishes to 6 Kafka topics:
  - `auth_topic` - Login/logout/signup events
  - `assessment_topic` - Assignment and quiz events
  - `video_topic` - Video watching events
  - `course_topic` - Course interaction events
  - `profile_topic` - Profile management events
  - `notification_topic` - Notification events

**Usage**:
```bash
python ingestion_layer/producer.py
```

### 2. HDFS Ingest (`ingest_layer.py`)
- Streams events from Kafka to HDFS
- For local development with Docker Compose
- Partitions data by topic

**Usage**:
```bash
python ingestion_layer/ingest_layer.py
```

### 3. MinIO Ingest (`minio_ingest.py`)
- Streams events from Kafka to MinIO (S3-compatible)
- For production deployment
- Partitions data by topic

**Usage**:
```bash
python ingestion_layer/minio_ingest.py
```

### 4. Kubernetes MinIO Ingest (`minio_ingest_k8s.py`)
- Optimized for Kubernetes deployment
- Uses environment variables for configuration
- Designed for Minikube/K8s clusters

**Usage**:
```bash
python ingestion_layer/minio_ingest_k8s.py
```

## Data Flow

```
Producer → Kafka Topics → Ingest Script → MinIO Storage
                                            ↓
                                     s3a://bucket-0/master_dataset/
                                     ├── topic=auth_topic/
                                     ├── topic=assessment_topic/
                                     ├── topic=video_topic/
                                     ├── topic=course_topic/
                                     ├── topic=profile_topic/
                                     └── topic=notification_topic/
```

## Configuration

See event schema: [docs/event-schema-specification.md](../docs/event-schema-specification.md)

## Docker Build

```bash
docker build -f ingestion_layer/Dockerfile.ingestion -t ingestion:latest .
```

## Next Steps

After ingestion, data flows to:
- **Batch Layer**: Processes historical data → precomputed views
- **Speed Layer**: Processes real-time streams → incremental updates
