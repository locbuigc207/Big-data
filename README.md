# BTLbigdata-group30 - University Learning Analytics System

Lambda Architecture implementation for student learning analytics with Kafka, Spark, and MinIO.

---

## 📁 Project Structure (Reorganized)

```
BTLbigdata-group30/
├── ingestion_layer/                # Kafka → Storage Ingestion
│   ├── producer.py                # Event generator (6 categories)
│   ├── ingest_layer.py           # Kafka → HDFS ingestion (local)
│   ├── minio_ingest.py           # Kafka → MinIO ingestion
│   ├── minio_ingest_k8s.py       # Kafka → MinIO (Kubernetes)
│   ├── Dockerfile.ingestion       # Docker image for ingestion
│   └── README.md                  # Ingestion layer documentation
│
├── batch_layer/                    # Batch Processing Layer ✅
│   ├── jobs/                      # PySpark batch jobs (5 jobs)
│   │   ├── auth_batch_job.py     # Authentication analytics
│   │   ├── assessment_batch_job.py    # Assessment analytics
│   │   ├── video_batch_job.py    # Video engagement analytics
│   │   ├── course_batch_job.py   # Course interaction analytics
│   │   └── profile_notification_batch_job.py
│   ├── oozie/                     # Oozie orchestration
│   │   ├── workflow.xml          # Parallel job workflow
│   │   ├── coordinator.xml       # Daily scheduler
│   │   └── job.properties        # Oozie configuration
│   ├── config.py                  # Centralized config
│   ├── run_batch_jobs.py         # Manual runner
│   ├── deploy_oozie.sh/.ps1      # Deployment scripts
│   ├── README.md                  # Complete documentation
│   ├── QUICKSTART.md              # Quick start guide
│   └── IMPLEMENTATION_SUMMARY.md  # Implementation summary
│
├── speed_layer/                    # Real-Time Stream Processing ⏳
│   ├── stream_layer.py           # Real-time processor (TBD)
│   └── README.md                  # Speed layer documentation
│
├── serving_layer/                  # Unified Query Interface ⏳
│   ├── serving_layer.py          # Query service (TBD)
│   └── README.md                  # Serving layer documentation
│
├── kafka/                          # Kafka Kubernetes configs
│   ├── deployment.yaml            # Kafka cluster (3 nodes)
│   ├── topics.yaml                # Topic definitions (6 topics)
│   ├── storage-class.yaml         # Storage configuration
│   ├── persistent-volumn.yaml     # Multi-node PVs
│   └── persistent-volumn-minikube.yaml
│
├── minio/                          # MinIO deployment
│   └── deployment.yaml            # S3-compatible storage
│
├── spark/                          # Spark jobs
│   └── ingestion-job.yaml         # Ingestion K8s job
│
├── deployment/                     # Deployment scripts & guides
│   ├── deploy_minikube.ps1       # Auto-deploy to Minikube
│   ├── cleanup_minikube.ps1      # Cleanup Minikube
│   ├── MINIKUBE_TESTING_GUIDE.md # Complete testing guide
│   ├── MINIKUBE_QUICK_REFERENCE.md
│   ├── TESTING_COMPARISON.md     # Local vs Minikube
│   └── TESTING_WORKFLOW.md       # Testing workflows
│
├── config/                         # Configuration files
│   ├── docker-compose.yml         # Local development setup
│   ├── hadoop.env                 # Hadoop configuration
│   └── requirements.txt           # Python dependencies
│
├── docs/                           # Documentation
│   ├── event-schema-specification.md  # Event schemas
│   ├── architecture-design.md     # System architecture
│   ├── problem-definition.md      # Project requirements
│   └── deployment-guide.md        # Setup instructions
│
├── batch_layer.py                  # DEPRECATED (moved to batch_layer/)
├── generate_fake_data.ipynb       # Data generation notebook
└── README.md                       # This file
```

---

## 🏗️ Lambda Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                                │
│            (Student Learning Events - 6 Categories)              │
└───────────────────────────┬─────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                   INGESTION LAYER (Kafka)                        │
│  Producer → Kafka Topics → MinIO (Raw Events)                    │
└────────┬────────────────────────────────────┬───────────────────┘
         ↓                                     ↓
┌────────────────────────┐        ┌───────────────────────────────┐
│   BATCH LAYER ✅       │        │   SPEED LAYER ⏳              │
│   (Historical Data)    │        │   (Real-Time Data)            │
│                        │        │                               │
│ • Oozie Scheduler      │        │ • Spark Streaming             │
│ • 5 PySpark Jobs       │        │ • Windowed Aggregations       │
│ • 37 Batch Views       │        │ • Incremental Updates         │
│ • Daily Processing     │        │ • Low Latency (seconds)       │
└────────┬───────────────┘        └───────────┬───────────────────┘
         │                                     │
         └──────────────┬──────────────────────┘
                        ↓
         ┌──────────────────────────────────┐
         │     SERVING LAYER ⏳             │
         │  (Unified Query Interface)       │
         │                                  │
         │  Batch Views + Speed Views       │
         │  REST API / Query Service        │
         └──────────────────────────────────┘
```

**Legend**:
- ✅ **Complete** - Fully implemented and tested
- ⏳ **Planned** - Next implementation phase

---

## 🎯 Implementation Status

| Layer | Status | Components | Batch Views |
|-------|--------|------------|-------------|
| **Ingestion** | ✅ Complete | 4 scripts, 6 Kafka topics | - |
| **Batch Layer** | ✅ Complete | 5 PySpark jobs, Oozie orchestration | 37 views |
| **Speed Layer** | ⏳ Planned | Real-time stream processing | TBD |
| **Serving Layer** | ⏳ Planned | Query API, view merger | TBD |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8-3.11
- Docker Desktop
- Apache Spark 3.5.0
- Kafka (via Docker Compose or Minikube)
- MinIO (via Docker Compose or Minikube)

### Option 1: Local Development (Docker Compose)

1. **Start infrastructure**:
```powershell
cd config
docker-compose up -d
```

2. **Generate events**:
```powershell
python ingestion_layer/producer.py
```

3. **Ingest to MinIO**:
```powershell
python ingestion_layer/minio_ingest.py
```

4. **Run batch processing**:
```powershell
python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
```

### Option 2: Minikube (Kubernetes)

1. **Deploy to Minikube**:
```powershell
cd deployment
.\deploy_minikube.ps1
```

2. **Port forward services**:
```powershell
# Terminal 1: Kafka
kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka

# Terminal 2: MinIO
kubectl port-forward service/minio 9000:9000 -n minio
```

3. **Run ingestion**:
```powershell
python ingestion_layer/producer.py
python ingestion_layer/minio_ingest_k8s.py
```

4. **Deploy Oozie batch jobs**:
```powershell
cd batch_layer
.\deploy_oozie.ps1
oozie job -oozie http://localhost:11000/oozie -config oozie/job.properties -run
```

**📚 Detailed Guides**:
- [Ingestion Layer README](ingestion_layer/README.md)
- [Batch Layer README](batch_layer/README.md)
- [Batch Layer Quick Start](batch_layer/QUICKSTART.md)
- [Minikube Testing Guide](deployment/MINIKUBE_TESTING_GUIDE.md)

---

## 📊 Event Schema

The system captures **6 event categories** with **30+ event types**:

1. **Authentication** (`auth_topic`) - Login, Logout, Signup
2. **Assessment** (`assessment_topic`) - Assignments, Quizzes, Grading
3. **Video** (`video_topic`) - Video watching behavior
4. **Course** (`course_topic`) - Enrollments, Materials, Downloads
5. **Profile** (`profile_topic`) - Profile updates, Avatar changes
6. **Notification** (`notification_topic`) - Notification delivery & engagement

**Complete specification**: [docs/event-schema-specification.md](docs/event-schema-specification.md)

---

## 🔧 Configuration

### MinIO Configuration
- **Endpoint**: `http://minio:9000`
- **Console**: `http://localhost:9001`
- **Credentials**: `minioadmin / minioadmin`

### Kafka Configuration
- **Bootstrap Server**: `localhost:9092`
- **Topics**: 6 topics (auth, assessment, video, course, profile, notification)

### Spark Configuration
- **Executor Memory**: 4GB
- **Executor Cores**: 2
- **Executors**: 3

Edit configurations in:
- `config/docker-compose.yml` - Local development
- `batch_layer/config.py` - Batch processing
- `batch_layer/oozie/job.properties` - Oozie jobs

---

## 📦 Batch Views (37 Total)

### Authentication (5 views)
- Daily active users (DAU)
- Hourly login patterns
- User session metrics
- Activity summary
- Registration analytics

### Assessment (7 views)
- Student submissions
- Engagement timeline
- Quiz performance
- Grading statistics
- Teacher workload
- Submission distribution
- Overall performance

### Video (7 views)
- Total watch time
- Student engagement
- Video popularity
- Daily engagement
- Course metrics
- Student-course summary
- Drop-off indicators

### Course (8 views)
- Enrollment stats
- Material access patterns
- Material popularity
- Download analytics
- Resource download stats
- Activity summary
- Daily engagement
- Overall metrics

### Profile & Notification (10 views)
- Profile update frequency
- Field changes
- Avatar changes
- Profile activity
- Notification delivery stats
- Engagement metrics
- Click-through rates
- User preferences
- Daily activity
- User summary

**Full documentation**: [batch_layer/README.md](batch_layer/README.md)

---

## 🧪 Testing

### Test Ingestion Layer
```powershell
# Start infrastructure
cd config
docker-compose up -d

# Run producer and ingestion
python ingestion_layer/producer.py
python ingestion_layer/minio_ingest.py

# Verify in MinIO Console: http://localhost:9001
```

### Test Batch Layer
```powershell
# Run all batch jobs
python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views

# Run specific job
python batch_layer/run_batch_jobs.py video s3a://bucket-0/master_dataset s3a://bucket-0/batch_views
```

### Test on Minikube
```powershell
cd deployment
.\deploy_minikube.ps1

# Follow prompts for testing
```

**Testing Guides**:
- [MINIKUBE_TESTING_GUIDE.md](deployment/MINIKUBE_TESTING_GUIDE.md)
- [TESTING_COMPARISON.md](deployment/TESTING_COMPARISON.md)

---

## 📖 Documentation

- **[Architecture Design](docs/architecture-design.md)** - System architecture overview
- **[Event Schema](docs/event-schema-specification.md)** - Complete event definitions
- **[Problem Definition](docs/problem-definition.md)** - Project requirements
- **[Deployment Guide](docs/deployment-guide.md)** - Setup instructions

**Layer Documentation**:
- [Ingestion Layer](ingestion_layer/README.md)
- [Batch Layer](batch_layer/README.md) + [Quick Start](batch_layer/QUICKSTART.md)
- [Speed Layer](speed_layer/README.md)
- [Serving Layer](serving_layer/README.md)

---

## 🛠️ Development Workflow

### Adding New Event Types

1. Update schema: `docs/event-schema-specification.md`
2. Update producer: `ingestion_layer/producer.py`
3. Create/update batch job: `batch_layer/jobs/<category>_batch_job.py`
4. Test ingestion → batch processing

### Adding New Batch Views

1. Create computation function in relevant batch job
2. Write output to MinIO: `s3a://bucket-0/batch_views/<view_name>`
3. Document in batch job docstring
4. Update `batch_layer/config.py` batch_views list

### Deploying Changes

**Local**:
```powershell
# Restart affected services
docker-compose restart kafka minio

# Re-run jobs
python batch_layer/run_batch_jobs.py ...
```

**Oozie**:
```powershell
cd batch_layer
.\deploy_oozie.ps1
oozie job -kill <old-coordinator-id>
oozie job -run -config oozie/job.properties
```

---

## 🔍 Monitoring

### MinIO Console
- URL: http://localhost:9001
- Credentials: `minioadmin / minioadmin`
- Browse: `bucket-0/master_dataset/` and `bucket-0/batch_views/`

### Spark UI
- URL: http://localhost:4040 (during job execution)
- Monitor: Job progress, stages, tasks

### Oozie UI
- URL: http://localhost:11000/oozie
- Monitor: Workflow status, coordinator jobs

---

## 🚧 Roadmap

### Phase 1: Ingestion & Batch ✅ (Complete)
- [x] Kafka event ingestion
- [x] MinIO storage
- [x] Batch layer with 5 PySpark jobs
- [x] 37 batch views
- [x] Oozie orchestration

### Phase 2: Speed Layer ⏳ (Next)
- [ ] Real-time stream processing
- [ ] Incremental view updates
- [ ] Windowed aggregations
- [ ] Late data handling

### Phase 3: Serving Layer ⏳
- [ ] Unified query interface
- [ ] Batch + speed view merger
- [ ] REST API
- [ ] Query optimization

### Phase 4: Production Readiness
- [ ] Monitoring dashboards (Grafana)
- [ ] Alerting system
- [ ] Performance optimization
- [ ] Scalability testing

---

## 👥 Team - Group 30

**Ingestion Layer Team**: Thịnh, Phú, Tiến  
**Batch Ingestion Team**: Lâm, Lộc

---

## 📄 License

University Big Data Course Project - 2025

---

## 🆘 Troubleshooting

### Common Issues

**Problem**: Cannot connect to Kafka  
**Solution**: Ensure Kafka is running: `docker ps | grep kafka` or check Minikube deployment

**Problem**: MinIO access denied  
**Solution**: Check credentials in config files (`minioadmin/minioadmin`)

**Problem**: Batch jobs fail with OOM  
**Solution**: Increase executor memory in `batch_layer/config.py`

**Problem**: No data in MinIO  
**Solution**: Ensure producer and ingestion are running, check Kafka topics have data

**More help**: Check respective README files in each layer directory

---

## 📞 Support

- Check layer-specific README files
- Review documentation in `docs/`
- Inspect logs: `docker logs <container>` or `oozie job -log <job-id>`
- Verify data: MinIO Console or `mc ls minio/bucket-0/`

---

**Project Status**: Batch Layer Complete ✅ | Speed Layer In Progress ⏳
