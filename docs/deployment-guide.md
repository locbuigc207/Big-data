# Deployment Guide for Class Project
## University Learning Analytics System

---

## 1. Deployment Options Comparison

### Option A: Local Development (Simplest) ⭐ **RECOMMENDED FOR LEARNING**
### Option B: Cloud-Based (Most Realistic)
### Option C: Hybrid (Best Balance)

---

## 2. ⭐ RECOMMENDED: Hybrid Approach for Class Project

### 2.1 Overview

```
Your Local Machine (Windows)          Cloud/University Servers
├── Development Environment           ├── Kafka Cluster
│   ├── Python/PySpark                │   └── 3 brokers (lightweight)
│   ├── Jupyter Notebooks             │
│   ├── Data Generation               ├── Hadoop/HDFS
│   └── Testing                       │   └── NameNode + 2 DataNodes
│                                     │
├── Docker Desktop                    ├── MongoDB Atlas (Free Tier)
│   ├── Kafka (single node)           │   └── 512MB cluster
│   ├── MongoDB (local)               │
│   ├── PostgreSQL                    └── Optional: University Cluster
│   └── Spark Master/Worker               └── If available
│
└── VS Code + Extensions
```

### 2.2 Why Hybrid?

✅ **Local Development**:
- Fast iteration (no network latency)
- No cost during development
- Full control over environment
- Easy debugging

✅ **Cloud/University for Production**:
- Demonstrates deployment skills
- More realistic architecture
- Can handle larger datasets
- Looks better in presentations

---

## 3. Detailed Setup by Component

### 3.1 Where Each System Should Run

| Component | Development | Testing | Demo/Presentation |
|-----------|-------------|---------|-------------------|
| **Data Generation** | Local (Jupyter) | Local | Pre-generated |
| **Kafka** | Docker (local) | Docker/University | Cloud/University |
| **HDFS** | MinIO/Local FS | Docker/University | Cloud/University |
| **Spark** | Local (standalone) | Docker/University | Cloud/University |
| **MongoDB** | Docker (local) | MongoDB Atlas Free | MongoDB Atlas |
| **PostgreSQL** | Docker (local) | Docker/Local | Docker/Cloud |
| **Dashboards** | Local (Grafana) | Local | Cloud (accessible URL) |
| **Kubernetes** | Minikube (optional) | Skip for class project | Optional (demo only) |

---

## 4. Phase 1: Local Development Setup (Week 1-2)

### 4.1 Your Windows Machine Setup

#### Prerequisites
```powershell
# 1. Install Chocolatey (package manager for Windows)
Set-ExecutionPolicy Bypass -Scope Process -Force
iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))

# 2. Install required software
choco install -y python311 git docker-desktop vscode openjdk11

# 3. Install Python packages
pip install pyspark pandas numpy faker jupyter notebook plotly dash
pip install pymongo psycopg2 kafka-python sqlalchemy

# 4. Install Spark locally
# Download from: https://spark.apache.org/downloads.html
# Extract to: C:\spark
# Set environment variables:
# SPARK_HOME=C:\spark
# JAVA_HOME=C:\Program Files\OpenJDK\jdk-11
# Add to PATH: %SPARK_HOME%\bin
```

#### Docker Compose Configuration
Create `docker-compose.yml` in your project:

```yaml
version: '3.8'

services:
  # Kafka & Zookeeper
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data

  # MongoDB
  mongodb:
    image: mongo:7.0
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password123
      MONGO_INITDB_DATABASE: edu_analytics
    volumes:
      - mongodb-data:/data/db

  # PostgreSQL
  postgres:
    image: postgres:16
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: edu_analytics
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: password123
    volumes:
      - postgres-data:/var/lib/postgresql/data

  # MinIO (S3-compatible storage, HDFS alternative)
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password123
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data

  # Grafana (Dashboards)
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana-data:/var/lib/grafana

  # Spark Master (Optional - for testing distributed mode)
  spark-master:
    image: bitnami/spark:3.5.0
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_HOST=spark-master
    ports:
      - "8080:8080"  # Web UI
      - "7077:7077"  # Spark master port
    volumes:
      - ./spark-apps:/opt/spark-apps
      - ./spark-data:/opt/spark-data

  # Spark Worker (Optional)
  spark-worker:
    image: bitnami/spark:3.5.0
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
    volumes:
      - ./spark-apps:/opt/spark-apps
      - ./spark-data:/opt/spark-data

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:
  mongodb-data:
  postgres-data:
  minio-data:
  grafana-data:
```

#### Start All Services
```powershell
# Navigate to project directory
cd d:\2025.1\big_data\btl

# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f kafka

# Stop all services
docker-compose down

# Stop and remove all data
docker-compose down -v
```

### 4.2 Local Spark Development

```python
# test_spark_local.py
from pyspark.sql import SparkSession

# Create local Spark session
spark = SparkSession.builder \
    .appName("EduAnalytics-Local") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Test with your generated data
df_students = spark.read.csv("generate_fake_data/students.csv", header=True, inferSchema=True)
df_grades = spark.read.csv("generate_fake_data/grades.csv", header=True, inferSchema=True)

# Simple join test
result = df_students.join(df_grades, "student_id") \
    .groupBy("faculty") \
    .agg({"total_score": "avg"})

result.show()

spark.stop()
```

---

## 5. Phase 2: Cloud Setup for Demo (Week 3-4)

### 5.1 Free Cloud Options

#### Option A: MongoDB Atlas (Recommended)
```
✅ Free Tier: 512MB storage
✅ Perfect for: Serving layer, real-time data
✅ Setup: 5 minutes
✅ URL: https://www.mongodb.com/cloud/atlas/register

Steps:
1. Sign up with university email
2. Create free cluster (M0)
3. Whitelist IP: 0.0.0.0/0 (allow from anywhere)
4. Create database user
5. Get connection string
```

#### Option B: Google Cloud Platform (Education Credits)
```
🎓 Students get $300 free credits
✅ Good for: HDFS (Cloud Storage), Dataproc (managed Spark)
✅ Duration: 90 days
✅ URL: https://cloud.google.com/edu

Resources to provision:
- Dataproc cluster (1 master + 2 workers) - ~$0.50/hour
- Cloud Storage bucket - free for <5GB
- Run only during development/demo
```

#### Option C: AWS Academy (If available)
```
🎓 Check if your university has AWS Academy
✅ Free credits for students
✅ Services: EC2, EMR (managed Spark), S3, RDS

Setup:
- EMR cluster (3 nodes) - on-demand when needed
- S3 for data storage
- RDS for PostgreSQL
```

#### Option D: Azure for Students
```
🎓 $100 free credits
✅ Services: HDInsight (Spark), Blob Storage, Cosmos DB
✅ URL: https://azure.microsoft.com/en-us/free/students/
```

### 5.2 University Resources (Check First!)

```
❓ Ask your professor/IT department:

1. Does the university have a Hadoop cluster?
   → Many CS departments have shared clusters

2. Is there access to cloud credits?
   → Some universities have education agreements

3. Are there dedicated servers for student projects?
   → Physical machines you can use

4. Can you use lab computers after hours?
   → Run distributed setup across lab machines
```

---

## 6. Recommended Architecture for Class Project

### 6.1 Minimal Viable Setup (Good Grade)

```
┌─────────────────────────────────────────────────────────┐
│ Your Laptop (Windows)                                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Data Generation (Jupyter)                             │
│  ↓                                                      │
│  Local Files (CSV/Parquet)                             │
│  ↓                                                      │
│  Docker Containers:                                     │
│    ├── Kafka (simulate streaming)                      │
│    ├── MongoDB (serving layer)                         │
│    └── PostgreSQL (analytics)                          │
│  ↓                                                      │
│  PySpark (local mode)                                  │
│    ├── Batch processing                                │
│    └── Structured Streaming                            │
│  ↓                                                      │
│  Jupyter Notebooks (analysis & visualization)          │
│                                                         │
└─────────────────────────────────────────────────────────┘

Storage: Local filesystem (simulates HDFS)
Deployment: "Production-ready code, local execution"
```

**Pros**:
- ✅ Zero cost
- ✅ Fast development
- ✅ Demonstrates all concepts
- ✅ Easy to debug

**Cons**:
- ❌ Not truly distributed
- ❌ Limited scalability demo

### 6.2 Enhanced Setup (Excellent Grade)

```
┌─────────────────────────────────────────────────────────┐
│ Your Laptop (Development)                               │
│  - Code development                                     │
│  - Testing                                              │
│  - Jupyter notebooks                                    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ↓ (Deploy to cloud for demo)
┌─────────────────────────────────────────────────────────┐
│ Cloud (GCP/AWS/Azure - Student Credits)                 │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Managed Spark (Dataproc/EMR/HDInsight)                │
│    ├── 1 master node (2 vCPU, 8GB RAM)                 │
│    └── 2 worker nodes (2 vCPU, 8GB RAM each)           │
│                                                         │
│  Cloud Storage (S3/GCS/Blob)                           │
│    └── Data files (CSV/Parquet)                        │
│                                                         │
│  MongoDB Atlas (Free M0 cluster)                       │
│    └── Real-time serving layer                         │
│                                                         │
│  Managed Kafka (Confluent Cloud Free Tier)             │
│    └── 1 topic, limited throughput                     │
│                                                         │
└─────────────────────────────────────────────────────────┘

Deployment: Docker images pushed to container registry
Access: Public URLs for demo/presentation
```

**Pros**:
- ✅ Truly distributed
- ✅ Production-like
- ✅ Impressive demo
- ✅ Can handle larger datasets

**Cons**:
- ⚠️ Requires cloud credits
- ⚠️ More complex setup
- ⚠️ Must manage costs

### 6.3 Cost Estimation (Enhanced Setup)

```
Monthly Costs (if running 24/7):
- Dataproc cluster (3 nodes): ~$150-200/month
- MongoDB Atlas Free: $0
- Cloud Storage (10GB): ~$0.50/month
- Confluent Cloud Free: $0
Total: ~$150-200/month

💡 COST SAVING STRATEGIES:

1. Run on-demand only:
   - Start cluster only when needed
   - Estimated: 20 hours/month = ~$10-15

2. Use preemptible/spot instances:
   - 70-80% cheaper
   - Good for non-critical workloads

3. Shut down overnight:
   - Automate with Cloud Scheduler
   - Run only during work hours

4. Use student credits:
   - GCP: $300 free (covers 3+ months)
   - AWS Academy: Variable
   - Azure: $100 free

Realistic cost for 4-week project: $0-30
(using free tiers + student credits)
```

---

## 7. Recommended Timeline & Deployment Strategy

### Week 1-2: Local Development
```
Location: Your laptop
Tasks:
  ✓ Generate fake data
  ✓ Set up Docker containers
  ✓ Write Spark batch jobs (local mode)
  ✓ Test basic transformations
  ✓ Develop streaming logic
  ✓ Create MongoDB queries
  ✓ Build initial dashboards

Why local: Fast iteration, no costs
```

### Week 3: Cloud Migration (Optional)
```
Location: Cloud (GCP/AWS/Azure)
Tasks:
  ✓ Provision cloud resources
  ✓ Upload data to cloud storage
  ✓ Deploy Spark jobs to cluster
  ✓ Configure managed Kafka
  ✓ Connect to MongoDB Atlas
  ✓ Test end-to-end pipeline

Why cloud: Demonstrates deployment skills
```

### Week 4: Demo Preparation
```
Location: Cloud (for live demo) OR Local (with recorded demo)
Tasks:
  ✓ Prepare presentation slides
  ✓ Create demo scenarios
  ✓ Record video walkthrough (backup)
  ✓ Prepare architecture diagrams
  ✓ Document code & setup

Presentation strategy:
  - Show live dashboard (cloud URL)
  - Walk through code (local VS Code)
  - Explain architecture (slides)
  - Demo streaming (Kafka producer)
```

---

## 8. Kubernetes: Do You Really Need It?

### 8.1 For Class Project: **Optional/Not Required**

```
❌ Kubernetes is OVERKILL for class project if:
  - You have limited time (4-8 weeks)
  - Team size < 4 people
  - Focus is on data processing, not DevOps
  - No prior K8s experience

✅ Kubernetes makes sense if:
  - Professor specifically requires it
  - You want to learn container orchestration
  - You have extra time
  - Team has DevOps interest
```

### 8.2 Alternative: "Kubernetes-Ready" Approach

```
Strategy: Write code as if deploying to K8s, but run locally

What this means:
  1. Use Docker containers (good practice)
  2. Write deployment YAML files (show you know K8s)
  3. Actually run with Docker Compose (simpler)
  4. Explain: "This is K8s-ready, running locally for demo"

Benefit:
  ✓ Shows you understand K8s concepts
  ✓ Saves massive setup time
  ✓ Still gets full points
  ✓ Focus on Spark/data processing (core of project)
```

### 8.3 If You Decide to Use K8s: Minikube

```powershell
# Install Minikube (local K8s cluster)
choco install minikube

# Start local cluster
minikube start --cpus=4 --memory=8192 --driver=docker

# Deploy applications
kubectl apply -f k8s-manifests/

# Access services
minikube service grafana

# Stop cluster
minikube stop

# Delete cluster
minikube delete
```

---

## 9. Final Recommendations

### 9.1 Best Setup for Your Situation

```
┌────────────────────────────────────────────────────────┐
│ RECOMMENDED SETUP FOR CLASS PROJECT                   │
├────────────────────────────────────────────────────────┤
│                                                        │
│ Development (Weeks 1-3):                              │
│   ✓ Your Windows laptop                               │
│   ✓ Docker Desktop (Kafka, MongoDB, PostgreSQL)      │
│   ✓ Local Spark (standalone mode)                    │
│   ✓ Local filesystem (simulate HDFS)                 │
│   ✓ Jupyter for analysis                             │
│                                                        │
│ Demo/Presentation (Week 4):                           │
│   Option A (Recommended):                             │
│     ✓ Keep local, record professional demo video      │
│     ✓ Show architecture slides                        │
│     ✓ Walk through live code                          │
│     ✓ Zero cost, less stress                          │
│                                                        │
│   Option B (If you have cloud credits):               │
│     ✓ Deploy to GCP Dataproc (2-3 days before demo)  │
│     ✓ MongoDB Atlas (free tier)                       │
│     ✓ Public dashboard URL                            │
│     ✓ Live demonstration                              │
│                                                        │
│ Skip:                                                  │
│   ✗ Kubernetes (unless required by professor)        │
│   ✗ Multi-cloud setup (overcomplicated)              │
│   ✗ Physical cluster setup (time-consuming)          │
│                                                        │
└────────────────────────────────────────────────────────┘
```

### 9.2 What Matters for Grading

```
High Impact (Focus Here):
  ✓ Architecture design (Lambda, well-justified)
  ✓ Spark code quality (complex transformations, optimizations)
  ✓ Data processing pipeline (batch + streaming working)
  ✓ ML models (GPA prediction, dropout risk)
  ✓ Visualization (clear dashboards)
  ✓ Documentation (README, architecture diagrams)
  ✓ Presentation (clear explanation of design decisions)

Medium Impact:
  ⚠ Cloud deployment (nice to have, not essential)
  ⚠ Kubernetes (shows DevOps knowledge)
  ⚠ Real HDFS cluster (vs. local filesystem)

Low Impact:
  - Fancy UI design
  - Overcomplicated architecture
  - Too many technologies (focus > breadth)
```

### 9.3 Red Flags to Avoid

```
❌ Don't:
  - Spend 80% of time on DevOps, 20% on data processing
  - Use technologies you don't understand (just for resume)
  - Over-engineer for a 4-week project
  - Forget to backup your work (use Git!)
  - Wait until week 4 to test integration

✅ Do:
  - Focus on Spark capabilities (project requirement)
  - Write clean, documented code
  - Test incrementally
  - Keep it simple, then enhance
  - Prepare backup demo (recorded video)
```

---

## 10. Quick Start Commands

### Start Development Environment
```powershell
# 1. Navigate to project
cd d:\2025.1\big_data\btl

# 2. Start all Docker services
docker-compose up -d

# 3. Wait for services to be ready (~30 seconds)
timeout /t 30

# 4. Verify services
docker-compose ps

# 5. Open Jupyter
jupyter notebook

# 6. Access UIs:
# - Kafka: http://localhost:9092
# - MongoDB: mongodb://admin:password123@localhost:27017
# - PostgreSQL: postgresql://admin:password123@localhost:5432/edu_analytics
# - Grafana: http://localhost:3000 (admin/admin)
# - Spark UI: http://localhost:8080 (if using Docker Spark)
```

### Daily Workflow
```powershell
# Morning: Start services
docker-compose start

# Work: Develop & test
code .  # Open VS Code

# Evening: Stop services (save resources)
docker-compose stop

# View logs if issues
docker-compose logs -f [service-name]
```

### Demo Day
```powershell
# Option 1: Local demo
docker-compose up -d
jupyter notebook
# Run notebooks, show dashboards

# Option 2: Cloud demo
# Access via public URLs (pre-deployed)
# https://your-grafana-instance.com
# https://your-mongodb-atlas.com
```

---

## 11. Troubleshooting

### Common Issues

**Docker containers won't start:**
```powershell
# Check Docker Desktop is running
# Restart Docker Desktop
# Check ports not already in use:
netstat -ano | findstr "9092"  # Kafka
netstat -ano | findstr "27017" # MongoDB

# Nuclear option: reset everything
docker-compose down -v
docker system prune -a
docker-compose up -d
```

**Spark runs out of memory:**
```python
# Reduce parallelism
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Use sampling for development
df_sample = df.sample(0.1)  # Use 10% of data
```

**Can't connect to cloud services:**
```
1. Check firewall settings
2. Verify IP whitelisting (MongoDB Atlas)
3. Check credentials/connection strings
4. Test with simple Python script first
```

---

## 12. Summary

### For Your Class Project:

🎯 **Primary Location**: Your Windows laptop (Docker + local Spark)

🎯 **Data Storage**: Local filesystem (CSV/Parquet files)

🎯 **Databases**: Docker containers (MongoDB + PostgreSQL)

🎯 **Streaming**: Kafka in Docker (single broker is fine)

🎯 **Spark Processing**: Local standalone mode

🎯 **Deployment**: Docker Compose (not Kubernetes)

🎯 **Cloud Usage**: Optional - only MongoDB Atlas (free) + demo hosting

🎯 **Cost**: $0-10 total (everything free + optional cloud for final demo)

---

**Next Steps:**
1. Run `docker-compose up -d` to start local environment
2. Test Spark locally with your generated data
3. Develop & iterate quickly
4. Decide on cloud deployment 2 weeks before deadline
5. Prepare killer presentation!

Good luck with your project! 🚀
