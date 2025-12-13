# Complete Minikube Testing Workflow

## Visual Guide: Step-by-Step Testing Process

---

## 📋 Pre-Test Checklist

Before starting, ensure you have:

- [ ] Docker Desktop installed and running
- [ ] Minikube installed (`choco install minikube`)
- [ ] kubectl installed (`choco install kubernetes-cli`)
- [ ] MinIO client installed (`choco install minio-client`)
- [ ] Python 3.8-3.11 with required packages (`pip install -r requirements.txt`)
- [ ] At least 8GB RAM available
- [ ] At least 20GB free disk space

---

## 🚀 Complete Testing Flow

### Stage 1: Infrastructure Deployment (One Time)

**Time**: 10-15 minutes

```
┌─────────────────────────────────────────────────────────┐
│ Step 1: Deploy Minikube Infrastructure                 │
└─────────────────────────────────────────────────────────┘

PowerShell Terminal 1:
> cd d:\2025.1\big_data\btl\BTLbigdata-group30
> .\deploy_minikube.ps1

Expected Output:
[1/10] Starting Minikube cluster... ✅
[2/10] Creating namespaces... ✅
[3/10] Installing Strimzi Kafka Operator... ✅
[4/10] Setting up storage... ✅
[5/10] Deploying Kafka cluster... ✅
[6/10] Creating Kafka topics... ✅
[7/10] Deploying MinIO... ✅
[8/10] Configuring MinIO bucket... ✅
[9/10] Checking deployment status... ✅
[10/10] Setup complete! ✅

What's Running:
├── Minikube (Kubernetes cluster)
├── Kafka Cluster (3 brokers)
│   ├── kafka-cluster-kafka-0
│   ├── kafka-cluster-kafka-1
│   └── kafka-cluster-kafka-2
├── MinIO (S3 storage)
└── 6 Kafka Topics
    ├── auth_topic
    ├── assessment_topic
    ├── video_topic
    ├── course_topic
    ├── profile_topic
    └── notification_topic
```

**Verify Deployment**:
```powershell
kubectl get pods -n kafka
kubectl get pods -n minio
kubectl get kafkatopics -n kafka
```

---

### Stage 2: Port Forwarding (Required Each Test Session)

**Time**: 1 minute

```
┌─────────────────────────────────────────────────────────┐
│ Step 2: Setup Port Forwards (3 Terminals)              │
└─────────────────────────────────────────────────────────┘

PowerShell Terminal 2 (Kafka):
> kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka

Output:
Forwarding from 127.0.0.1:9092 -> 9092
Forwarding from [::1]:9092 -> 9092

PowerShell Terminal 3 (MinIO API):
> kubectl port-forward service/minio 9000:9000 -n minio

Output:
Forwarding from 127.0.0.1:9000 -> 9000
Forwarding from [::1]:9000 -> 9000

PowerShell Terminal 4 (MinIO Console):
> kubectl port-forward service/minio 9001:9001 -n minio

Output:
Forwarding from 127.0.0.1:9001 -> 9001
Forwarding from [::1]:9001 -> 9001

Keep These Terminals Running! ⚠️
```

**Test Connectivity**:
```powershell
# In new terminal
Test-NetConnection localhost -Port 9092
Test-NetConnection localhost -Port 9000
Test-NetConnection localhost -Port 9001
```

---

### Stage 3: Data Generation (Producer)

**Time**: Continuous

```
┌─────────────────────────────────────────────────────────┐
│ Step 3: Start Event Producer                           │
└─────────────────────────────────────────────────────────┘

PowerShell Terminal 5 (Producer):
> python producer.py

Output:
Producing events to Kafka...
Bootstrap servers: ['localhost:9092']
Topics: auth_topic, assessment_topic, video_topic, course_topic, profile_topic, notification_topic
Press Ctrl+C to stop...

Sent LOGIN to auth_topic
Sent VIDEO_WATCHED to video_topic (user=SV001, video=VID042, duration=120s)
Sent QUIZ_COMPLETED to assessment_topic (user=SV003, quiz=QUIZ012, correct=8)
Sent ENROLL_COURSE to course_topic
Sent UPDATE_PROFILE to profile_topic
Sent NOTIFICATION_SENT to notification_topic
...

Event Distribution (per 100 events):
├── VIDEO events: 35%
├── ASSESSMENT events: 25%
├── AUTH events: 15%
├── COURSE events: 10%
├── PROFILE events: 10%
└── NOTIFICATION events: 5%

Let Run for 2-3 Minutes ⏱️
```

**Verify Kafka Messages**:
```powershell
# In new terminal
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic auth_topic --from-beginning --max-messages 5
exit
```

---

### Stage 4: Data Ingestion (Kafka → MinIO)

**Time**: Continuous

```
┌─────────────────────────────────────────────────────────┐
│ Step 4: Start Ingestion Layer                          │
└─────────────────────────────────────────────────────────┘

PowerShell Terminal 6 (Ingestion):
> python minio_ingest_k8s.py

Output:
============================================================
KAFKA TO MINIO INGESTION LAYER
============================================================
Kafka Bootstrap Servers: localhost:9092
MinIO Endpoint: http://localhost:9000
============================================================
============================================================
INGESTION LAYER STARTED
============================================================
Topics: auth_topic, assessment_topic, video_topic, course_topic, profile_topic, notification_topic
Destination: s3a://bucket-0/master_dataset/
Checkpoint: s3a://bucket-0/checkpoints/ingest/
Partitioning: By topic
Trigger: Every 1 minute
============================================================
Streaming query running... Press Ctrl+C to stop
============================================================

[After 1 minute]
Batch: 1
  - Input rows: 1247
  - Output rows: 1247
  - Topics: auth_topic (187), video_topic (437), assessment_topic (312), ...

[After 2 minutes]
Batch: 2
  - Input rows: 1189
  - Output rows: 1189
  - Topics: auth_topic (178), video_topic (416), assessment_topic (297), ...

Data Flow:
Producer → Kafka → Ingestion → MinIO ✅
          (9092)    (PySpark)   (S3A)
```

**Let Both Run for 3-5 Minutes** ⏱️

---

### Stage 5: Verification (Multiple Methods)

**Time**: 2-3 minutes

```
┌─────────────────────────────────────────────────────────┐
│ Step 5: Verify Data Ingestion                          │
└─────────────────────────────────────────────────────────┘

Method 1: MinIO Web Console
────────────────────────────
1. Open Browser: http://localhost:9001
2. Login:
   Username: minioadmin
   Password: minioadmin
3. Navigate: Object Browser → bucket-0 → master_dataset
4. Expected Structure:
   bucket-0/
   └── master_dataset/
       ├── topic=auth_topic/
       │   ├── part-00000-xxx.parquet
       │   ├── part-00001-xxx.parquet
       │   └── ...
       ├── topic=assessment_topic/
       ├── topic=video_topic/
       ├── topic=course_topic/
       ├── topic=profile_topic/
       └── topic=notification_topic/

Method 2: MinIO CLI
────────────────────
PowerShell Terminal 7:
> mc alias set minikube http://localhost:9000 minioadmin minioadmin
> mc ls --recursive minikube/bucket-0/master_dataset/

Output:
[2025-12-11 10:15:30]  1.2MiB bucket-0/master_dataset/topic=auth_topic/part-00000.parquet
[2025-12-11 10:15:30]  2.3MiB bucket-0/master_dataset/topic=video_topic/part-00000.parquet
[2025-12-11 10:15:30]  1.8MiB bucket-0/master_dataset/topic=assessment_topic/part-00000.parquet
...

Method 3: Kafka Topic Check
────────────────────────────
> kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bin/kafka-topics.sh --describe --topic auth_topic --bootstrap-server localhost:9092

Output:
Topic: auth_topic       PartitionCount: 3       ReplicationFactor: 3
Topic: auth_topic       Partition: 0    Leader: 0       Replicas: 0,1,2 Isr: 0,1,2

Method 4: Download and Inspect Data
────────────────────────────────────
> mc cp minikube/bucket-0/master_dataset/topic=auth_topic/part-00000.parquet ./test.parquet

# Install pyarrow if needed
> pip install pyarrow pandas

# Inspect with Python
> python
>>> import pandas as pd
>>> df = pd.read_parquet('test.parquet')
>>> print(df.head())
>>> print(df.info())
>>> exit()
```

---

### Stage 6: Monitoring & Debugging

**Time**: As needed

```
┌─────────────────────────────────────────────────────────┐
│ Step 6: Monitor System Health                          │
└─────────────────────────────────────────────────────────┘

Check Kafka Cluster Health:
────────────────────────────
> kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster

Expected:
NAME                         READY   STATUS    RESTARTS   AGE
kafka-cluster-kafka-0        1/1     Running   0          10m
kafka-cluster-kafka-1        1/1     Running   0          10m
kafka-cluster-kafka-2        1/1     Running   0          10m

Check MinIO Health:
────────────────────
> kubectl get pods -n minio

Expected:
NAME                     READY   STATUS    RESTARTS   AGE
minio-xxxxxxxxxx-xxxxx   1/1     Running   0          10m

Check Kafka Logs:
────────────────────
> kubectl logs kafka-cluster-kafka-0 -n kafka --tail=50

Check MinIO Logs:
────────────────────
> kubectl logs deployment/minio -n minio --tail=50

Check Topic Lag (Consumer Groups):
────────────────────────────────────
> kubectl exec kafka-cluster-kafka-0 -n kafka -- \
  bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups

Minikube Dashboard (Visual):
────────────────────────────
> minikube dashboard

Opens browser with K8s dashboard showing:
├── Pods status
├── Resource usage
├── Events
└── Logs
```

---

### Stage 7: Data Validation

**Time**: 5 minutes

```
┌─────────────────────────────────────────────────────────┐
│ Step 7: Validate Data Quality                          │
└─────────────────────────────────────────────────────────┘

Count Events per Topic:
────────────────────────
PowerShell:
> python

Python Script:
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataValidation") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Read data
df = spark.read.parquet("s3a://bucket-0/master_dataset/")

# Count by topic
df.groupBy("topic").count().show()

# Sample data from each topic
for topic in ["auth_topic", "video_topic", "assessment_topic"]:
    print(f"\n=== {topic} sample ===")
    df.filter(df.topic == topic).select("value").show(5, truncate=False)

spark.stop()

Expected Output:
+----------------------+------+
|topic                 |count |
+----------------------+------+
|auth_topic            |1567  |
|video_topic           |3245  |
|assessment_topic      |2189  |
|course_topic          |987   |
|profile_topic         |876   |
|notification_topic    |432   |
+----------------------+------+
```

---

### Stage 8: Cleanup

**Time**: 2 minutes

```
┌─────────────────────────────────────────────────────────┐
│ Step 8: Stop and Cleanup                               │
└─────────────────────────────────────────────────────────┘

Stop Producer & Ingestion:
────────────────────────────
Terminal 5 (Producer): Press Ctrl+C
Terminal 6 (Ingestion): Press Ctrl+C

Stop Port Forwards:
────────────────────────────
Terminals 2, 3, 4: Press Ctrl+C

Or stop all jobs:
> Get-Job | Stop-Job
> Get-Job | Remove-Job

Cleanup Minikube (Keep Cluster):
────────────────────────────────
> minikube stop

Full Cleanup (Delete Everything):
────────────────────────────────
> .\cleanup_minikube.ps1

Output:
Stopping port-forwards... ✅
Deleting Kafka cluster... ✅
Deleting MinIO... ✅
Deleting Kafka namespace... ✅
Stopping Minikube... ✅
Do you want to DELETE the Minikube cluster completely? [yes/no]
> yes
Deleting Minikube cluster... ✅
CLEANUP COMPLETE! ✅
```

---

## 📊 Success Metrics

After running for 5 minutes, you should have:

| Metric | Expected Value |
|--------|----------------|
| **Kafka Topics** | 6 topics (all have messages) |
| **Kafka Messages** | 5000-10000 total |
| **MinIO Partitions** | 6 topic folders |
| **Parquet Files** | Multiple files per topic |
| **File Size** | 1-5 MB per file |
| **Checkpoints** | Present in bucket-0/checkpoints/ |
| **Producer Rate** | ~100-500 events/sec |
| **Ingestion Batches** | Every 1 minute |
| **Data Latency** | <2 minutes from producer to MinIO |

---

## 🐛 Common Issues & Solutions

### Issue 1: Producer Can't Connect to Kafka

**Symptom**:
```
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```

**Solution**:
```powershell
# Check port-forward is running
Get-Job | Where-Object { $_.Command -like "*9092*" }

# If not running, restart
kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka

# Test connection
Test-NetConnection localhost -Port 9092
```

### Issue 2: Ingestion Can't Write to MinIO

**Symptom**:
```
java.io.IOException: No FileSystem for scheme: s3a
```

**Solution**:
```powershell
# Check hadoop-aws package is included
# Should be in minio_ingest_k8s.py:
.config("spark.jars.packages", "...,org.apache.hadoop:hadoop-aws:3.3.4")

# Check MinIO port-forward
kubectl port-forward service/minio 9000:9000 -n minio
```

### Issue 3: No Data in MinIO After 5 Minutes

**Check**:
```powershell
# 1. Verify producer is sending
# Terminal 5 should show: "Sent XXX to YYY_topic"

# 2. Verify Kafka has messages
kubectl exec kafka-cluster-kafka-0 -n kafka -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic auth_topic \
  --from-beginning \
  --max-messages 5

# 3. Check ingestion logs
# Terminal 6 should show: "Batch: X" every minute

# 4. Check MinIO connection
mc admin info minikube
```

### Issue 4: Kafka Pods Not Starting

**Symptom**:
```
kafka-cluster-kafka-0   0/1     Pending   0          5m
```

**Solution**:
```powershell
# Check PersistentVolumes
kubectl get pv

# Check events
kubectl get events -n kafka --sort-by='.lastTimestamp'

# Describe pod
kubectl describe pod kafka-cluster-kafka-0 -n kafka

# If volume issues, recreate PVs
kubectl delete pv pv-kafka-0 pv-kafka-1 pv-kafka-2
kubectl apply -f kafka/persistent-volumn-minikube.yaml
```

---

## ⏱️ Timeline Summary

| Stage | Time | Can Skip? |
|-------|------|-----------|
| 1. Deploy infrastructure | 10-15 min | No (one time) |
| 2. Port forwarding | 1 min | No (each session) |
| 3. Start producer | 30 sec | No |
| 4. Start ingestion | 30 sec | No |
| 5. Wait for data | 2-3 min | No |
| 6. Verification | 2-3 min | No |
| 7. Validation | 5 min | Yes (optional) |
| 8. Cleanup | 2 min | No |
| **Total** | **25-30 min** | First time |
| **After setup** | **10-15 min** | Subsequent tests |

---

## 📝 Testing Checklist

Before demo/presentation:

- [ ] Deploy infrastructure successfully
- [ ] All pods running (3 Kafka + 1 MinIO)
- [ ] 6 topics created
- [ ] Producer sends events
- [ ] Ingestion writes to MinIO
- [ ] Data visible in MinIO Console
- [ ] Parquet files in all 6 topic partitions
- [ ] Checkpoints directory exists
- [ ] Can read data with PySpark
- [ ] Know how to restart if needed
- [ ] Cleanup script works
- [ ] Can explain architecture

---

## 🎓 Next Steps

After successful testing:

1. **Batch Processing Layer**: Read from MinIO, perform aggregations
2. **Stream Processing Layer**: Real-time metrics from Kafka
3. **Serving Layer**: Unified view of batch + stream
4. **Dashboard**: Visualize results
5. **Documentation**: Architecture diagrams
6. **Presentation**: Practice explaining the flow

---

## 📚 Related Documents

- [MINIKUBE_TESTING_GUIDE.md](MINIKUBE_TESTING_GUIDE.md) - Complete detailed guide
- [MINIKUBE_QUICK_REFERENCE.md](MINIKUBE_QUICK_REFERENCE.md) - Command reference
- [TESTING_COMPARISON.md](TESTING_COMPARISON.md) - Local vs Minikube
- [README.md](README.md) - Project overview

---

**You're all set! Follow this workflow for reliable testing. Good luck! 🚀**
