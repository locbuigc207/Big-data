# Minikube Testing Guide
## Testing Kafka Ingestion Layer on Kubernetes

This guide will help you test the complete ingestion pipeline (Producer → Kafka → Ingestion Layer → MinIO) using Minikube on Windows.

---

## Prerequisites

### 1. Install Required Tools

```powershell
# Install Minikube
choco install minikube

# Install kubectl (Kubernetes CLI)
choco install kubernetes-cli

# Verify installations
minikube version
kubectl version --client
```

### 2. System Requirements
- Windows 10/11
- Docker Desktop installed and running
- At least 8GB RAM available
- 20GB free disk space

---

## Step 1: Start Minikube Cluster

```powershell
# Start Minikube with sufficient resources
minikube start --cpus=4 --memory=8192 --driver=docker

# Verify cluster is running
kubectl get nodes

# Enable metrics (optional, for monitoring)
minikube addons enable metrics-server

# Check Minikube status
minikube status
```

Expected output:
```
✅ minikube
✅ type: Control Plane
✅ host: Running
✅ kubelet: Running
✅ apiserver: Running
✅ kubeconfig: Configured
```

---

## Step 2: Install Strimzi Kafka Operator

Strimzi manages Kafka on Kubernetes.

```powershell
# Create kafka namespace
kubectl create namespace kafka

# Install Strimzi operator
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Wait for operator to be ready (takes 1-2 minutes)
kubectl wait deployment/strimzi-cluster-operator --for=condition=Available --timeout=300s -n kafka

# Verify operator is running
kubectl get pods -n kafka
```

Expected output:
```
NAME                                        READY   STATUS    RESTARTS   AGE
strimzi-cluster-operator-xxxxxxxxxx-xxxxx   1/1     Running   0          1m
```

---

## Step 3: Deploy Storage and Kafka Cluster

### Apply Storage Configuration

```powershell
# Apply storage class
kubectl apply -f kafka/storage-class.yaml -n kafka

# Create persistent volumes (for Minikube, we'll use local path)
# Note: The persistent-volumn.yaml references specific nodes, we need to adapt it
# For Minikube, we'll create dynamic PVs

# Create directories in Minikube node
minikube ssh "sudo mkdir -p /mnt/kafka-data/0 /mnt/kafka-data/1 /mnt/kafka-data/2"

# Apply persistent volumes
kubectl apply -f kafka/persistent-volumn-minikube.yaml -n kafka

# Verify PVs
kubectl get pv
```

### Deploy Kafka Cluster

```powershell
# Apply Kafka deployment
kubectl apply -f kafka/deployment.yaml -n kafka

# Wait for Kafka cluster to be ready (takes 3-5 minutes)
kubectl wait kafka/kafka-cluster --for=condition=Ready --timeout=600s -n kafka

# Check Kafka pods
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster
```

Expected output:
```
NAME                         READY   STATUS    RESTARTS   AGE
kafka-cluster-kafka-0        1/1     Running   0          3m
kafka-cluster-kafka-1        1/1     Running   0          3m
kafka-cluster-kafka-2        1/1     Running   0          3m
```

---

## Step 4: Deploy MinIO (S3-Compatible Storage)

```powershell
# Create MinIO namespace
kubectl create namespace minio

# Apply MinIO deployment
kubectl apply -f minio/deployment.yaml -n minio

# Wait for MinIO to be ready
kubectl wait deployment/minio --for=condition=Available --timeout=300s -n minio

# Check MinIO pods
kubectl get pods -n minio
```

---

## Step 5: Create Kafka Topics

```powershell
# Apply Kafka topics configuration
kubectl apply -f kafka/topics.yaml -n kafka

# Verify topics are created
kubectl get kafkatopics -n kafka
```

Expected output:
```
NAME                 CLUSTER         PARTITIONS   REPLICATION FACTOR
auth-topic           kafka-cluster   3            3
assessment-topic     kafka-cluster   5            3
video-topic          kafka-cluster   5            3
course-topic         kafka-cluster   3            3
profile-topic        kafka-cluster   2            3
notification-topic   kafka-cluster   3            3
```

---

## Step 6: Port Forwarding for Access

Open 3 separate PowerShell terminals:

### Terminal 1: Kafka Port Forward
```powershell
kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka
```

### Terminal 2: MinIO API Port Forward
```powershell
kubectl port-forward service/minio 9000:9000 -n minio
```

### Terminal 3: MinIO Console Port Forward
```powershell
kubectl port-forward service/minio 9001:9001 -n minio
```

**Keep these terminals running!**

---

## Step 7: Configure MinIO Bucket

```powershell
# Open new terminal (Terminal 4)
# Install MinIO client
choco install minio-client

# Configure MinIO client
mc alias set minikube http://localhost:9000 minioadmin minioadmin

# Create bucket for ingestion
mc mb minikube/bucket-0

# Verify bucket
mc ls minikube
```

Expected output:
```
[2025-12-11 10:00:00 UTC]     0B bucket-0/
```

---

## Step 8: Run Producer (Generate Events)

Open new terminal (Terminal 5):

```powershell
# Navigate to project directory
cd d:\2025.1\big_data\btl\BTLbigdata-group30

# Activate Python environment (if using venv)
# .\venv\Scripts\Activate.ps1

# Install dependencies
pip install kafka-python faker

# Run producer
python producer.py
```

Expected output:
```
Producing events to Kafka...
Topics: auth_topic, assessment_topic, video_topic, course_topic, profile_topic, notification_topic
Press Ctrl+C to stop...

Sent LOGIN event to auth_topic
Sent VIDEO_WATCHED event to video_topic
Sent QUIZ_COMPLETED event to assessment_topic
...
```

**Let producer run for 2-3 minutes to generate test data**

---

## Step 9: Deploy Ingestion Layer (PySpark)

### Option A: Run Locally with Spark

```powershell
# Open new terminal (Terminal 6)
cd d:\2025.1\big_data\btl\BTLbigdata-group30

# Install PySpark
pip install pyspark==3.5.0

# Update minio_ingest.py to use correct Kafka bootstrap servers
# kafka.bootstrap.servers should be "localhost:9092"

# Run ingestion layer
python minio_ingest.py
```

### Option B: Deploy as Kubernetes Job

```powershell
# Build Docker image with Spark and ingestion code
docker build -t ingestion-layer:latest -f Dockerfile.ingestion .

# Load image into Minikube
minikube image load ingestion-layer:latest

# Deploy ingestion job
kubectl apply -f spark/ingestion-job.yaml -n kafka

# Check job status
kubectl get jobs -n kafka
kubectl logs job/kafka-ingestion -n kafka -f
```

Expected output:
```
============================================================
INGESTION LAYER - Kafka to MinIO
============================================================
Topics: auth, assessment, video, course, profile, notification
Destination: s3a://bucket-0/master_dataset/
Checkpoint: s3a://bucket-0/checkpoints/ingest/
Partitioning: By topic
Trigger: Every 1 minute
============================================================
```

---

## Step 10: Verify Data Ingestion

### Check MinIO for Data

```powershell
# List objects in bucket
mc ls --recursive minikube/bucket-0/master_dataset/

# Expected directory structure:
# bucket-0/master_dataset/topic=auth_topic/...parquet files
# bucket-0/master_dataset/topic=assessment_topic/...parquet files
# bucket-0/master_dataset/topic=video_topic/...parquet files
# etc.
```

### Access MinIO Web Console

1. Open browser: http://localhost:9001
2. Login: `minioadmin` / `minioadmin`
3. Navigate to **Object Browser** → **bucket-0** → **master_dataset**
4. You should see partitioned folders by topic with Parquet files

### Verify Kafka Messages

```powershell
# Enter Kafka pod
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bash

# Inside pod, consume messages from a topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic auth_topic --from-beginning --max-messages 10

# Exit pod
exit
```

---

## Step 11: Monitor and Debug

### Check Ingestion Job Logs

```powershell
# Get ingestion pod name
kubectl get pods -n kafka | findstr ingestion

# View logs
kubectl logs <ingestion-pod-name> -n kafka -f
```

### Check Kafka Topics

```powershell
# List topics
kubectl exec kafka-cluster-kafka-0 -n kafka -- bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
kubectl exec kafka-cluster-kafka-0 -n kafka -- bin/kafka-topics.sh --describe --topic auth_topic --bootstrap-server localhost:9092
```

### Check MinIO Health

```powershell
mc admin info minikube
```

### Minikube Dashboard (Visual Monitoring)

```powershell
minikube dashboard
```

---

## Complete Test Flow Summary

```
1. Producer (localhost) → Kafka (Minikube) via port-forward
   └── Generates 6 event types to 6 topics
   
2. Kafka (Minikube) → Ingestion Layer (PySpark)
   └── Reads from all 6 topics
   
3. Ingestion Layer → MinIO (Minikube) via S3A protocol
   └── Writes Parquet files partitioned by topic
   
4. MinIO (Minikube) ← Query/Verify via mc client or web console
   └── Verify partitioned data exists
```

---

## Expected Results

After running for 5 minutes:

✅ **Kafka Topics**: 6 topics with messages (check with kafka-console-consumer)  
✅ **MinIO Bucket**: `bucket-0/master_dataset/` with 6 topic partitions  
✅ **Parquet Files**: Multiple `.parquet` files in each topic partition  
✅ **Checkpoints**: `bucket-0/checkpoints/ingest/` directory exists  
✅ **Producer Logs**: Shows successful message sends  
✅ **Ingestion Logs**: Shows successful writes every 1 minute  

---

## Troubleshooting

### Producer Can't Connect to Kafka
```powershell
# Check port-forward is running
# Restart: kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka

# Test connection
telnet localhost 9092
```

### Ingestion Layer Can't Write to MinIO
```powershell
# Check MinIO credentials in minio_ingest.py
# Should have S3A configurations:
# spark.hadoop.fs.s3a.access.key=minioadmin
# spark.hadoop.fs.s3a.secret.key=minioadmin
# spark.hadoop.fs.s3a.endpoint=http://localhost:9000
```

### Kafka Pods Not Starting
```powershell
# Check PersistentVolumes
kubectl get pv

# Check events
kubectl get events -n kafka --sort-by='.lastTimestamp'

# Describe pod for errors
kubectl describe pod kafka-cluster-kafka-0 -n kafka
```

### Out of Memory Errors
```powershell
# Increase Minikube resources
minikube stop
minikube delete
minikube start --cpus=6 --memory=12288 --driver=docker
```

---

## Cleanup

```powershell
# Stop producer (Ctrl+C in producer terminal)

# Stop ingestion layer (Ctrl+C or delete job)
kubectl delete job kafka-ingestion -n kafka

# Stop port-forwards (Ctrl+C in each terminal)

# Delete Kafka cluster
kubectl delete kafka kafka-cluster -n kafka

# Delete MinIO deployment
kubectl delete -f minio/deployment.yaml -n minio

# Delete namespaces
kubectl delete namespace kafka minio

# Stop Minikube
minikube stop

# Optional: Delete cluster completely
minikube delete
```

---

## Next Steps

After successful testing:

1. **Batch Processing Layer**: Create PySpark job to read from MinIO and perform aggregations
2. **Stream Processing Layer**: Deploy real-time processing with Spark Structured Streaming
3. **Serving Layer**: Deploy MongoDB and create query APIs
4. **Dashboard**: Deploy Grafana/custom dashboard to visualize results

---

## Quick Reference Commands

```powershell
# Start everything
minikube start --cpus=4 --memory=8192 --driver=docker
kubectl apply -f kafka/storage-class.yaml -n kafka
kubectl apply -f kafka/deployment.yaml -n kafka
kubectl apply -f minio/deployment.yaml -n minio
kubectl apply -f kafka/topics.yaml -n kafka

# Port forwards (3 terminals)
kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka
kubectl port-forward service/minio 9000:9000 -n minio
kubectl port-forward service/minio 9001:9001 -n minio

# Run producer
python producer.py

# Run ingestion
python minio_ingest.py

# Check data
mc ls --recursive minikube/bucket-0/master_dataset/

# Stop everything
minikube stop
```

---

## Performance Expectations

- **Minikube Startup**: 1-2 minutes
- **Kafka Deployment**: 3-5 minutes
- **Topic Creation**: 10-30 seconds
- **Producer Rate**: ~100-500 events/second
- **Ingestion Batch**: Every 1 minute
- **First Data in MinIO**: 2-3 minutes after starting producer

---

## Contact & Support

If you encounter issues:
1. Check logs: `kubectl logs <pod-name> -n kafka`
2. Check events: `kubectl get events -n kafka`
3. Verify port-forwards are running
4. Ensure producer.py and minio_ingest.py use `localhost:9092` for Kafka
5. Verify MinIO credentials match in ingestion script

**Good luck with your testing!** 🚀
