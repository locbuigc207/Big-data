# 🚀 Minikube Quick Reference

## Essential Commands

### Cluster Management
```powershell
# Start cluster
minikube start --cpus=4 --memory=8192 --driver=docker

# Check status
minikube status

# Stop cluster (preserves data)
minikube stop

# Delete cluster (removes all data)
minikube delete

# Access Kubernetes dashboard
minikube dashboard

# SSH into Minikube node
minikube ssh
```

### kubectl Commands
```powershell
# Get all resources in namespace
kubectl get all -n kafka
kubectl get all -n minio

# Get specific resources
kubectl get pods -n kafka
kubectl get services -n kafka
kubectl get kafkatopics -n kafka

# Describe resource (detailed info)
kubectl describe pod <pod-name> -n kafka

# View logs
kubectl logs <pod-name> -n kafka
kubectl logs <pod-name> -n kafka -f  # Follow logs

# Execute command in pod
kubectl exec -it <pod-name> -n kafka -- bash

# Delete resource
kubectl delete pod <pod-name> -n kafka
```

### Port Forwarding
```powershell
# Kafka
kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka

# MinIO API
kubectl port-forward service/minio 9000:9000 -n minio

# MinIO Console
kubectl port-forward service/minio 9001:9001 -n minio

# List all port-forwards
Get-Job | Where-Object { $_.Command -like "*kubectl port-forward*" }

# Stop all port-forwards
Get-Job | Stop-Job; Get-Job | Remove-Job
```

## Testing Workflow

### 1. Deploy Infrastructure (One Time)
```powershell
.\deploy_minikube.ps1
```

### 2. Start Port Forwards (3 Terminals)
```powershell
# Terminal 1
kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka

# Terminal 2
kubectl port-forward service/minio 9000:9000 -n minio

# Terminal 3
kubectl port-forward service/minio 9001:9001 -n minio
```

### 3. Run Producer (Terminal 4)
```powershell
python producer.py
```

### 4. Run Ingestion (Terminal 5)
```powershell
python minio_ingest_k8s.py
```

### 5. Verify Data
```powershell
# MinIO Console: http://localhost:9001
# Username: minioadmin
# Password: minioadmin

# Or use CLI
mc ls --recursive minikube/bucket-0/master_dataset/
```

## Troubleshooting

### Check Kafka Status
```powershell
# All Kafka resources
kubectl get kafka,kafkatopic,kafkauser -n kafka

# Kafka pods
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster

# Kafka logs
kubectl logs kafka-cluster-kafka-0 -n kafka

# Test Kafka connection
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Check MinIO Status
```powershell
# MinIO pods
kubectl get pods -n minio

# MinIO logs
kubectl logs deployment/minio -n minio

# Test MinIO connection
mc admin info minikube
```

### Common Issues

#### Producer can't connect
```powershell
# Check port-forward is running
Get-Job | Where-Object { $_.Command -like "*9092*" }

# Test connection
Test-NetConnection localhost -Port 9092
```

#### Ingestion not writing data
```powershell
# Check ingestion logs
# (if running locally, check terminal output)

# Verify MinIO bucket exists
mc ls minikube

# Check topics have messages
kubectl exec kafka-cluster-kafka-0 -n kafka -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic auth_topic \
  --from-beginning \
  --max-messages 5
```

#### Pods not starting
```powershell
# Check pod status
kubectl get pods -n kafka
kubectl describe pod <pod-name> -n kafka

# Check events
kubectl get events -n kafka --sort-by='.lastTimestamp'

# Check persistent volumes
kubectl get pv
kubectl get pvc -n kafka
```

#### Out of resources
```powershell
# Check Minikube resources
minikube status

# Increase resources
minikube stop
minikube delete
minikube start --cpus=6 --memory=12288 --driver=docker
```

## Data Verification

### Using MinIO Console (Web UI)
1. Open: http://localhost:9001
2. Login: `minioadmin` / `minioadmin`
3. Navigate: **Object Browser** → **bucket-0** → **master_dataset**
4. Check folders: `topic=auth_topic`, `topic=video_topic`, etc.

### Using MinIO Client (CLI)
```powershell
# List all objects
mc ls --recursive minikube/bucket-0/master_dataset/

# Check specific topic
mc ls minikube/bucket-0/master_dataset/topic=auth_topic/

# Download a file to inspect
mc cp minikube/bucket-0/master_dataset/topic=auth_topic/part-00000.parquet ./test.parquet
```

### Using Kafka Console Consumer
```powershell
# Connect to Kafka pod
kubectl exec -it kafka-cluster-kafka-0 -n kafka -- bash

# Consume messages from topic
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic auth_topic \
  --from-beginning \
  --max-messages 10

# Check topic details
bin/kafka-topics.sh \
  --describe \
  --topic auth_topic \
  --bootstrap-server localhost:9092
```

## Performance Monitoring

### Resource Usage
```powershell
# Minikube resources
minikube status

# Kubernetes metrics
kubectl top nodes
kubectl top pods -n kafka
kubectl top pods -n minio
```

### Event Throughput
```powershell
# Check Kafka lag
kubectl exec kafka-cluster-kafka-0 -n kafka -- \
  bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --all-groups
```

## Cleanup

### Stop Everything (Keep Data)
```powershell
# Stop producer and ingestion (Ctrl+C)
# Stop port-forwards
Get-Job | Stop-Job; Get-Job | Remove-Job

# Stop Minikube
minikube stop
```

### Delete Everything
```powershell
.\cleanup_minikube.ps1
```

## Useful Aliases

Add to PowerShell profile (`$PROFILE`):
```powershell
function k { kubectl $args }
function kgp { kubectl get pods -n kafka }
function kgm { kubectl get pods -n minio }
function klogs { kubectl logs $args -n kafka }
function mkstart { minikube start --cpus=4 --memory=8192 --driver=docker }
function mkstop { minikube stop }
```

## Expected Timings

| Action | Duration |
|--------|----------|
| Minikube start | 1-2 minutes |
| Deploy Kafka | 3-5 minutes |
| Deploy MinIO | 30-60 seconds |
| Create topics | 10-30 seconds |
| First data in MinIO | 2-3 minutes after producer starts |
| Ingestion batch interval | 1 minute |

## Resource Requirements

| Component | CPU | Memory | Storage |
|-----------|-----|--------|---------|
| Minikube | 4 cores | 8GB | 20GB |
| Kafka (3 nodes) | 2 cores | 4GB | 60GB |
| MinIO | 500m | 512MB | 50GB |
| Ingestion | 1 core | 2GB | - |

## Architecture Flow

```
Producer (localhost:9092)
    ↓
Port Forward
    ↓
Kafka Cluster (3 brokers in Minikube)
    ├── auth_topic
    ├── assessment_topic
    ├── video_topic
    ├── course_topic
    ├── profile_topic
    └── notification_topic
    ↓
Ingestion Layer (PySpark)
    ↓
MinIO (S3-compatible storage in Minikube)
    └── bucket-0/master_dataset/
        ├── topic=auth_topic/*.parquet
        ├── topic=assessment_topic/*.parquet
        ├── topic=video_topic/*.parquet
        ├── topic=course_topic/*.parquet
        ├── topic=profile_topic/*.parquet
        └── topic=notification_topic/*.parquet
```

## Success Criteria

✅ Minikube running  
✅ 3 Kafka pods running  
✅ 1 MinIO pod running  
✅ 6 topics created  
✅ Producer sending events  
✅ Ingestion writing data every minute  
✅ Parquet files visible in MinIO  
✅ Data partitioned by topic  

---

**For full details, see: [MINIKUBE_TESTING_GUIDE.md](MINIKUBE_TESTING_GUIDE.md)**
