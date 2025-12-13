# Testing Options Comparison

## Local Testing vs Minikube Testing

This document helps you choose the right testing approach for your needs.

---

## Overview

| Aspect | Local (Docker Compose) | Minikube (Kubernetes) |
|--------|------------------------|------------------------|
| **Setup Time** | 5 minutes | 15-20 minutes |
| **Complexity** | Simple | Moderate |
| **Resources** | 4GB RAM, 2 CPUs | 8GB RAM, 4 CPUs |
| **Production-like** | No | Yes |
| **Storage** | HDFS | MinIO (S3-compatible) |
| **Best For** | Quick development | Final testing & demo |

---

## Local Testing (Docker Compose)

### ✅ Advantages

- **Fast setup**: Just `docker-compose up -d`
- **Easy debugging**: Direct access to logs
- **Lower resource usage**: Runs on modest hardware
- **Familiar tools**: Standard Docker commands
- **Quick iterations**: Fast restart and testing cycles

### ❌ Disadvantages

- Not production-like
- Uses HDFS instead of cloud storage
- Single-node Kafka (no real distribution)
- Doesn't demonstrate K8s knowledge

### When to Use

- **Daily development**: Testing code changes quickly
- **Initial debugging**: Finding and fixing bugs
- **Learning phase**: Understanding how components work
- **Week 1-3**: Building and refining the pipeline

### How to Use

```powershell
# Start infrastructure
docker-compose up -d

# Wait 30 seconds
Start-Sleep -Seconds 30

# Run producer (Terminal 1)
python producer.py

# Run ingestion (Terminal 2)
python ingest_layer.py

# Verify HDFS
# Open browser: http://localhost:9870
# Navigate: Utilities → Browse the file system → /data/master_dataset

# Stop
docker-compose down
```

**Files used**: `ingest_layer.py` (writes to HDFS)

---

## Minikube Testing (Kubernetes)

### ✅ Advantages

- **Production-like**: Real Kubernetes deployment
- **Distributed**: 3-node Kafka cluster
- **Cloud storage**: MinIO (S3-compatible)
- **Demonstrates knowledge**: Shows K8s understanding
- **Scalable**: Can be adapted for real cloud deployment

### ❌ Disadvantages

- Longer setup time
- More complex debugging
- Higher resource requirements
- Slower iteration cycles

### When to Use

- **Final testing**: Before presentation/demo
- **Integration testing**: Full pipeline validation
- **Week 4-5**: Preparing for project submission
- **Demo preparation**: Showing production readiness
- **Professor requires K8s**: Meeting project requirements

### How to Use

```powershell
# Deploy infrastructure
.\deploy_minikube.ps1

# Start port forwards (3 terminals)
kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka
kubectl port-forward service/minio 9000:9000 -n minio
kubectl port-forward service/minio 9001:9001 -n minio

# Run producer (Terminal 4)
python producer.py

# Run ingestion (Terminal 5)
python minio_ingest_k8s.py

# Verify MinIO
# Open browser: http://localhost:9001
# Login: minioadmin/minioadmin
# Navigate: Object Browser → bucket-0 → master_dataset

# Cleanup
.\cleanup_minikube.ps1
```

**Files used**: `minio_ingest_k8s.py` (writes to MinIO)

---

## Recommended Testing Strategy

### Phase 1: Development (Week 1-3)

**Use**: Local (Docker Compose)

**Why**: Fast iterations, easy debugging

**Focus**:
- Get producer working
- Verify events in Kafka
- Test ingestion to HDFS
- Develop batch/stream processing layers
- Quick bug fixes

**Time per test cycle**: 2-3 minutes

### Phase 2: Integration (Week 4)

**Use**: Minikube (Kubernetes)

**Why**: Test full pipeline in production-like environment

**Focus**:
- Verify all components work together
- Test with realistic data volumes
- Check data partitioning and storage
- Identify performance issues
- Practice for demo

**Time per test cycle**: 10-15 minutes

### Phase 3: Demo Preparation (Week 5)

**Use**: Minikube (Kubernetes)

**Why**: Demonstrate production readiness

**Focus**:
- Stable, repeatable deployments
- Clear monitoring and logging
- Professional presentation
- Documentation
- Q&A preparation

---

## Component Comparison

### Producer (`producer.py`)

| Aspect | Local | Minikube |
|--------|-------|----------|
| Kafka Connection | `localhost:9092` | `localhost:9092` (via port-forward) |
| Topics | Same 6 topics | Same 6 topics |
| Events | Same events | Same events |
| Changes needed | None | None |

**Verdict**: Producer is identical for both environments ✅

### Ingestion Layer

| Aspect | Local | Minikube |
|--------|-------|----------|
| File | `ingest_layer.py` | `minio_ingest_k8s.py` |
| Kafka | `localhost:9092` | `localhost:9092` (via port-forward) |
| Storage | HDFS (`hdfs://namenode:9000`) | MinIO (`s3a://bucket-0`) |
| Protocol | HDFS | S3A |
| Config | Basic Spark | S3A credentials |

**Verdict**: Need different ingestion scripts ⚠️

### Verification

| Aspect | Local | Minikube |
|--------|-------|----------|
| Web UI | http://localhost:9870 | http://localhost:9001 |
| CLI | `docker exec namenode hdfs dfs -ls` | `mc ls --recursive minikube/bucket-0` |
| Data Format | Parquet on HDFS | Parquet in MinIO |
| Path | `/data/master_dataset/topic=...` | `bucket-0/master_dataset/topic=...` |

**Verdict**: Different tools, same data structure ✅

---

## File Structure

```
BTLbigdata-group30/
├── docker-compose.yml              # Local infrastructure
├── producer.py                     # Same for both
├── ingest_layer.py                # For local (HDFS)
├── minio_ingest_k8s.py            # For Minikube (MinIO)
│
├── kafka/                          # Kubernetes configs
│   ├── deployment.yaml             # Kafka cluster
│   ├── topics.yaml                 # Topic definitions
│   ├── storage-class.yaml          # Storage config
│   ├── persistent-volumn.yaml      # Multi-node PVs
│   └── persistent-volumn-minikube.yaml  # Single-node PVs
│
├── minio/
│   └── deployment.yaml             # MinIO deployment
│
├── spark/
│   └── ingestion-job.yaml          # Ingestion K8s job (optional)
│
├── deploy_minikube.ps1            # Quick deployment
├── cleanup_minikube.ps1           # Quick cleanup
├── MINIKUBE_TESTING_GUIDE.md      # Detailed guide
└── MINIKUBE_QUICK_REFERENCE.md    # Command reference
```

---

## Decision Matrix

### Choose Local if:
- ✅ You're developing new features
- ✅ You need quick feedback
- ✅ You're debugging issues
- ✅ Resources are limited
- ✅ Week 1-3 of development

### Choose Minikube if:
- ✅ Testing before demo
- ✅ Demonstrating K8s knowledge
- ✅ Professor requires K8s
- ✅ Integration testing
- ✅ Week 4-5 (final prep)

---

## Migration Path

### From Local to Minikube

1. **Ensure local setup works completely**
   ```powershell
   docker-compose up -d
   python producer.py
   python ingest_layer.py
   # Verify data in HDFS
   ```

2. **Deploy Minikube**
   ```powershell
   .\deploy_minikube.ps1
   ```

3. **Setup port forwards**
   ```powershell
   # 3 terminals as shown above
   ```

4. **Test with Minikube ingestion**
   ```powershell
   python producer.py
   python minio_ingest_k8s.py
   # Verify data in MinIO
   ```

5. **Update batch/stream layers**
   - Change input path from HDFS to MinIO
   - Update S3A configurations
   - Test end-to-end pipeline

### From Minikube to Cloud (Future)

1. Replace Minikube with real K8s cluster (GKE, EKS, AKS)
2. Replace MinIO with cloud object storage (S3, GCS, Azure Blob)
3. Use managed Kafka (Confluent Cloud, MSK, Event Hubs)
4. No code changes needed (just config)!

---

## Performance Comparison

### Startup Time

| Component | Local | Minikube |
|-----------|-------|----------|
| Infrastructure | 1 minute | 5 minutes |
| Kafka ready | 30 seconds | 3 minutes |
| Storage ready | Instant (HDFS) | 1 minute (MinIO) |
| Total | ~2 minutes | ~10 minutes |

### Data Throughput

| Metric | Local | Minikube |
|--------|-------|----------|
| Producer rate | ~500 events/sec | ~500 events/sec |
| Ingestion batch | 1 minute | 1 minute |
| Write latency | Low (HDFS) | Medium (MinIO) |

### Resource Usage

| Resource | Local | Minikube |
|----------|-------|----------|
| Docker containers | 5-6 | Minikube VM |
| CPU | 2 cores | 4 cores |
| Memory | 4GB | 8GB |
| Disk | 10GB | 20GB |

---

## Troubleshooting Strategy

### Local Issues
1. Check Docker containers: `docker ps`
2. Check logs: `docker logs <container>`
3. Restart: `docker-compose restart`
4. Clean start: `docker-compose down -v && docker-compose up -d`

### Minikube Issues
1. Check pods: `kubectl get pods -n kafka`
2. Check logs: `kubectl logs <pod> -n kafka`
3. Check events: `kubectl get events -n kafka`
4. Restart: `minikube stop && minikube start`
5. Clean start: `minikube delete && .\deploy_minikube.ps1`

---

## Cost Analysis

| Aspect | Local | Minikube | Cloud |
|--------|-------|----------|-------|
| Infrastructure | Free | Free | $$$ |
| Development time | Low | Medium | High |
| Learning curve | Low | Medium | High |
| Operational overhead | Low | Medium | High |
| Demo readiness | Medium | High | Highest |

**Recommendation**: Use Local for development, Minikube for testing/demo

---

## Summary

### Quick Reference

```
Development Phase (Week 1-3):
  → Use docker-compose (Local)
  → File: ingest_layer.py
  → Storage: HDFS
  → Verify: http://localhost:9870

Testing Phase (Week 4):
  → Use Minikube (Kubernetes)
  → File: minio_ingest_k8s.py
  → Storage: MinIO
  → Verify: http://localhost:9001

Demo Phase (Week 5):
  → Use Minikube (Kubernetes)
  → Prepare scripts: deploy_minikube.ps1
  → Practice full flow
  → Document architecture
```

---

## Questions?

**Q: Can I use both simultaneously?**  
A: Yes, but not on the same ports. Stop docker-compose before starting Minikube.

**Q: Which one for the final demo?**  
A: Minikube shows more sophistication and K8s knowledge.

**Q: What if Minikube is too slow on my laptop?**  
A: Use Local for development, Minikube only for final testing.

**Q: Do I need to change batch_layer.py?**  
A: Yes, update input paths from HDFS to MinIO when using Minikube.

**Q: Can I skip Minikube?**  
A: Yes, if professor doesn't require K8s. Local is sufficient for core functionality.

---

## Next Steps

1. **Start with Local**: Get comfortable with the pipeline
2. **Test on Minikube**: Validate before demo (Week 4-5)
3. **Document both**: Show you understand both approaches
4. **Prepare presentation**: Explain trade-offs and decisions

**Good luck! 🚀**
