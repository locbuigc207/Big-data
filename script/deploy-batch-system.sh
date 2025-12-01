#!/bin/bash

# Deployment script for Batch Processing System on Kubernetes
# Author: BTLbigdata-group30


set -e

echo "================================================"
echo "  Deploying Batch Processing System to K8s"
echo "================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Create namespace
echo -e "\n${YELLOW}[1/8] Creating namespace...${NC}"
kubectl create namespace bigdata --dry-run=client -o yaml | kubectl apply -f -

# Step 2: Install Strimzi operator for Kafka
echo -e "\n${YELLOW}[2/8] Installing Strimzi Kafka operator...${NC}"
kubectl create -f 'https://strimzi.io/install/latest?namespace=bigdata' -n bigdata || true
sleep 10

# Step 3: Deploy HDFS
echo -e "\n${YELLOW}[3/8] Deploying HDFS cluster...${NC}"
kubectl apply -f hdfs-complete-deployment.yaml

# Wait for HDFS to be ready
echo "Waiting for HDFS NameNode to be ready..."
kubectl wait --for=condition=ready pod -l app=hdfs-namenode -n bigdata --timeout=300s

# Step 4: Deploy Kafka
echo -e "\n${YELLOW}[4/8] Deploying Kafka cluster...${NC}"
kubectl apply -f kafka-deployment.yaml

# Wait for Kafka
echo "Waiting for Kafka cluster to be ready..."
kubectl wait kafka/edu-kafka --for=condition=Ready --timeout=600s -n bigdata

# Step 5: Deploy MongoDB and PostgreSQL
echo -e "\n${YELLOW}[5/8] Deploying databases...${NC}"
# Already included in hdfs-complete-deployment.yaml

# Step 6: Create PVC for data
echo -e "\n${YELLOW}[6/8] Creating PVC for CSV data...${NC}"
kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: batch-data-pvc
  namespace: bigdata
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
EOF

# Step 7: Upload Python script to ConfigMap
echo -e "\n${YELLOW}[7/8] Creating ConfigMap for scripts...${NC}"
kubectl create configmap batch-scripts \
  --from-file=batch_ingestion_k8s.py \
  -n bigdata \
  --dry-run=client -o yaml | kubectl apply -f -

# Step 8: Deploy batch job
echo -e "\n${YELLOW}[8/8] Deploying batch ingestion job...${NC}"
kubectl apply -f batch-ingestion-job.yaml

echo -e "\n${GREEN}================================================${NC}"
echo -e "${GREEN}  Deployment Complete!${NC}"
echo -e "${GREEN}================================================${NC}"

echo -e "\n${YELLOW}Next steps:${NC}"
echo "1. Upload CSV files to PVC:"
echo "   kubectl cp <local-data-folder> bigdata/<upload-pod>:/data"
echo ""
echo "2. Check HDFS UI:"
echo "   kubectl port-forward svc/hdfs-namenode -n bigdata 30870:9870"
echo "   Open: http://localhost:30870"
echo ""
echo "3. Run manual batch ingestion:"
echo "   kubectl apply -f batch-ingestion-job.yaml"
echo ""
echo "4. Check job logs:"
echo "   kubectl logs -f job/batch-ingestion-manual -n bigdata"
echo ""
echo "5. Verify data in HDFS:"
echo "   kubectl exec -it hdfs-namenode-0 -n bigdata -- hdfs dfs -ls /raw"

echo -e "\n${YELLOW}Useful commands:${NC}"
echo "- Get all pods:    kubectl get pods -n bigdata"
echo "- Get all jobs:    kubectl get jobs -n bigdata"
echo "- Get all pvc:     kubectl get pvc -n bigdata"
echo "- Describe pod:    kubectl describe pod <pod-name> -n bigdata"
echo "- Delete all:      kubectl delete namespace bigdata"