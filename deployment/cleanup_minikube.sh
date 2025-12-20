#!/bin/bash

# Cleanup Script for Minikube
# Usage: ./cleanup_minikube.sh [--delete-cluster]

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}================================${NC}"
echo -e "${CYAN}CLEANUP MINIKUBE RESOURCES      ${NC}"
echo -e "${CYAN}================================${NC}"
echo ""

# 1. Delete Application Layers
echo -e "${YELLOW}Deleting Speed & Serving Layers...${NC}"
kubectl delete deployment speed-layer serving-layer -n default --ignore-not-found=true
kubectl delete service serving-layer -n default --ignore-not-found=true
kubectl delete job batch-layer-job -n default --ignore-not-found=true
echo -e "${GREEN}✅ Application layers deleted${NC}"
echo ""

# 2. Delete MinIO and Kafka (and Persistent Volumes)
echo -e "${YELLOW}Deleting MinIO and Kafka...${NC}"
kubectl delete namespace minio --ignore-not-found=true
kubectl delete namespace kafka --ignore-not-found=true

# Delete PVs so they can be re-bound cleanly
kubectl delete pv pv-kafka-0 pv-kafka-1 pv-kafka-2 --ignore-not-found=true

# Cleanup physical data on Minikube host to prevent "Invalid cluster.id" errors
echo -e "${YELLOW}Cleaning up physical data on Minikube host...${NC}"
minikube ssh "sudo rm -rf /mnt/kafka-data/*"

# Wait for deletion or force it
echo -e "${YELLOW}Waiting for namespaces to terminate...${NC}"
sleep 10
if kubectl get ns kafka &> /dev/null; then
    echo -e "${RED}⚠️ Namespace 'kafka' is stuck Terminating. Forcing deletion...${NC}"
    kubectl get namespace kafka -o json | tr -d "\n" | sed "s/\"finalizers\": \[[^]]*\]/\"finalizers\": []/" | kubectl replace --raw /api/v1/namespaces/kafka/finalize -f -
fi
if kubectl get ns minio &> /dev/null; then
    echo -e "${RED}⚠️ Namespace 'minio' is stuck Terminating. Forcing deletion...${NC}"
    kubectl get namespace minio -o json | tr -d "\n" | sed "s/\"finalizers\": \[[^]]*\]/\"finalizers\": []/" | kubectl replace --raw /api/v1/namespaces/minio/finalize -f -
fi

echo -e "${GREEN}✅ Infrastructure namespaces deleted${NC}"
echo ""

# 3. Stop Minikube
echo -e "${YELLOW}Stopping Minikube...${NC}"
minikube stop
echo -e "${GREEN}✅ Minikube stopped${NC}"
echo ""

if [[ "$1" == "--delete-cluster" ]]; then
    echo -e "${YELLOW}Deleting Minikube cluster (FULL RESET)...${NC}"
    minikube delete
    echo -e "${GREEN}✅ Minikube cluster deleted${NC}"
else
    echo -e "${CYAN}Minikube cluster preserved. To delete completely, run:${NC}"
    echo -e "  ./cleanup_minikube.sh --delete-cluster"
fi

echo ""
echo -e "${GREEN}Cleanup complete! ✅${NC}"
