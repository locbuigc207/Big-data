#!/bin/bash

# Deploy Batch Processing Layer
# Usage: ./deploy-batch-processing.sh

set -e

echo "================================================"
echo "  Deploying Batch Processing Layer"
echo "================================================"

NAMESPACE="bigdata"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Step 1: Verify prerequisites
echo -e "\n${YELLOW}[1/5] Verifying prerequisites...${NC}"

# Check if namespace exists
if ! kubectl get namespace $NAMESPACE &> /dev/null; then
    echo -e "${RED}Error: Namespace $NAMESPACE does not exist${NC}"
    echo "Please deploy infrastructure first: ./deploy-batch-system.sh"
    exit 1
fi

# Check if HDFS is running
if ! kubectl get statefulset hdfs-namenode -n $NAMESPACE &> /dev/null; then
    echo -e "${RED}Error: HDFS not deployed${NC}"
    exit 1
fi

# Check if batch ingestion has run
echo "Checking if batch ingestion has completed..."
kubectl exec -n $NAMESPACE hdfs-namenode-0 -- hdfs dfs -test -d /raw/students/$(date +%Y-%m-%d) 2>/dev/null

if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Warning: No ingested data found for today${NC}"
    echo "Run batch ingestion first or use: kubectl exec hdfs-namenode-0 -n bigdata -- hdfs dfs -ls /raw"
fi

echo -e "${GREEN}✓ Prerequisites verified${NC}"

# Step 2: Create ConfigMap from Python script
echo -e "\n${YELLOW}[2/5] Creating ConfigMap for batch processing script...${NC}"

if [ ! -f "${SCRIPT_DIR}/batch_processing_complete.py" ]; then
    echo -e "${RED}Error: batch_processing_complete.py not found${NC}"
    exit 1
fi

kubectl create configmap batch-processing-scripts \
    --from-file=${SCRIPT_DIR}/batch_processing_complete.py \
    -n $NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

echo -e "${GREEN}✓ ConfigMap created${NC}"

# Step 3: Deploy batch processing jobs
echo -e "\n${YELLOW}[3/5] Deploying batch processing jobs...${NC}"

kubectl apply -f ${SCRIPT_DIR}/batch-processing-job.yaml

echo -e "${GREEN}✓ Jobs deployed${NC}"

# Step 4: Run manual test (optional)
echo -e "\n${YELLOW}[4/5] Would you like to run a manual test? (y/n)${NC}"
read -r response

if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "Starting manual batch processing job..."
    
    # Delete old job if exists
    kubectl delete job batch-processing-manual -n $NAMESPACE 2>/dev/null || true
    sleep 2
    
    # Create new job
    kubectl create job --from=cronjob/batch-processing-daily \
        batch-processing-manual-$(date +%s) -n $NAMESPACE
    
    echo ""
    echo "Job started. Monitor with:"
    echo "  kubectl logs -f job/batch-processing-manual-$(date +%s) -n $NAMESPACE"
    echo ""
    
    # Wait a moment and show logs
    sleep 5
    kubectl logs -f -l app=batch-processing,run=manual -n $NAMESPACE --tail=50 || true
fi

# Step 5: Show status and next steps
echo -e "\n${YELLOW}[5/5] Deployment Summary${NC}"
echo "================================================"
echo -e "${GREEN}✓ Batch Processing Layer Deployed${NC}"
echo "================================================"

echo ""
echo "CronJob Schedule:"
kubectl get cronjob -n $NAMESPACE -o wide

echo ""
echo "Available Commands:"
echo "----------------------------------------"
echo "1. Run manual processing:"
echo "   kubectl create job --from=cronjob/batch-processing-daily test-job-\$(date +%s) -n bigdata"
echo ""
echo "2. Monitor job:"
echo "   kubectl get jobs -n bigdata -w"
echo "   kubectl logs -f job/<job-name> -n bigdata"
echo ""
echo "3. Check processed data in HDFS:"
echo "   kubectl exec -it hdfs-namenode-0 -n bigdata -- bash"
echo "   hdfs dfs -ls /views/batch/\$(date +%Y-%m-%d)/"
echo ""
echo "4. View batch views:"
echo "   hdfs dfs -ls /views/batch/\$(date +%Y-%m-%d)/student_gpa"
echo "   hdfs dfs -ls /views/batch/\$(date +%Y-%m-%d)/at_risk_students"
echo ""
echo "5. Delete a job:"
echo "   kubectl delete job <job-name> -n bigdata"
echo ""
echo "6. View CronJob schedule:"
echo "   kubectl get cronjob -n bigdata"
echo ""
echo "7. Suspend/Resume CronJob:"
echo "   kubectl patch cronjob batch-processing-daily -n bigdata -p '{\"spec\":{\"suspend\":true}}'"
echo "   kubectl patch cronjob batch-processing-daily -n bigdata -p '{\"spec\":{\"suspend\":false}}'"

echo ""
echo "================================================"
echo -e "${GREEN}Deployment Complete!${NC}"
echo "================================================"