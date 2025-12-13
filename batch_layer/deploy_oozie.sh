#!/bin/bash
# Deploy Batch Layer to Oozie
# This script uploads batch jobs and Oozie workflow to HDFS/MinIO

set -e

# Configuration
HDFS_USER="batch_layer"
HDFS_BASE_PATH="/user/${HDFS_USER}"
LOCAL_BATCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================="
echo "Deploying Batch Layer to Oozie"
echo "========================================="
echo "Local directory: ${LOCAL_BATCH_DIR}"
echo "HDFS base path: ${HDFS_BASE_PATH}"
echo ""

# Create HDFS directories
echo "[1/5] Creating HDFS directory structure..."
hdfs dfs -mkdir -p ${HDFS_BASE_PATH}/batch_layer/jobs
hdfs dfs -mkdir -p ${HDFS_BASE_PATH}/batch_layer/oozie
hdfs dfs -mkdir -p ${HDFS_BASE_PATH}/batch_layer/lib

# Upload batch job Python files
echo "[2/5] Uploading batch job files..."
hdfs dfs -put -f ${LOCAL_BATCH_DIR}/jobs/*.py ${HDFS_BASE_PATH}/batch_layer/jobs/

# Upload Oozie workflow and coordinator
echo "[3/5] Uploading Oozie workflow definitions..."
hdfs dfs -put -f ${LOCAL_BATCH_DIR}/oozie/workflow.xml ${HDFS_BASE_PATH}/batch_layer/oozie/
hdfs dfs -put -f ${LOCAL_BATCH_DIR}/oozie/coordinator.xml ${HDFS_BASE_PATH}/batch_layer/oozie/
hdfs dfs -put -f ${LOCAL_BATCH_DIR}/oozie/job.properties ${HDFS_BASE_PATH}/batch_layer/oozie/

# Upload configuration
echo "[4/5] Uploading configuration files..."
hdfs dfs -put -f ${LOCAL_BATCH_DIR}/config.py ${HDFS_BASE_PATH}/batch_layer/

# Set permissions
echo "[5/5] Setting HDFS permissions..."
hdfs dfs -chmod -R 755 ${HDFS_BASE_PATH}/batch_layer

echo ""
echo "========================================="
echo "✅ Batch Layer deployed successfully!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Verify deployment:"
echo "   hdfs dfs -ls -R ${HDFS_BASE_PATH}/batch_layer"
echo ""
echo "2. Submit Oozie coordinator:"
echo "   oozie job -oozie http://localhost:11000/oozie -config ${LOCAL_BATCH_DIR}/oozie/job.properties -run"
echo ""
echo "3. Check job status:"
echo "   oozie job -oozie http://localhost:11000/oozie -info <job-id>"
echo ""
