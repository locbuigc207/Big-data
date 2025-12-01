#!/bin/bash

# Script to upload CSV files to Kubernetes PVC
# Usage: ./upload-csv-data.sh <local-data-folder>

set -e

LOCAL_DATA_FOLDER=${1:-"./generate_fake_data"}
NAMESPACE="bigdata"
PVC_NAME="batch-data-pvc"

echo "================================================"
echo "  Uploading CSV Data to Kubernetes"
echo "================================================"

# Check if local folder exists
if [ ! -d "$LOCAL_DATA_FOLDER" ]; then
    echo "Error: Folder $LOCAL_DATA_FOLDER does not exist"
    exit 1
fi

# Required CSV files
REQUIRED_FILES=(
    "students.csv"
    "teachers.csv"
    "courses.csv"
    "classes.csv"
    "enrollments.csv"
    "grades.csv"
    "sessions.csv"
    "attendance.csv"
)

# Verify all files exist
echo "Checking required files..."
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$LOCAL_DATA_FOLDER/$file" ]; then
        echo "Error: Missing required file: $file"
        exit 1
    fi
    echo "✓ Found: $file"
done

# Create a temporary pod to mount the PVC
echo ""
echo "Creating temporary pod to mount PVC..."

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: data-uploader
  namespace: $NAMESPACE
spec:
  containers:
  - name: uploader
    image: busybox:latest
    command: ['sh', '-c', 'sleep 3600']
    volumeMounts:
    - name: data-volume
      mountPath: /data
  volumes:
  - name: data-volume
    persistentVolumeClaim:
      claimName: $PVC_NAME
  restartPolicy: Never
EOF

# Wait for pod to be ready
echo "Waiting for pod to be ready..."
kubectl wait --for=condition=ready pod/data-uploader -n $NAMESPACE --timeout=120s

# Upload each file
echo ""
echo "Uploading CSV files..."
for file in "${REQUIRED_FILES[@]}"; do
    echo "Uploading: $file"
    kubectl cp "$LOCAL_DATA_FOLDER/$file" "$NAMESPACE/data-uploader:/data/$file"
    echo "✓ Uploaded: $file"
done

# Verify uploads
echo ""
echo "Verifying uploads..."
kubectl exec data-uploader -n $NAMESPACE -- ls -lh /data

# Calculate total size
echo ""
echo "Total data size:"
kubectl exec data-uploader -n $NAMESPACE -- du -sh /data

echo ""
echo "================================================"
echo "  Upload Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo "1. Clean up the uploader pod:"
echo "   kubectl delete pod data-uploader -n $NAMESPACE"
echo ""
echo "2. Run the batch ingestion job:"
echo "   kubectl apply -f batch-ingestion-job.yaml"
echo ""
echo "3. Monitor the job:"
echo "   kubectl logs -f job/batch-ingestion-manual -n $NAMESPACE"

# Ask if user wants to delete the uploader pod
read -p "Delete uploader pod now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    kubectl delete pod data-uploader -n $NAMESPACE
    echo "✓ Uploader pod deleted"
fi