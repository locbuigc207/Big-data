#!/bin/bash

# Monitor Batch Processing in Real-time
# Usage: ./monitor-batch-processing.sh

NAMESPACE="bigdata"

echo "================================================"
echo "  Batch Processing Monitor"
echo "================================================"
echo "Press Ctrl+C to exit"
echo ""

# Function to get job status
get_job_status() {
    kubectl get jobs -n $NAMESPACE -l app=batch-processing \
        --sort-by=.metadata.creationTimestamp \
        -o custom-columns=NAME:.metadata.name,COMPLETIONS:.status.succeeded,DURATION:.status.completionTime,STATUS:.status.conditions[0].type \
        | tail -5
}

# Function to get pod status
get_pod_status() {
    kubectl get pods -n $NAMESPACE -l app=batch-processing \
        --sort-by=.metadata.creationTimestamp \
        -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName \
        | tail -5
}

# Function to get resource usage
get_resource_usage() {
    kubectl top pods -n $NAMESPACE -l app=batch-processing 2>/dev/null || echo "Metrics not available"
}

# Function to get HDFS usage
get_hdfs_usage() {
    kubectl exec -n $NAMESPACE hdfs-namenode-0 -- hdfs dfs -du -s -h /views/batch 2>/dev/null || echo "N/A"
}

# Function to get latest logs
get_latest_logs() {
    local pod=$(kubectl get pods -n $NAMESPACE -l app=batch-processing --sort-by=.metadata.creationTimestamp -o name | tail -1)
    if [ -n "$pod" ]; then
        kubectl logs -n $NAMESPACE $pod --tail=10 2>/dev/null || echo "No logs available"
    else
        echo "No active pods"
    fi
}

# Main monitoring loop
while true; do
    clear
    echo "================================================"
    echo "  Batch Processing Monitor"
    echo "  Time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "================================================"
    
    echo ""
    echo "📊 Recent Jobs:"
    echo "----------------------------------------"
    get_job_status
    
    echo ""
    echo "🔄 Active Pods:"
    echo "----------------------------------------"
    get_pod_status
    
    echo ""
    echo "💾 Resource Usage:"
    echo "----------------------------------------"
    get_resource_usage
    
    echo ""
    echo "📁 HDFS Storage:"
    echo "----------------------------------------"
    echo "Batch views size: $(get_hdfs_usage)"
    
    echo ""
    echo "📝 Latest Logs:"
    echo "----------------------------------------"
    get_latest_logs
    
    echo ""
    echo "================================================"
    echo "Refreshing in 10 seconds... (Ctrl+C to exit)"
    
    sleep 10
done