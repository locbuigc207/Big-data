#!/bin/bash

# ML Training Monitor - Real-time monitoring of ML pipeline jobs
# Usage: ./monitor-ml-training.sh

NAMESPACE="bigdata"
REFRESH_INTERVAL=15

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo "================================================"
echo "  ML Training Pipeline Monitor"
echo "================================================"
echo "Namespace: ${NAMESPACE}"
echo "Refresh: ${REFRESH_INTERVAL}s"
echo "Press Ctrl+C to exit"
echo ""
sleep 2

# Function to get job status with colors
get_job_status() {
    echo -e "${CYAN}Active ML Jobs:${NC}"
    kubectl get jobs -n ${NAMESPACE} -l app=ml-pipeline \
        --sort-by=.metadata.creationTimestamp \
        -o custom-columns=\
NAME:.metadata.name,\
COMPONENT:.metadata.labels.component,\
COMPLETIONS:.status.succeeded,\
ACTIVE:.status.active,\
FAILED:.status.failed,\
DURATION:.status.completionTime,\
AGE:.metadata.creationTimestamp 2>/dev/null | tail -10
}

# Function to get pod status
get_pod_status() {
    echo -e "${CYAN}Active Pods:${NC}"
    kubectl get pods -n ${NAMESPACE} -l app=ml-pipeline \
        --sort-by=.metadata.creationTimestamp \
        -o custom-columns=\
NAME:.metadata.name,\
STATUS:.status.phase,\
RESTARTS:.status.containerStatuses[0].restartCount,\
NODE:.spec.nodeName 2>/dev/null | tail -10
}

# Function to get resource usage
get_resource_usage() {
    echo -e "${CYAN}Resource Usage:${NC}"
    kubectl top pods -n ${NAMESPACE} -l app=ml-pipeline 2>/dev/null || echo "Metrics not available (install metrics-server)"
}

# Function to check HDFS models
check_hdfs_models() {
    echo -e "${CYAN}Trained Models in HDFS:${NC}"
    kubectl exec -n ${NAMESPACE} hdfs-namenode-0 -- \
        hdfs dfs -ls /models/ 2>/dev/null | tail -5 || echo "No models found or HDFS unavailable"
}

# Function to check ML features
check_ml_features() {
    echo -e "${CYAN}ML Features Status:${NC}"
    local today=$(date +%Y-%m-%d)
    kubectl exec -n ${NAMESPACE} hdfs-namenode-0 -- \
        hdfs dfs -du -s -h /ml_features/${today} 2>/dev/null || echo "No features for today"
}

# Function to get latest logs from each component
get_component_logs() {
    local component=$1
    local lines=${2:-10}
    
    local pod=$(kubectl get pods -n ${NAMESPACE} \
        -l app=ml-pipeline,component=${component} \
        --sort-by=.metadata.creationTimestamp \
        -o name 2>/dev/null | tail -1)
    
    if [ -n "$pod" ]; then
        kubectl logs -n ${NAMESPACE} ${pod} --tail=${lines} 2>/dev/null || echo "No logs available"
    else
        echo "No active pods for ${component}"
    fi
}

# Function to get CronJob status
get_cronjob_status() {
    echo -e "${CYAN}Scheduled Training (CronJobs):${NC}"
    kubectl get cronjob -n ${NAMESPACE} -l app=ml-pipeline \
        -o custom-columns=\
NAME:.metadata.name,\
SCHEDULE:.spec.schedule,\
SUSPEND:.spec.suspend,\
ACTIVE:.status.active,\
LAST_SCHEDULE:.status.lastScheduleTime 2>/dev/null || echo "No CronJobs found"
}

# Function to get job completion stats
get_completion_stats() {
    echo -e "${CYAN}Job Completion Statistics:${NC}"
    
    local total=$(kubectl get jobs -n ${NAMESPACE} -l app=ml-pipeline -o json 2>/dev/null | jq '.items | length')
    local succeeded=$(kubectl get jobs -n ${NAMESPACE} -l app=ml-pipeline -o json 2>/dev/null | jq '[.items[] | select(.status.succeeded == 1)] | length')
    local failed=$(kubectl get jobs -n ${NAMESPACE} -l app=ml-pipeline -o json 2>/dev/null | jq '[.items[] | select(.status.failed != null)] | length')
    local active=$(kubectl get jobs -n ${NAMESPACE} -l app=ml-pipeline -o json 2>/dev/null | jq '[.items[] | select(.status.active != null)] | length')
    
    if [ "$total" != "null" ] && [ "$total" != "0" ]; then
        echo "  Total Jobs:     ${total}"
        echo "  Succeeded:      ${succeeded}"
        echo "  Failed:         ${failed}"
        echo "  Active:         ${active}"
        
        if [ "$succeeded" != "null" ] && [ "$total" != "0" ]; then
            local success_rate=$(echo "scale=2; ${succeeded} * 100 / ${total}" | bc 2>/dev/null)
            echo "  Success Rate:   ${success_rate}%"
        fi
    else
        echo "  No jobs found"
    fi
}

# Function to display summary
display_summary() {
    clear
    
    echo "================================================"
    echo "  ML Training Pipeline Monitor"
    echo "  Time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "================================================"
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " JOBS STATUS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    get_job_status
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " PODS STATUS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    get_pod_status
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " RESOURCE USAGE"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    get_resource_usage
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " COMPLETION STATISTICS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    get_completion_stats
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " SCHEDULED TRAINING"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    get_cronjob_status
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " HDFS STORAGE"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    check_ml_features
    echo ""
    check_hdfs_models
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " LATEST LOGS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    echo ""
    echo -e "${YELLOW}Feature Engineering:${NC}"
    get_component_logs "feature-engineering" 5
    
    echo ""
    echo -e "${YELLOW}GPA Training:${NC}"
    get_component_logs "gpa-training" 5
    
    echo ""
    echo -e "${YELLOW}Dropout Training:${NC}"
    get_component_logs "dropout-training" 5
    
    echo ""
    echo "================================================"
    echo -e "Refreshing in ${REFRESH_INTERVAL}s... (Ctrl+C to exit)"
    echo "================================================"
}

# Handle Ctrl+C gracefully
trap cleanup SIGINT

cleanup() {
    echo ""
    echo ""
    echo "================================================"
    echo "  Monitor stopped"
    echo "================================================"
    exit 0
}

# Main monitoring loop
while true; do
    display_summary
    sleep ${REFRESH_INTERVAL}
done