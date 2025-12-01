#!/bin/bash

# Test Batch Processing Results
# Usage: ./test-batch-processing.sh

set -e

NAMESPACE="bigdata"
PROCESSING_DATE=$(date +%Y-%m-%d)

echo "================================================"
echo "  Testing Batch Processing Results"
echo "  Date: $PROCESSING_DATE"
echo "================================================"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Function to run HDFS command
run_hdfs_cmd() {
    kubectl exec -n $NAMESPACE hdfs-namenode-0 -- $@
}

# Function to check if path exists
check_hdfs_path() {
    local path=$1
    local name=$2
    
    if run_hdfs_cmd hdfs dfs -test -d $path 2>/dev/null; then
        echo -e "${GREEN}✓${NC} $name exists"
        
        # Get file count and size
        local stats=$(run_hdfs_cmd hdfs dfs -du -s -h $path 2>/dev/null | tail -1)
        echo "  Size: $stats"
        
        # Count files
        local file_count=$(run_hdfs_cmd hdfs dfs -ls -R $path 2>/dev/null | grep -c "^-" || echo "0")
        echo "  Files: $file_count"
        
        return 0
    else
        echo -e "${RED}✗${NC} $name NOT FOUND"
        return 1
    fi
}

# Test 1: Check if batch processing has run
echo -e "\n${YELLOW}Test 1: Checking batch views directory${NC}"
echo "----------------------------------------"

BASE_PATH="/views/batch/$PROCESSING_DATE"

if check_hdfs_path "$BASE_PATH" "Batch views directory"; then
    echo -e "${GREEN}Batch processing has run for $PROCESSING_DATE${NC}"
else
    echo -e "${RED}Batch processing has NOT run for $PROCESSING_DATE${NC}"
    echo ""
    echo "Available dates:"
    run_hdfs_cmd hdfs dfs -ls /views/batch/ 2>/dev/null || echo "No batch views found"
    exit 1
fi

# Test 2: Check all expected views
echo -e "\n${YELLOW}Test 2: Verifying all batch views${NC}"
echo "----------------------------------------"

VIEWS=(
    "student_gpa"
    "class_rankings"
    "attendance_metrics"
    "course_statistics"
    "faculty_performance"
    "at_risk_students"
    "student_profile_view"
    "student_distribution_pivot"
    "course_performance_pivot"
    "course_cube"
)

PASSED=0
FAILED=0

for view in "${VIEWS[@]}"; do
    if check_hdfs_path "$BASE_PATH/$view" "$view"; then
        ((PASSED++))
    else
        ((FAILED++))
    fi
    echo ""
done

echo "Summary: $PASSED passed, $FAILED failed"

# Test 3: Sample data from key views
echo -e "\n${YELLOW}Test 3: Sampling data from key views${NC}"
echo "----------------------------------------"

echo "Sample from student_gpa:"
run_hdfs_cmd hdfs dfs -cat "$BASE_PATH/student_gpa/semester=*/part-00000*.parquet" 2>/dev/null | head -c 500 || echo "Unable to sample"

echo ""
echo ""
echo "Sample from at_risk_students:"
run_hdfs_cmd hdfs dfs -cat "$BASE_PATH/at_risk_students/semester=*/overall_risk_level=*/part-00000*.parquet" 2>/dev/null | head -c 500 || echo "Unable to sample"

# Test 4: Check partitioning
echo -e "\n${YELLOW}Test 4: Verifying data partitioning${NC}"
echo "----------------------------------------"

echo "Student GPA partitions (by semester):"
run_hdfs_cmd hdfs dfs -ls "$BASE_PATH/student_gpa/" 2>/dev/null | grep "semester=" || echo "No partitions found"

echo ""
echo "At-Risk Students partitions (by semester and risk level):"
run_hdfs_cmd hdfs dfs -ls "$BASE_PATH/at_risk_students/" 2>/dev/null | grep "semester=" || echo "No partitions found"

# Test 5: Check file formats and compression
echo -e "\n${YELLOW}Test 5: Checking file formats${NC}"
echo "----------------------------------------"

echo "File types in student_gpa:"
run_hdfs_cmd hdfs dfs -ls -R "$BASE_PATH/student_gpa/" 2>/dev/null | grep -E "\.parquet$|\.snappy$" | head -5

# Test 6: Performance metrics
echo -e "\n${YELLOW}Test 6: Storage metrics${NC}"
echo "----------------------------------------"

echo "Total size of batch views:"
run_hdfs_cmd hdfs dfs -du -s -h "$BASE_PATH" 2>/dev/null

echo ""
echo "Size by view:"
for view in "${VIEWS[@]}"; do
    if run_hdfs_cmd hdfs dfs -test -d "$BASE_PATH/$view" 2>/dev/null; then
        size=$(run_hdfs_cmd hdfs dfs -du -s -h "$BASE_PATH/$view" 2>/dev/null | tail -1 | awk '{print $1 " " $2}')
        printf "  %-30s: %s\n" "$view" "$size"
    fi
done

# Test 7: Check job history
echo -e "\n${YELLOW}Test 7: Recent batch processing jobs${NC}"
echo "----------------------------------------"

echo "Recent jobs:"
kubectl get jobs -n $NAMESPACE --sort-by=.metadata.creationTimestamp | grep batch-processing | tail -5

echo ""
echo "Last job status:"
kubectl get jobs -n $NAMESPACE -l app=batch-processing --sort-by=.metadata.creationTimestamp -o wide | tail -2

# Test 8: Check CronJob configuration
echo -e "\n${YELLOW}Test 8: CronJob configuration${NC}"
echo "----------------------------------------"

kubectl get cronjob batch-processing-daily -n $NAMESPACE -o yaml | grep -A 5 "schedule:"

# Final summary
echo ""
echo "================================================"
echo "  Test Summary"
echo "================================================"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo ""
    echo "Batch processing is working correctly."
    echo ""
    echo "Next steps:"
    echo "1. Query the data:"
    echo "   kubectl exec -it hdfs-namenode-0 -n bigdata -- bash"
    echo "   hdfs dfs -cat /views/batch/$PROCESSING_DATE/student_gpa/semester=*/part-*.parquet | head"
    echo ""
    echo "2. Run Spark SQL queries (next phase)"
    echo "3. Export to serving layer (MongoDB, PostgreSQL)"
else
    echo -e "${RED}✗ Some tests failed${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "1. Check job logs:"
    echo "   kubectl logs -l app=batch-processing -n bigdata --tail=100"
    echo ""
    echo "2. Check HDFS health:"
    echo "   kubectl exec hdfs-namenode-0 -n bigdata -- hdfs dfsadmin -report"
    echo ""
    echo "3. Re-run batch processing:"
    echo "   kubectl create job --from=cronjob/batch-processing-daily test-\$(date +%s) -n bigdata"
fi

echo "================================================"