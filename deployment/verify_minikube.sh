#!/bin/bash

# Automated Verification Script for Lambda Architecture
# Usage: ./verify_minikube.sh

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}===================================${NC}"
echo -e "${CYAN}FULL SYSTEM VERIFICATION STARTING${NC}"
echo -e "${CYAN}===================================${NC}"
echo ""
echo -e "${YELLOW}Verifying Complete Ingestion Pipeline:${NC}"
echo "  Producer → Kafka → Batch Ingestion → Master Dataset"
echo "  Master Dataset → Batch Jobs → Batch Views → Serving API"
echo "  Kafka → Speed Layer → Speed Views → Serving API"
echo ""

# 1. Check Kafka Cluster
echo -e "${YELLOW}[1/8] Checking Kafka Cluster...${NC}"
KAFKA_READY=$(kubectl get kafka kafka-cluster -n kafka -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
if [ "$KAFKA_READY" == "True" ]; then
    echo -e "${GREEN}✅ Kafka Cluster is Ready${NC}"
else
    echo -e "${RED}❌ Kafka Cluster not ready${NC}"
fi
echo ""

# 2. Verify Kafka Topics Exist
echo -e "${YELLOW}[2/8] Verifying Kafka Topics...${NC}"
EXPECTED_TOPICS=("auth_topic" "course_topic" "video_topic" "assessment_topic" "profile_topic" "notification_topic")
KAFKA_POD=$(kubectl get pods -n kafka -l strimzi.io/name=kafka-cluster-kafka -o jsonpath='{.items[0].metadata.name}')

ALL_TOPICS_OK=true
for topic in "${EXPECTED_TOPICS[@]}"; do
    TOPIC_EXISTS=$(kubectl exec -it $KAFKA_POD -n kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -w "$topic" | wc -l)
    if [ "$TOPIC_EXISTS" -eq 1 ]; then
        echo -e "   ${GREEN}✓${NC} $topic"
    else
        echo -e "   ${RED}✗${NC} $topic (missing)"
        ALL_TOPICS_OK=false
    fi
done

if [ "$ALL_TOPICS_OK" = true ]; then
    echo -e "${GREEN}✅ All 6 Kafka topics exist${NC}"
else
    echo -e "${RED}❌ Some Kafka topics are missing${NC}"
fi
echo ""

# 3. Check Kafka Topic Messages
echo -e "${YELLOW}[3/8] Checking Kafka Topics for Messages...${NC}"
MSG_COUNT=$(kubectl exec -it $KAFKA_POD -n kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic course_topic \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 3000 2>/dev/null | grep -v "Processed" | wc -l)

if [ "$MSG_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ Kafka topics contain messages (Producer is working)${NC}"
else
    echo -e "${RED}❌ No messages found in Kafka topics (Producer may not be running)${NC}"
fi
echo ""

# 4. Check Pod Status
echo -e "${YELLOW}[4/8] Checking Application Pods...${NC}"
SPEED_POD=$(kubectl get pods -n default -l app=speed-layer -o jsonpath='{.items[0].status.phase}')
SERVING_POD=$(kubectl get pods -n default -l app=serving-layer -o jsonpath='{.items[0].status.phase}')
MINIO_POD=$(kubectl get pods -n minio -l app=minio -o jsonpath='{.items[0].status.phase}')

if [ "$SPEED_POD" == "Running" ] && [ "$SERVING_POD" == "Running" ] && [ "$MINIO_POD" == "Running" ]; then
    echo -e "${GREEN}✅ All Application Pods are Running${NC}"
    echo "   Speed Layer: $SPEED_POD"
    echo "   Serving Layer: $SERVING_POD"
    echo "   MinIO: $MINIO_POD"
else
    echo -e "${RED}❌ Some pods are not ready:${NC}"
    echo "   Speed Layer: $SPEED_POD"
    echo "   Serving Layer: $SERVING_POD"
    echo "   MinIO: $MINIO_POD"
fi
echo ""

# 5. Setup Port Forwarding for MinIO Tests
echo -e "${YELLOW}[5/8] Setting up temporary port-forwarding...${NC}"
kubectl port-forward service/minio 9000:9000 -n minio > /dev/null 2>&1 &
PF_PID=$!
sleep 5
echo -e "${GREEN}✅ Port-forwarding active${NC}"
echo ""

# 6. Verify MinIO Master Dataset (Raw Data Ingestion)
echo -e "${YELLOW}[6/8] Verifying Master Dataset Ingestion...${NC}"
if command -v mc &> /dev/null; then
    mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1
    MASTER_COUNT=$(mc ls --recursive minikube/bucket-0/master_dataset/ 2>/dev/null | wc -l)
    # Check for files modified in the last 5 minutes (indicates active ingestion)
    RECENT_FILES=$(mc ls --recursive minikube/bucket-0/master_dataset/ 2>/dev/null | \
        awk '{print $4, $5, $6}' | while read -r line; do
            file_date=$(date -d "$line" +%s 2>/dev/null || echo 0)
            now=$(date +%s)
            age=$((now - file_date))
            if [ $age -lt 300 ]; then  # Less than 5 minutes old
                echo "recent"
            fi
        done | wc -l)
else
    MASTER_COUNT=$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c \
        "mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1; \
         mc ls --recursive minikube/bucket-0/master_dataset/ 2>/dev/null | wc -l")
    # Check for recent files via docker
    RECENT_FILES=$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c \
        "mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1; \
         mc ls --recursive minikube/bucket-0/master_dataset/ 2>/dev/null" | \
        awk '{print $4, $5, $6}' | while read -r line; do
            file_date=$(date -d "$line" +%s 2>/dev/null || echo 0)
            now=$(date +%s)
            age=$((now - file_date))
            if [ $age -lt 300 ]; then
                echo "recent"
            fi
        done | wc -l)
fi

if [ "$MASTER_COUNT" -gt 0 ]; then
    if [ "$RECENT_FILES" -gt 0 ]; then
        echo -e "${GREEN}✅ Master Dataset Ingestion: $MASTER_COUNT total files, $RECENT_FILES modified in last 5 minutes${NC}"
    else
        echo -e "${YELLOW}⚠️  Master Dataset: $MASTER_COUNT files exist, but NONE modified recently (ingestion may be stalled)${NC}"
    fi
else
    echo -e "${RED}❌ Master Dataset Empty (Batch ingestion not running)${NC}"
fi
echo ""

# 7. Verify Batch Layer Views (Master Dataset → Batch Jobs → Batch Views)
echo -e "${YELLOW}[7/10] Verifying Batch Layer Views...${NC}"
if command -v mc &> /dev/null; then
    BATCH_COUNT=$(mc ls --recursive minikube/bucket-0/batch_views/ 2>/dev/null | wc -l)
    # Check for recent batch view files
    RECENT_BATCH=$(mc ls --recursive minikube/bucket-0/batch_views/ 2>/dev/null | \
        awk '{print $4, $5, $6}' | while read -r line; do
            file_date=$(date -d "$line" +%s 2>/dev/null || echo 0)
            now=$(date +%s)
            age=$((now - file_date))
            if [ $age -lt 3600 ]; then  # Less than 1 hour old
                echo "recent"
            fi
        done | wc -l)
else
    BATCH_COUNT=$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c \
        "mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1; \
         mc ls --recursive minikube/bucket-0/batch_views/ 2>/dev/null | wc -l")
    RECENT_BATCH=$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c \
        "mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1; \
         mc ls --recursive minikube/bucket-0/batch_views/ 2>/dev/null" | \
        awk '{print $4, $5, $6}' | while read -r line; do
            file_date=$(date -d "$line" +%s 2>/dev/null || echo 0)
            now=$(date +%s)
            age=$((now - file_date))
            if [ $age -lt 3600 ]; then
                echo "recent"
            fi
        done | wc -l)
fi

if [ "$BATCH_COUNT" -gt 0 ]; then
    if [ "$RECENT_BATCH" -gt 0 ]; then
        echo -e "${GREEN}✅ Batch Views: $BATCH_COUNT total files, $RECENT_BATCH modified in last hour${NC}"
    else
        echo -e "${YELLOW}⚠️  Batch Views: $BATCH_COUNT files exist, but NONE modified recently (batch jobs may not be running)${NC}"
    fi
else
    echo -e "${YELLOW}⚠️ Batch Views Empty (Run batch jobs to populate)${NC}"
fi
echo ""

# 8. Verify Speed Layer Views (Kafka → Speed Layer → Speed Views)
echo -e "${YELLOW}[8/10] Verifying Speed Layer Real-time Views...${NC}"
if command -v mc &> /dev/null; then
    SPEED_COUNT=$(mc ls --recursive minikube/bucket-0/speed_views/ 2>/dev/null | wc -l)
    # Check for files modified in the last 5 minutes (speed layer should update frequently)
    RECENT_SPEED=$(mc ls --recursive minikube/bucket-0/speed_views/ 2>/dev/null | \
        awk '{print $4, $5, $6}' | while read -r line; do
            file_date=$(date -d "$line" +%s 2>/dev/null || echo 0)
            now=$(date +%s)
            age=$((now - file_date))
            if [ $age -lt 300 ]; then  # Less than 5 minutes old
                echo "recent"
            fi
        done | wc -l)
else
    SPEED_COUNT=$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c \
        "mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1; \
         mc ls --recursive minikube/bucket-0/speed_views/ 2>/dev/null | wc -l")
    RECENT_SPEED=$(docker run --rm --network host --entrypoint /bin/sh minio/mc -c \
        "mc alias set minikube http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1; \
         mc ls --recursive minikube/bucket-0/speed_views/ 2>/dev/null" | \
        awk '{print $4, $5, $6}' | while read -r line; do
            file_date=$(date -d "$line" +%s 2>/dev/null || echo 0)
            now=$(date +%s)
            age=$((now - file_date))
            if [ $age -lt 300 ]; then
                echo "recent"
            fi
        done | wc -l)
fi

if [ "$SPEED_COUNT" -gt 0 ]; then
    if [ "$RECENT_SPEED" -gt 0 ]; then
        echo -e "${GREEN}✅ Speed Views: $SPEED_COUNT total files, $RECENT_SPEED modified in last 5 minutes${NC}"
    else
        echo -e "${YELLOW}⚠️  Speed Views: $SPEED_COUNT files exist, but NONE modified recently (speed layer may be stalled)${NC}"
    fi
else
    echo -e "${YELLOW}⚠️ Speed Views Empty (Speed layer may need time to process)${NC}"
fi
echo ""

# 9. Verify Data Consistency (Master Dataset vs Views)
echo -e "${YELLOW}[9/10] Verifying Data Pipeline Consistency...${NC}"
PIPELINE_OK=true
PIPELINE_WARNINGS=""

# Check if we have master data but no views at all
if [ "$MASTER_COUNT" -gt 0 ] && [ "$SPEED_COUNT" -eq 0 ] && [ "$BATCH_COUNT" -eq 0 ]; then
    PIPELINE_OK=false
    PIPELINE_WARNINGS="${PIPELINE_WARNINGS}\n  ⚠️  Master dataset exists but NO views generated (processing layers not working)"
fi

# Check if views exist but master data is stale
if [ "$MASTER_COUNT" -gt 0 ] && [ "$RECENT_FILES" -eq 0 ]; then
    if [ "$RECENT_SPEED" -eq 0 ]; then
        PIPELINE_WARNINGS="${PIPELINE_WARNINGS}\n  ⚠️  No recent data in master_dataset AND speed_views (ingestion stalled)"
    fi
fi

# Check if speed layer is behind
if [ "$MSG_COUNT" -gt 0 ] && [ "$SPEED_COUNT" -eq 0 ]; then
    PIPELINE_WARNINGS="${PIPELINE_WARNINGS}\n  ⚠️  Kafka has messages but speed layer has no output (speed layer pod may be failing)"
fi

if [ "$PIPELINE_OK" = true ] && [ -z "$PIPELINE_WARNINGS" ]; then
    echo -e "${GREEN}✅ Data pipeline consistency checks passed${NC}"
else
    echo -e "${YELLOW}Data Pipeline Warnings:${NC}"
    echo -e "$PIPELINE_WARNINGS"
fi
echo ""

# 10. Verify Serving Layer API
echo -e "${YELLOW}[10/10] Verifying Serving Layer API...${NC}"
kubectl port-forward service/serving-layer 8000:8000 -n default > /dev/null 2>&1 &
API_PF_PID=$!
sleep 5

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/)
if [ "$HTTP_CODE" == "200" ]; then
    echo -e "${GREEN}✅ Serving API is responding (HTTP 200)${NC}"
else
    echo -e "${RED}❌ Serving API failed (HTTP $HTTP_CODE)${NC}"
fi
kill $API_PF_PID 2>/dev/null
echo ""

# Cleanup
kill $PF_PID 2>/dev/null

# Summary
echo -e "${CYAN}===================================${NC}"
echo -e "${CYAN}VERIFICATION SUMMARY${NC}"
echo -e "${CYAN}===================================${NC}"
echo ""
echo -e "${YELLOW}Complete Ingestion Pipeline Status:${NC}"
echo "  1. Kafka Cluster: $([ "$KAFKA_READY" == "True" ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}")"
echo "  2. Kafka Topics: $([ "$ALL_TOPICS_OK" = true ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}")"
echo "  3. Kafka Messages (Producer): $([ "$MSG_COUNT" -gt 0 ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}")"
echo "  4. Master Dataset Ingestion: $([ "$MASTER_COUNT" -gt 0 ] && [ "$RECENT_FILES" -gt 0 ] && echo -e "${GREEN}✓${NC}" || echo -e "${YELLOW}⚠${NC}")"
echo "  5. Batch Views: $([ "$BATCH_COUNT" -gt 0 ] && echo -e "${GREEN}✓${NC}" || echo -e "${YELLOW}⚠${NC}")"
echo "  6. Speed Views: $([ "$SPEED_COUNT" -gt 0 ] && [ "$RECENT_SPEED" -gt 0 ] && echo -e "${GREEN}✓${NC}" || echo -e "${YELLOW}⚠${NC}")"
echo "  7. Serving API: $([ "$HTTP_CODE" == "200" ] && echo -e "${GREEN}✓${NC}" || echo -e "${RED}✗${NC}")"
echo ""
echo -e "${YELLOW}Dataflow Health:${NC}"
echo "  • Producer → Kafka: $([ "$MSG_COUNT" -gt 0 ] && echo -e "${GREEN}Active${NC}" || echo -e "${RED}No messages${NC}")"
echo "  • Kafka → Master Dataset: $([ "$RECENT_FILES" -gt 0 ] && echo -e "${GREEN}Ingesting ($RECENT_FILES new files)${NC}" || echo -e "${YELLOW}Stalled${NC}")"
echo "  • Master → Batch Views: $([ "$RECENT_BATCH" -gt 0 ] && echo -e "${GREEN}Processing ($RECENT_BATCH new files)${NC}" || echo -e "${YELLOW}Stalled or waiting${NC}")"
echo "  • Kafka → Speed Views: $([ "$RECENT_SPEED" -gt 0 ] && echo -e "${GREEN}Streaming ($RECENT_SPEED new files)${NC}" || echo -e "${YELLOW}Stalled${NC}")"
echo ""
echo -e "${YELLOW}Next Steps (if issues found):${NC}"
echo "  - If Kafka has no messages: Check producer (kubectl logs -f \$(cat /tmp/producer.pid 2>/dev/null) || python ingestion_layer/producer.py)"
echo "  - If Master Dataset stalled: Check ingestion (kubectl logs -f \$(cat /tmp/ingestion.pid 2>/dev/null) || python ingestion_layer/minio_ingest_k8s.py)"
echo "  - If Batch Views empty/stale: Run batch job (kubectl create job batch-manual --from=cronjob/batch-layer || run directly)"
echo "  - If Speed Views stalled: Check speed layer logs (kubectl logs -l app=speed-layer -f)"
echo "  - If API down: Check serving layer logs (kubectl logs -l app=serving-layer -f)"
echo ""
echo -e "${CYAN}===================================${NC}"

