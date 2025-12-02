#!/bin/bash

# Deploy ML Pipeline to Kubernetes
# Usage: ./deploy-ml-pipeline.sh

set -e

echo "================================================"
echo "  Deploying ML Pipeline to Kubernetes"
echo "================================================"

NAMESPACE="bigdata"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Step 1: Verify prerequisites
echo -e "\n${YELLOW}[1/6] Verifying prerequisites...${NC}"

# Check namespace
if ! kubectl get namespace $NAMESPACE &> /dev/null; then
    echo -e "${RED}Error: Namespace $NAMESPACE does not exist${NC}"
    exit 1
fi

# Check HDFS
if ! kubectl get statefulset hdfs-namenode -n $NAMESPACE &> /dev/null; then
    echo -e "${RED}Error: HDFS not deployed${NC}"
    exit 1
fi

# Check batch processing
echo "Verifying batch processing has completed..."
kubectl exec -n $NAMESPACE hdfs-namenode-0 -- \
    hdfs dfs -test -d /views/batch/$(date +%Y-%m-%d) 2>/dev/null

if [ $? -ne 0 ]; then
    echo -e "${YELLOW}Warning: Batch views not found for today${NC}"
    echo "Please run batch processing first"
fi

echo -e "${GREEN}✓ Prerequisites verified${NC}"

# Step 2: Create ConfigMap with Python scripts
echo -e "\n${YELLOW}[2/6] Creating ConfigMap for ML scripts...${NC}"

# Find ML scripts
ML_SCRIPTS_DIR="${SCRIPT_DIR}/../hdfs"

if [ ! -d "$ML_SCRIPTS_DIR" ]; then
    echo -e "${RED}Error: ML scripts directory not found: $ML_SCRIPTS_DIR${NC}"
    exit 1
fi

# Check required scripts
REQUIRED_SCRIPTS=(
    "ml_feature_engineering.py"
    "ml_gpa_predictor.py"
    "ml_dropout_classifier.py"
    "ml_model_serving.py"
)

for script in "${REQUIRED_SCRIPTS[@]}"; do
    if [ ! -f "$ML_SCRIPTS_DIR/$script" ]; then
        echo -e "${RED}Error: Required script not found: $script${NC}"
        exit 1
    fi
    echo "✓ Found: $script"
done

# Create ConfigMap
kubectl create configmap ml-pipeline-scripts \
    --from-file="$ML_SCRIPTS_DIR/ml_feature_engineering.py" \
    --from-file="$ML_SCRIPTS_DIR/ml_gpa_predictor.py" \
    --from-file="$ML_SCRIPTS_DIR/ml_dropout_classifier.py" \
    --from-file="$ML_SCRIPTS_DIR/ml_model_serving.py" \
    -n $NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

echo -e "${GREEN}✓ ConfigMap created${NC}"

# Step 3: Deploy RBAC and ServiceAccount
echo -e "\n${YELLOW}[3/6] Deploying RBAC...${NC}"

kubectl apply -f "${SCRIPT_DIR}/../hdfs/ml-pipeline-deployment.yaml"

echo -e "${GREEN}✓ RBAC deployed${NC}"

# Step 4: Run Feature Engineering
echo -e "\n${YELLOW}[4/6] Running Feature Engineering...${NC}"

# Delete old job if exists
kubectl delete job ml-feature-engineering -n $NAMESPACE 2>/dev/null || true
sleep 2

# Create job
FEATURE_JOB_NAME="ml-feature-engineering-$(date +%s)"

cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${FEATURE_JOB_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: ml-pipeline
    component: feature-engineering
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: ml-pipeline
        component: feature-engineering
    spec:
      serviceAccountName: spark-ml-sa
      restartPolicy: Never
      containers:
      - name: spark-feature-engineer
        image: bitnami/spark:3.5.0
        command: ["/bin/bash", "-c"]
        args:
        - |
          echo "=========================================="
          echo "  Feature Engineering Starting"
          echo "=========================================="
          
          pip install --quiet pyspark==3.5.0
          
          spark-submit \
            --master k8s://https://kubernetes.default.svc:443 \
            --deploy-mode client \
            --name ml-features \
            --conf spark.kubernetes.namespace=${NAMESPACE} \
            --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-ml-sa \
            --conf spark.kubernetes.container.image=bitnami/spark:3.5.0 \
            --conf spark.executor.instances=3 \
            --conf spark.executor.memory=4g \
            --conf spark.driver.memory=4g \
            --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000 \
            /opt/scripts/ml_feature_engineering.py
          
          EXIT_CODE=\$?
          
          if [ \$EXIT_CODE -eq 0 ]; then
            echo "Feature Engineering Completed Successfully"
          else
            echo "Feature Engineering Failed"
          fi
          
          exit \$EXIT_CODE
        env:
        - name: HDFS_NAMENODE
          value: "hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000"
        volumeMounts:
        - name: scripts
          mountPath: /opt/scripts
        resources:
          requests:
            memory: "5Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
      volumes:
      - name: scripts
        configMap:
          name: ml-pipeline-scripts
EOF

echo "Waiting for feature engineering to complete (max 10 minutes)..."
kubectl wait --for=condition=complete --timeout=600s \
    job/${FEATURE_JOB_NAME} -n ${NAMESPACE} 2>/dev/null

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Feature engineering completed${NC}"
else
    echo -e "${YELLOW}⚠ Feature engineering timed out or failed${NC}"
    echo "Check logs with: kubectl logs -f job/${FEATURE_JOB_NAME} -n ${NAMESPACE}"
fi

# Step 5: Deploy training jobs
echo -e "\n${YELLOW}[5/6] ML Training Deployment Options${NC}"
echo "================================================"
echo "Choose deployment option:"
echo "  1) Run all training jobs now (GPA + Dropout)"
echo "  2) Deploy CronJob only (weekly retraining)"
echo "  3) Deploy both (immediate + scheduled)"
echo "  4) Skip training deployment"
echo ""
read -p "Enter choice [1-4]: " choice

case $choice in
    1)
        echo ""
        echo "Deploying immediate training jobs..."
        
        # Deploy GPA training
        GPA_JOB_NAME="ml-gpa-training-$(date +%s)"
        
        cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${GPA_JOB_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: ml-pipeline
    component: gpa-training
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: ml-pipeline
        component: gpa-training
    spec:
      serviceAccountName: spark-ml-sa
      restartPolicy: Never
      containers:
      - name: spark-gpa-trainer
        image: bitnami/spark:3.5.0
        command: ["/bin/bash", "-c"]
        args:
        - |
          echo "=========================================="
          echo "  GPA Model Training Starting"
          echo "=========================================="
          
          pip install --quiet pyspark==3.5.0
          
          spark-submit \
            --master k8s://https://kubernetes.default.svc:443 \
            --deploy-mode client \
            --name ml-gpa-training \
            --conf spark.kubernetes.namespace=${NAMESPACE} \
            --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-ml-sa \
            --conf spark.kubernetes.container.image=bitnami/spark:3.5.0 \
            --conf spark.executor.instances=4 \
            --conf spark.executor.memory=6g \
            --conf spark.driver.memory=6g \
            --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000 \
            /opt/scripts/ml_gpa_predictor.py
          
          exit \$?
        env:
        - name: HDFS_NAMENODE
          value: "hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000"
        volumeMounts:
        - name: scripts
          mountPath: /opt/scripts
        resources:
          requests:
            memory: "7Gi"
            cpu: "2500m"
          limits:
            memory: "10Gi"
            cpu: "4"
      volumes:
      - name: scripts
        configMap:
          name: ml-pipeline-scripts
EOF
        
        echo "✓ GPA training job created: ${GPA_JOB_NAME}"
        
        # Deploy Dropout training
        DROPOUT_JOB_NAME="ml-dropout-training-$(date +%s)"
        
        cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${DROPOUT_JOB_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: ml-pipeline
    component: dropout-training
spec:
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: ml-pipeline
        component: dropout-training
    spec:
      serviceAccountName: spark-ml-sa
      restartPolicy: Never
      containers:
      - name: spark-dropout-trainer
        image: bitnami/spark:3.5.0
        command: ["/bin/bash", "-c"]
        args:
        - |
          echo "=========================================="
          echo "  Dropout Model Training Starting"
          echo "=========================================="
          
          pip install --quiet pyspark==3.5.0
          
          spark-submit \
            --master k8s://https://kubernetes.default.svc:443 \
            --deploy-mode client \
            --name ml-dropout-training \
            --conf spark.kubernetes.namespace=${NAMESPACE} \
            --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-ml-sa \
            --conf spark.kubernetes.container.image=bitnami/spark:3.5.0 \
            --conf spark.executor.instances=4 \
            --conf spark.executor.memory=6g \
            --conf spark.driver.memory=6g \
            --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000 \
            /opt/scripts/ml_dropout_classifier.py
          
          exit \$?
        env:
        - name: HDFS_NAMENODE
          value: "hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000"
        volumeMounts:
        - name: scripts
          mountPath: /opt/scripts
        resources:
          requests:
            memory: "7Gi"
            cpu: "2500m"
          limits:
            memory: "10Gi"
            cpu: "4"
      volumes:
      - name: scripts
        configMap:
          name: ml-pipeline-scripts
EOF
        
        echo "✓ Dropout training job created: ${DROPOUT_JOB_NAME}"
        echo ""
        echo "Monitor with:"
        echo "  kubectl get jobs -n ${NAMESPACE} -w"
        echo "  kubectl logs -f job/${GPA_JOB_NAME} -n ${NAMESPACE}"
        echo "  kubectl logs -f job/${DROPOUT_JOB_NAME} -n ${NAMESPACE}"
        ;;
        
    2)
        echo ""
        echo "Deploying CronJob for weekly retraining..."
        
        cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ml-weekly-retraining
  namespace: ${NAMESPACE}
  labels:
    app: ml-pipeline
    component: scheduled-training
spec:
  schedule: "0 2 * * 0"  # Every Sunday at 2 AM
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        metadata:
          labels:
            app: ml-pipeline
            component: scheduled-training
        spec:
          serviceAccountName: spark-ml-sa
          restartPolicy: OnFailure
          containers:
          - name: ml-retraining-orchestrator
            image: bitnami/spark:3.5.0
            command: ["/bin/bash", "-c"]
            args:
            - |
              echo "=========================================="
              echo "  Weekly ML Retraining Pipeline"
              echo "  \$(date)"
              echo "=========================================="
              
              pip install --quiet pyspark==3.5.0
              
              # Step 1: Feature Engineering
              echo ""
              echo "[1/3] Running Feature Engineering..."
              spark-submit \
                --master k8s://https://kubernetes.default.svc:443 \
                --deploy-mode client \
                --name ml-features-weekly \
                --conf spark.kubernetes.namespace=${NAMESPACE} \
                --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-ml-sa \
                --conf spark.kubernetes.container.image=bitnami/spark:3.5.0 \
                --conf spark.executor.instances=3 \
                --conf spark.executor.memory=4g \
                --conf spark.driver.memory=4g \
                --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000 \
                /opt/scripts/ml_feature_engineering.py
              
              if [ \$? -ne 0 ]; then
                echo "Feature engineering failed!"
                exit 1
              fi
              
              # Step 2: Train GPA Predictor
              echo ""
              echo "[2/3] Training GPA Predictor..."
              spark-submit \
                --master k8s://https://kubernetes.default.svc:443 \
                --deploy-mode client \
                --name ml-gpa-weekly \
                --conf spark.kubernetes.namespace=${NAMESPACE} \
                --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-ml-sa \
                --conf spark.kubernetes.container.image=bitnami/spark:3.5.0 \
                --conf spark.executor.instances=4 \
                --conf spark.executor.memory=6g \
                --conf spark.driver.memory=6g \
                --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000 \
                /opt/scripts/ml_gpa_predictor.py
              
              if [ \$? -ne 0 ]; then
                echo "GPA training failed!"
                exit 1
              fi
              
              # Step 3: Train Dropout Classifier
              echo ""
              echo "[3/3] Training Dropout Classifier..."
              spark-submit \
                --master k8s://https://kubernetes.default.svc:443 \
                --deploy-mode client \
                --name ml-dropout-weekly \
                --conf spark.kubernetes.namespace=${NAMESPACE} \
                --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-ml-sa \
                --conf spark.kubernetes.container.image=bitnami/spark:3.5.0 \
                --conf spark.executor.instances=4 \
                --conf spark.executor.memory=6g \
                --conf spark.driver.memory=6g \
                --conf spark.hadoop.fs.defaultFS=hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000 \
                /opt/scripts/ml_dropout_classifier.py
              
              if [ \$? -ne 0 ]; then
                echo "Dropout training failed!"
                exit 1
              fi
              
              echo ""
              echo "=========================================="
              echo "  Weekly Retraining Completed Successfully"
              echo "=========================================="
            env:
            - name: HDFS_NAMENODE
              value: "hdfs://hdfs-namenode.${NAMESPACE}.svc.cluster.local:9000"
            volumeMounts:
            - name: scripts
              mountPath: /opt/scripts
            resources:
              requests:
                memory: "8Gi"
                cpu: "3"
              limits:
                memory: "12Gi"
                cpu: "5"
          volumes:
          - name: scripts
            configMap:
              name: ml-pipeline-scripts
EOF
        
        echo -e "${GREEN}✓ CronJob deployed${NC}"
        echo ""
        echo "CronJob will run every Sunday at 2 AM"
        echo "View schedule: kubectl get cronjob ml-weekly-retraining -n ${NAMESPACE}"
        ;;
        
    3)
        echo ""
        echo "Deploying both immediate jobs and CronJob..."
        
        # Run option 1 (immediate jobs)
        choice=1
        eval "$(sed -n '/case \$choice in/,/;;/p' "$0" | head -n -1 | tail -n +2)"
        
        # Run option 2 (cronjob)
        choice=2
        eval "$(sed -n '/2)/,/;;/p' "$0" | head -n -1 | tail -n +2)"
        
        echo -e "${GREEN}✓ Both immediate and scheduled training deployed${NC}"
        ;;
        
    *)
        echo ""
        echo "Skipping training deployment"
        ;;
esac

# Step 6: Summary and next steps
echo ""
echo -e "\n${YELLOW}[6/6] Deployment Summary${NC}"
echo "================================================"
echo -e "${GREEN}✓ ML Pipeline Deployed Successfully${NC}"
echo "================================================"

echo ""
echo "📊 Deployed Resources:"
echo "----------------------------------------"
kubectl get all -n ${NAMESPACE} -l app=ml-pipeline

echo ""
echo "📝 ConfigMaps:"
echo "----------------------------------------"
kubectl get configmap ml-pipeline-scripts -n ${NAMESPACE}

echo ""
echo "🔐 ServiceAccounts:"
echo "----------------------------------------"
kubectl get serviceaccount spark-ml-sa -n ${NAMESPACE}

echo ""
echo "⏰ CronJobs:"
echo "----------------------------------------"
kubectl get cronjob -n ${NAMESPACE} -l app=ml-pipeline 2>/dev/null || echo "No CronJobs deployed"

echo ""
echo "================================================"
echo "📚 Useful Commands"
echo "================================================"

echo ""
echo "1. Monitor training jobs:"
echo "   kubectl get jobs -n ${NAMESPACE} -l app=ml-pipeline -w"
echo ""

echo "2. View job logs:"
echo "   kubectl logs -f job/<job-name> -n ${NAMESPACE}"
echo ""

echo "3. Check feature engineering output:"
echo "   kubectl exec hdfs-namenode-0 -n ${NAMESPACE} -- \\"
echo "     hdfs dfs -ls /ml_features/\$(date +%Y-%m-%d)/"
echo ""

echo "4. Check trained models:"
echo "   kubectl exec hdfs-namenode-0 -n ${NAMESPACE} -- \\"
echo "     hdfs dfs -ls /models/"
echo ""

echo "5. View CronJob schedule:"
echo "   kubectl get cronjob ml-weekly-retraining -n ${NAMESPACE}"
echo ""

echo "6. Manually trigger CronJob:"
echo "   kubectl create job --from=cronjob/ml-weekly-retraining \\"
echo "     manual-run-\$(date +%s) -n ${NAMESPACE}"
echo ""

echo "7. Delete all ML jobs:"
echo "   kubectl delete jobs -n ${NAMESPACE} -l app=ml-pipeline"
echo ""

echo "8. Monitor with custom script:"
echo "   ./script/monitor-ml-training.sh"
echo ""

echo "================================================"
echo -e "${GREEN}Deployment Complete!${NC}"
echo "================================================"
echo ""
echo "Next steps:"
echo "  1. Monitor job progress"
echo "  2. Verify models in HDFS"
echo "  3. Run model serving to generate predictions"
echo ""