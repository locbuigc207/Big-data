# Quick Start Script for Minikube Testing
# Run this script to deploy everything at once

Write-Host "================================" -ForegroundColor Cyan
Write-Host "MINIKUBE KAFKA INGESTION TEST" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Start Minikube
Write-Host "[1/10] Starting Minikube cluster..." -ForegroundColor Yellow
minikube start --cpus=4 --memory=8192 --driver=docker
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to start Minikube" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Minikube started" -ForegroundColor Green
Write-Host ""

# Step 2: Create namespaces
Write-Host "[2/10] Creating namespaces..." -ForegroundColor Yellow
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace minio --dry-run=client -o yaml | kubectl apply -f -
Write-Host "✅ Namespaces created" -ForegroundColor Green
Write-Host ""

# Step 3: Install Strimzi operator
Write-Host "[3/10] Installing Strimzi Kafka Operator..." -ForegroundColor Yellow
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
Start-Sleep -Seconds 5
kubectl wait deployment/strimzi-cluster-operator --for=condition=Available --timeout=300s -n kafka
Write-Host "✅ Strimzi operator ready" -ForegroundColor Green
Write-Host ""

# Step 4: Create storage
Write-Host "[4/10] Setting up storage..." -ForegroundColor Yellow
minikube ssh "sudo mkdir -p /mnt/kafka-data/0 /mnt/kafka-data/1 /mnt/kafka-data/2"
kubectl apply -f kafka/storage-class.yaml -n kafka
kubectl apply -f kafka/persistent-volumn-minikube.yaml -n kafka
Write-Host "✅ Storage configured" -ForegroundColor Green
Write-Host ""

# Step 5: Deploy Kafka
Write-Host "[5/10] Deploying Kafka cluster (this takes 3-5 minutes)..." -ForegroundColor Yellow
kubectl apply -f kafka/deployment.yaml -n kafka
kubectl wait kafka/kafka-cluster --for=condition=Ready --timeout=600s -n kafka
Write-Host "✅ Kafka cluster ready" -ForegroundColor Green
Write-Host ""

# Step 6: Create topics
Write-Host "[6/10] Creating Kafka topics..." -ForegroundColor Yellow
kubectl apply -f kafka/topics.yaml -n kafka
Start-Sleep -Seconds 10
Write-Host "✅ Topics created" -ForegroundColor Green
Write-Host ""

# Step 7: Deploy MinIO
Write-Host "[7/10] Deploying MinIO..." -ForegroundColor Yellow
kubectl apply -f minio/deployment.yaml -n minio
kubectl wait deployment/minio --for=condition=Available --timeout=300s -n minio
Write-Host "✅ MinIO ready" -ForegroundColor Green
Write-Host ""

# Step 8: Setup MinIO bucket
Write-Host "[8/10] Configuring MinIO bucket..." -ForegroundColor Yellow
Write-Host "Starting port-forward for MinIO setup..." -ForegroundColor Cyan
$minioJob = Start-Job -ScriptBlock { kubectl port-forward service/minio 9000:9000 -n minio }
Start-Sleep -Seconds 5

# Configure mc and create bucket
mc alias set minikube http://localhost:9000 minioadmin minioadmin 2>$null
mc mb minikube/bucket-0 2>$null

Stop-Job -Job $minioJob
Remove-Job -Job $minioJob
Write-Host "✅ MinIO bucket created" -ForegroundColor Green
Write-Host ""

# Step 9: Display status
Write-Host "[9/10] Checking deployment status..." -ForegroundColor Yellow
Write-Host ""
Write-Host "Kafka Pods:" -ForegroundColor Cyan
kubectl get pods -n kafka -l strimzi.io/cluster=kafka-cluster
Write-Host ""
Write-Host "MinIO Pods:" -ForegroundColor Cyan
kubectl get pods -n minio
Write-Host ""
Write-Host "Kafka Topics:" -ForegroundColor Cyan
kubectl get kafkatopics -n kafka
Write-Host ""

# Step 10: Start port forwarding
Write-Host "[10/10] Setting up port forwarding..." -ForegroundColor Yellow
Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "NEXT STEPS:" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Open 3 NEW PowerShell terminals and run:" -ForegroundColor Yellow
Write-Host "   Terminal 1: kubectl port-forward service/kafka-cluster-kafka-bootstrap 9092:9092 -n kafka" -ForegroundColor White
Write-Host "   Terminal 2: kubectl port-forward service/minio 9000:9000 -n minio" -ForegroundColor White
Write-Host "   Terminal 3: kubectl port-forward service/minio 9001:9001 -n minio" -ForegroundColor White
Write-Host ""
Write-Host "2. Wait 30 seconds for port-forwards to be ready" -ForegroundColor Yellow
Write-Host ""
Write-Host "3. Run the producer:" -ForegroundColor Yellow
Write-Host "   python producer.py" -ForegroundColor White
Write-Host ""
Write-Host "4. Run the ingestion layer:" -ForegroundColor Yellow
Write-Host "   python minio_ingest_k8s.py" -ForegroundColor White
Write-Host ""
Write-Host "5. Access MinIO Console:" -ForegroundColor Yellow
Write-Host "   http://localhost:9001" -ForegroundColor White
Write-Host "   Username: minioadmin" -ForegroundColor White
Write-Host "   Password: minioadmin" -ForegroundColor White
Write-Host ""
Write-Host "6. Verify data ingestion:" -ForegroundColor Yellow
Write-Host "   mc ls --recursive minikube/bucket-0/master_dataset/" -ForegroundColor White
Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "DEPLOYMENT COMPLETE! ✅" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "For detailed testing guide, see: MINIKUBE_TESTING_GUIDE.md" -ForegroundColor Cyan
Write-Host ""
