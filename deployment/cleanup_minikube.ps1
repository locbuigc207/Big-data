# Stop and cleanup Minikube deployment

Write-Host "================================" -ForegroundColor Cyan
Write-Host "MINIKUBE CLEANUP" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Stopping port-forwards..." -ForegroundColor Yellow
Get-Job | Where-Object { $_.Command -like "*kubectl port-forward*" } | Stop-Job
Get-Job | Where-Object { $_.Command -like "*kubectl port-forward*" } | Remove-Job
Write-Host "✅ Port-forwards stopped" -ForegroundColor Green
Write-Host ""

Write-Host "Deleting Kafka cluster..." -ForegroundColor Yellow
kubectl delete kafka kafka-cluster -n kafka --ignore-not-found=true
Start-Sleep -Seconds 5
Write-Host "✅ Kafka deleted" -ForegroundColor Green
Write-Host ""

# 2. Delete MinIO and Kafka
Write-Host "Deleting MinIO and Kafka..." -ForegroundColor Yellow
kubectl delete namespace minio --ignore-not-found=$true
kubectl delete namespace kafka --ignore-not-found=$true

# Delete PVs
kubectl delete pv pv-kafka-0 pv-kafka-1 pv-kafka-2 --ignore-not-found=$true

# Cleanup physical data on Minikube host
Write-Host "Cleaning up physical data on Minikube host..." -ForegroundColor Yellow
minikube ssh "sudo rm -rf /mnt/kafka-data/*"

Write-Host "Infrastructure namespaces deleted" -ForegroundColor Green
Write-Host ""

Write-Host "Stopping Minikube..." -ForegroundColor Yellow
minikube stop
Write-Host "✅ Minikube stopped" -ForegroundColor Green
Write-Host ""

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Do you want to DELETE the Minikube cluster completely?" -ForegroundColor Yellow
Write-Host "This will remove all data and require full redeployment." -ForegroundColor Yellow
Write-Host "================================" -ForegroundColor Cyan
$response = Read-Host "Type 'yes' to delete, or press Enter to keep"

if ($response -eq 'yes') {
    Write-Host ""
    Write-Host "Deleting Minikube cluster..." -ForegroundColor Yellow
    minikube delete
    Write-Host "✅ Minikube cluster deleted" -ForegroundColor Green
}
else {
    Write-Host ""
    Write-Host "Minikube cluster preserved. Use 'minikube start' to restart." -ForegroundColor Cyan
}

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "CLEANUP COMPLETE! ✅" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Cyan
