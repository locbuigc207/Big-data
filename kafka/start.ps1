# Kafka Streaming System - Startup Script
# This script starts all necessary services

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Kafka Streaming System - Startup" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is running
Write-Host "[1/5] Checking Docker..." -ForegroundColor Yellow
try {
    docker info | Out-Null
    Write-Host "✓ Docker is running" -ForegroundColor Green
}
catch {
    Write-Host "✗ Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Navigate to kafka directory
$kafkaDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $kafkaDir

# Start Kafka infrastructure
Write-Host ""
Write-Host "[2/5] Starting Kafka infrastructure..." -ForegroundColor Yellow
docker-compose up -d

# Wait for services to be healthy
Write-Host ""
Write-Host "[3/5] Waiting for services to be ready..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0

while ($attempt -lt $maxAttempts) {
    $attempt++
    Write-Host "  Attempt $attempt/$maxAttempts..." -ForegroundColor Gray
    
    $kafkaHealthy = docker inspect --format='{{.State.Health.Status}}' kafka 2>$null
    $kafkaUIHealthy = docker inspect --format='{{.State.Health.Status}}' kafka-ui 2>$null
    
    if ($kafkaHealthy -eq "healthy" -and $kafkaUIHealthy -eq "healthy") {
        Write-Host "✓ All services are ready" -ForegroundColor Green
        break
    }
    
    if ($attempt -eq $maxAttempts) {
        Write-Host "✗ Services did not become ready in time" -ForegroundColor Red
        Write-Host "  Check logs with: docker-compose logs" -ForegroundColor Yellow
        exit 1
    }
    
    Start-Sleep -Seconds 2
}

# Install Python dependencies if needed
Write-Host ""
Write-Host "[4/5] Checking Python dependencies..." -ForegroundColor Yellow
if (Test-Path "requirements.txt") {
    pip install -r requirements.txt --quiet
    Write-Host "✓ Python dependencies installed" -ForegroundColor Green
}
else {
    Write-Host "! requirements.txt not found" -ForegroundColor Yellow
}

# Show service information
Write-Host ""
Write-Host "[5/5] System Information" -ForegroundColor Yellow
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "Kafka Broker:  localhost:9092" -ForegroundColor White
Write-Host "Kafka UI:      http://localhost:8080" -ForegroundColor White
Write-Host "Zookeeper:     localhost:2181" -ForegroundColor White
Write-Host "================================================" -ForegroundColor Cyan

Write-Host ""
Write-Host "✓ Kafka Streaming System is ready!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Start the signal simulator:  python signal_simulator.py" -ForegroundColor White
Write-Host "  2. Start the stream consumer:   python stream_consumer.py" -ForegroundColor White
Write-Host "  3. Open Kafka UI:               http://localhost:8080" -ForegroundColor White
Write-Host ""
Write-Host "To stop the system:               .\stop.ps1" -ForegroundColor White
Write-Host ""
