# Kafka Streaming System - Quick Test Script
# This script runs a quick test to verify the system is working

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Kafka Streaming System - Quick Test" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# Navigate to kafka directory
$kafkaDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $kafkaDir

# Check if services are running
Write-Host "[1/4] Checking services..." -ForegroundColor Yellow
$kafkaRunning = docker ps --filter "name=kafka" --filter "status=running" --format "{{.Names}}" | Select-String "kafka"
if (-not $kafkaRunning) {
    Write-Host "✗ Kafka is not running. Start it with: .\start.ps1" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Kafka is running" -ForegroundColor Green

# List topics
Write-Host ""
Write-Host "[2/4] Listing Kafka topics..." -ForegroundColor Yellow
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Start producer in background (burst mode - 100 events)
Write-Host ""
Write-Host "[3/4] Sending 100 test events..." -ForegroundColor Yellow
$producerJob = Start-Job -ScriptBlock {
    param($dir)
    Set-Location $dir
    python signal_simulator.py --mode burst --events 100
} -ArgumentList $kafkaDir

# Wait a moment for messages to be sent
Start-Sleep -Seconds 3

# Start consumer for 10 seconds
Write-Host ""
Write-Host "[4/4] Consuming messages for 10 seconds..." -ForegroundColor Yellow
$consumerJob = Start-Job -ScriptBlock {
    param($dir)
    Set-Location $dir
    python stream_consumer.py --stats-interval 20
} -ArgumentList $kafkaDir

# Wait for 10 seconds
Start-Sleep -Seconds 10

# Stop jobs
Stop-Job $consumerJob -ErrorAction SilentlyContinue
Stop-Job $producerJob -ErrorAction SilentlyContinue

# Show results
Write-Host ""
Write-Host "Producer output:" -ForegroundColor Cyan
Receive-Job $producerJob

Write-Host ""
Write-Host "Consumer output:" -ForegroundColor Cyan
Receive-Job $consumerJob

# Clean up
Remove-Job $producerJob -ErrorAction SilentlyContinue
Remove-Job $consumerJob -ErrorAction SilentlyContinue

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "✓ Test complete!" -ForegroundColor Green
Write-Host "Check the Kafka UI for more details: http://localhost:8080" -ForegroundColor White
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""
