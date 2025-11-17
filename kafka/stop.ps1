# Kafka Streaming System - Stop Script
# This script stops all services

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Kafka Streaming System - Shutdown" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# Navigate to kafka directory
$kafkaDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $kafkaDir

Write-Host "Stopping services..." -ForegroundColor Yellow
docker-compose down

Write-Host ""
Write-Host "✓ All services stopped" -ForegroundColor Green
Write-Host ""
Write-Host "To remove all data volumes, run:" -ForegroundColor Yellow
Write-Host "  docker-compose down -v" -ForegroundColor White
Write-Host ""
