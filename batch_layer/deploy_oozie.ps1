# Deploy Batch Layer to Oozie (Windows PowerShell)
# This script uploads batch jobs and Oozie workflow to HDFS

$ErrorActionPreference = "Stop"

# Configuration
$HDFS_USER = "batch_layer"
$HDFS_BASE_PATH = "/user/$HDFS_USER"
$LOCAL_BATCH_DIR = $PSScriptRoot

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Deploying Batch Layer to Oozie" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Local directory: $LOCAL_BATCH_DIR"
Write-Host "HDFS base path: $HDFS_BASE_PATH"
Write-Host ""

# Create HDFS directories
Write-Host "[1/5] Creating HDFS directory structure..." -ForegroundColor Yellow
hdfs dfs -mkdir -p "$HDFS_BASE_PATH/batch_layer/jobs"
hdfs dfs -mkdir -p "$HDFS_BASE_PATH/batch_layer/oozie"
hdfs dfs -mkdir -p "$HDFS_BASE_PATH/batch_layer/lib"

# Upload batch job Python files
Write-Host "[2/5] Uploading batch job files..." -ForegroundColor Yellow
Get-ChildItem "$LOCAL_BATCH_DIR\jobs\*.py" | ForEach-Object {
    hdfs dfs -put -f $_.FullName "$HDFS_BASE_PATH/batch_layer/jobs/"
}

# Upload Oozie workflow and coordinator
Write-Host "[3/5] Uploading Oozie workflow definitions..." -ForegroundColor Yellow
hdfs dfs -put -f "$LOCAL_BATCH_DIR\oozie\workflow.xml" "$HDFS_BASE_PATH/batch_layer/oozie/"
hdfs dfs -put -f "$LOCAL_BATCH_DIR\oozie\coordinator.xml" "$HDFS_BASE_PATH/batch_layer/oozie/"
hdfs dfs -put -f "$LOCAL_BATCH_DIR\oozie\job.properties" "$HDFS_BASE_PATH/batch_layer/oozie/"

# Upload configuration
Write-Host "[4/5] Uploading configuration files..." -ForegroundColor Yellow
hdfs dfs -put -f "$LOCAL_BATCH_DIR\config.py" "$HDFS_BASE_PATH/batch_layer/"

# Set permissions
Write-Host "[5/5] Setting HDFS permissions..." -ForegroundColor Yellow
hdfs dfs -chmod -R 755 "$HDFS_BASE_PATH/batch_layer"

Write-Host ""
Write-Host "=========================================" -ForegroundColor Green
Write-Host "✅ Batch Layer deployed successfully!" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Verify deployment:"
Write-Host "   hdfs dfs -ls -R $HDFS_BASE_PATH/batch_layer"
Write-Host ""
Write-Host "2. Submit Oozie coordinator:"
Write-Host "   oozie job -oozie http://localhost:11000/oozie -config $LOCAL_BATCH_DIR\oozie\job.properties -run"
Write-Host ""
Write-Host "3. Check job status:"
Write-Host "   oozie job -oozie http://localhost:11000/oozie -info <job-id>"
Write-Host ""
