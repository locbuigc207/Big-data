# Batch Processing Layer - Quick Start Guide

## 📋 Overview

Batch processing layer thực hiện các phép biến đổi phức tạp trên dữ liệu đã được ingest, bao gồm:

- ✅ **Data Cleaning**: Deduplication, validation
- ✅ **Complex Joins**: Broadcast joins, sort-merge joins
- ✅ **Aggregations**: Window functions, CUBE, ROLLUP
- ✅ **GPA Calculation**: Weighted averages với custom logic
- ✅ **Student Rankings**: rank(), dense_rank(), percent_rank(), ntile()
- ✅ **Attendance Analysis**: Multi-dimensional aggregations
- ✅ **Course Statistics**: Percentiles, distributions
- ✅ **Faculty Performance**: Performance metrics và ranking
- ✅ **At-Risk Detection**: Multi-criteria risk scoring
- ✅ **Pivot Tables**: Cross-tabulation analysis
- ✅ **Correlation Analysis**: Statistical correlations

---

## 🚀 Quick Start

### 1. Deploy Batch Processing

```bash
# Make scripts executable
chmod +x deploy-batch-processing.sh
chmod +x test-batch-processing.sh
chmod +x monitor-batch-processing.sh

# Deploy
./deploy-batch-processing.sh
```

### 2. Run Manual Processing (Test)

```bash
# Run immediately
kubectl create job --from=cronjob/batch-processing-daily \
  test-batch-$(date +%s) -n bigdata

# Monitor progress
kubectl logs -f -l app=batch-processing -n bigdata
```

### 3. Verify Results

```bash
./test-batch-processing.sh
```

### 4. Monitor (Real-time)

```bash
./monitor-batch-processing.sh
```

---

## 📂 Output Structure

Sau khi batch processing hoàn thành, HDFS sẽ có cấu trúc:

```
/views/batch/YYYY-MM-DD/
├── student_gpa/                    # GPA calculations
│   ├── semester=2023-1/
│   ├── semester=2023-2/
│   ├── semester=2024-1/
│   └── semester=2024-2/
│
├── class_rankings/                 # Student rankings per class
│   ├── semester=2023-1/
│   ├── semester=2023-2/
│   └── ...
│
├── attendance_metrics/             # Attendance analysis
│   ├── semester=2023-1/
│   └── ...
│
├── course_statistics/              # Course difficulty, pass rates
│   ├── semester=2023-1/
│   └── ...
│
├── faculty_performance/            # Teacher performance metrics
│   ├── semester=2023-1/
│   └── ...
│
├── at_risk_students/               # Students needing intervention
│   ├── semester=2023-1/
│   │   ├── overall_risk_level=CRITICAL/
│   │   ├── overall_risk_level=HIGH/
│   │   ├── overall_risk_level=MEDIUM/
│   │   └── overall_risk_level=LOW/
│   └── ...
│
├── student_profile_view/           # Comprehensive student profiles
│   ├── semester=2023-1/
│   └── ...
│
├── student_distribution_pivot/     # Pivot: Students by faculty × tier
├── course_performance_pivot/       # Pivot: Course pass rates by semester
└── course_cube/                    # CUBE: Multi-dimensional aggregation
```

---

## 🔍 Spark Features Demonstrated

### 1. Complex Joins

```python
# Broadcast join for small dimension tables
grades_with_credits = grades \
    .join(broadcast(classes), "class_id") \
    .join(broadcast(courses), "course_id")
```

### 2. Window Functions

```python
# Ranking within partitions
window = Window.partitionBy("class_id").orderBy(col("total_score").desc())
ranked = df.withColumn("rank", rank().over(window))
```

### 3. Custom Aggregations

```python
# Weighted GPA calculation
semester_gpa = grades \
    .groupBy("student_id", "semester") \
    .agg(
        (sum(col("total_score") * col("credits")) / sum("credits")).alias("semester_gpa")
    )
```

### 4. CUBE Operations

```python
# Multi-dimensional aggregation
cube_result = df \
    .cube("semester", "faculty", "course_id") \
    .agg(count("*").alias("enrollment"))
```

### 5. Pivot Tables

```python
# Cross-tabulation
pivot = df \
    .groupBy("faculty") \
    .pivot("performance_tier") \
    .count()
```

---

## 📊 Key Metrics Generated

### Student Metrics

- **semester_gpa**: GPA for each semester
- **cumulative_gpa**: Overall GPA across all semesters
- **gpa_trend**: Change in GPA between semesters
- **performance_tier**: Excellent / Very Good / Good / Average / At Risk
- **academic_standing**: Dean's List / Good Standing / Warning / Probation

### Attendance Metrics

- **attendance_rate**: Percentage of sessions attended
- **absence_rate**: Percentage of unexcused absences
- **attendance_status**: Excellent / Very Good / Good / Acceptable / Poor

### Course Metrics

- **avg_score**: Average score across all students
- **pass_rate**: Percentage of students passing
- **difficulty_level**: Easy / Moderate / Challenging / Difficult
- **percentiles**: Q1, Median, Q3 scores

### Faculty Metrics

- **avg_student_score**: Average score of students taught
- **pass_rate**: Overall pass rate
- **performance_category**: Outstanding / Excellent / Very Good / Good
- **teaching_consistency**: Measured by stddev of student scores

### At-Risk Metrics

- **risk_score**: 0-100 composite risk score
- **overall_risk_level**: CRITICAL / HIGH / MEDIUM / LOW
- **intervention_priority**: 1 (highest) to 4 (lowest)

---

## 🛠️ Troubleshooting

### Job Failed

```bash
# Check logs
kubectl logs -l app=batch-processing -n bigdata --tail=100

# Check pod events
kubectl get events -n bigdata --sort-by='.lastTimestamp' | grep batch-processing

# Describe failed pod
kubectl describe pod <pod-name> -n bigdata
```

### Out of Memory

```bash
# Increase executor memory in batch-processing-job.yaml
--conf spark.executor.memory=6g
--conf spark.driver.memory=6g
```

### HDFS Connection Issues

```bash
# Test HDFS connectivity
kubectl exec -it hdfs-namenode-0 -n bigdata -- hdfs dfsadmin -report

# Check if namenode is healthy
kubectl get pods -n bigdata -l app=hdfs-namenode
```

### Data Not Found

```bash
# Verify ingestion completed
kubectl exec hdfs-namenode-0 -n bigdata -- hdfs dfs -ls /raw/students/$(date +%Y-%m-%d)

# Check if data exists for a different date
kubectl exec hdfs-namenode-0 -n bigdata -- hdfs dfs -ls /raw/students/
```

---

## 📅 Scheduled Execution

Batch processing runs automatically daily at 2:30 AM:

```bash
# Check schedule
kubectl get cronjob batch-processing-daily -n bigdata

# Suspend scheduling (for maintenance)
kubectl patch cronjob batch-processing-daily -n bigdata \
  -p '{"spec":{"suspend":true}}'

# Resume scheduling
kubectl patch cronjob batch-processing-daily -n bigdata \
  -p '{"spec":{"suspend":false}}'
```

---

## 🔗 Integration with Next Steps

After batch processing completes:

1. **ML Pipeline** (Phase 3): Train prediction models
   - GPA prediction
   - Dropout risk classification
   
2. **Serving Layer** (Phase 4): Export to databases
   - MongoDB: Operational queries
   - PostgreSQL: Analytical queries
   
3. **Streaming Layer** (Phase 5): Real-time processing
   - Merge with real-time activity data
   - Generate alerts

---

## 📚 Additional Resources

- **Architecture**: See `architecture-design.md`
- **Spark Optimization**: See code comments in `batch_processing_complete.py`
- **HDFS Guide**: See `hdfs/README.md`
- **Deployment Guide**: See `deployment-guide.md`

---

## ✅ Success Criteria

Batch processing is working correctly if:

- ✅ All 10 views are created in HDFS
- ✅ Data is properly partitioned (check `/views/batch/YYYY-MM-DD/`)
- ✅ No failed jobs in last 24 hours
- ✅ Test script passes all checks
- ✅ Storage size is reasonable (~100MB-1GB depending on data)
- ✅ Processing completes in < 30 minutes

