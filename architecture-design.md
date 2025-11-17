# Architecture and Design Document
## University Learning Analytics System

---

## 1. Overview

### 1.1 Project Context
- **Domain**: University Teaching and Learning Analytics
- **Data Scale**: 3,000+ students, 20 teachers, 40 courses across multiple semesters
- **Primary Use Cases**:
  - Real-time student engagement monitoring
  - Attendance tracking and alerts
  - Performance analytics and early warning systems
  - Learning behavior analysis
  - Academic performance prediction

### 1.2 Data Characteristics

#### Static/Batch Data (Historical)
- **Students**: Profile, faculty, enrollment history
- **Teachers**: Profile, specialty, teaching assignments
- **Courses**: Course metadata, credits, prerequisites
- **Classes**: Class sections per semester
- **Enrollments**: Student-class registrations
- **Grades**: Midterm, final scores with weighted calculations
- **Sessions**: Scheduled class sessions

#### Streaming Data (Real-time)
- **Activities**: Student learning activities (150,000+ events/month)
  - Login events
  - Video viewing sessions
  - Assignment submissions
  - Quiz attempts
  - Forum interactions
  - Material downloads
- **Attendance**: Real-time check-in/check-out events

---

## 2. Architecture Selection: **Lambda Architecture**

### 2.1 Why Lambda Architecture?

✅ **Best Fit Reasons**:

1. **Dual Data Nature**: 
   - Historical academic data (batch): Students, grades, enrollments
   - Real-time activities (streaming): Login, video views, submissions

2. **Different Query Patterns**:
   - **Batch**: Complex analytics (semester GPA, graduation rates, course difficulty analysis)
   - **Real-time**: Current engagement (who's online, live attendance, instant alerts)

3. **Data Accuracy vs. Speed Trade-off**:
   - Batch layer ensures accuracy for official records (transcripts, GPAs)
   - Speed layer provides immediate insights for intervention

4. **Reprocessing Capability**:
   - Academic policies change (grade calculation formulas)
   - Need to recompute historical analytics

5. **Project Requirements Alignment**:
   - Demonstrates both batch and streaming Spark processing
   - Shows complex joins, aggregations, and ML capabilities

### 2.2 Lambda vs Kappa Comparison

| Aspect | Lambda (Selected) | Kappa (Alternative) |
|--------|-------------------|---------------------|
| **Complexity** | Higher (2 pipelines) | Lower (1 pipeline) |
| **Reprocessing** | ✅ Full batch recomputation | Limited to replay window |
| **Accuracy** | ✅ Guaranteed via batch | Eventually consistent |
| **Latency** | Low (speed layer) | Very low |
| **Academic Records** | ✅ Perfect for immutable transcripts | Risk of data loss |
| **Cost** | Higher | Lower |
| **Best For** | ✅ Mixed workloads (our case) | Pure streaming |

**Decision**: Lambda Architecture is chosen for its ability to handle both historical academic records (requiring accuracy) and real-time student activities (requiring speed).

---

## 3. Detailed Architecture Components

### 3.1 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                    │
├─────────────────────────────────────────────────────────────────────────┤
│  Historical DB     LMS Events      Attendance      Grading System       │
│  (Students,        (Activity       System          (Scores)             │
│   Courses)         Logs)                                                │
└──────┬──────────────────┬──────────────┬────────────────┬───────────────┘
       │                  │              │                │
       │                  │              │                │
       ▼                  ▼              ▼                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐          ┌────────────────────────────────────┐  │
│  │  Batch Ingestion │          │    Streaming Ingestion             │  │
│  │  (Apache NiFi/   │          │    (Kafka Producers)               │  │
│  │   Sqoop/Script)  │          │                                    │  │
│  │                  │          │  Topics:                           │  │
│  │  - Students CSV  │          │  - student-activities              │  │
│  │  - Grades CSV    │          │  - real-time-attendance            │  │
│  │  - Enrollments   │          │  - assignment-submissions          │  │
│  └────────┬─────────┘          │  - quiz-attempts                   │  │
│           │                    └─────────┬──────────────────────────┘  │
│           ▼                              ▼                              │
│  ┌─────────────────┐          ┌──────────────────────┐                 │
│  │  HDFS (Raw)     │          │  Apache Kafka        │                 │
│  │  /raw/students  │          │  (Message Queue)     │                 │
│  │  /raw/grades    │          │  - Partitioned       │                 │
│  │  /raw/courses   │          │  - Replicated        │                 │
│  └─────────────────┘          │  - Retention: 7 days │                 │
│                               └──────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                ┌────────────────────┴────────────────────┐
                ▼                                         ▼
┌─────────────────────────────────┐    ┌──────────────────────────────────┐
│      BATCH LAYER                │    │      SPEED LAYER                 │
│      (Historical Truth)         │    │      (Real-time Views)           │
├─────────────────────────────────┤    ├──────────────────────────────────┤
│                                 │    │                                  │
│  Apache Spark (Batch)           │    │  Apache Spark Streaming          │
│                                 │    │  (Structured Streaming)          │
│  Processing:                    │    │                                  │
│  ┌───────────────────────────┐ │    │  Processing:                     │
│  │ 1. Data Cleaning          │ │    │  ┌────────────────────────────┐  │
│  │    - Deduplication        │ │    │  │ 1. Windowed Aggregations   │  │
│  │    - Validation           │ │    │  │    - 5-min activity counts │  │
│  │    - Standardization      │ │    │  │    - Engagement scores     │  │
│  ├───────────────────────────┤ │    │  ├────────────────────────────┤  │
│  │ 2. Complex Joins          │ │    │  │ 2. Stateful Processing     │  │
│  │    - Students ⋈ Enrollment│ │    │  │    - Session tracking      │  │
│  │    - Courses ⋈ Grades     │ │    │  │    - Watermarking (10 min) │  │
│  │    - Broadcast joins      │ │    │  ├────────────────────────────┤  │
│  ├───────────────────────────┤ │    │  │ 3. Real-time Alerts        │  │
│  │ 3. Aggregations           │ │    │  │    - Low engagement        │  │
│  │    - Semester GPA         │ │    │  │    - Attendance warnings   │  │
│  │    - Course pass rates    │ │    │  ├────────────────────────────┤  │
│  │    - Faculty analytics    │ │    │  │ 4. Stream-Stream Joins     │  │
│  │    - Window functions     │ │    │  │    - Activities + Attendance│ │
│  ├───────────────────────────┤ │    │  └────────────────────────────┘  │
│  │ 4. ML Models              │ │    │                                  │
│  │    - GPA prediction       │ │    │  Output Modes:                   │
│  │    - Dropout risk         │ │    │  - Append (alerts)               │
│  │    - Performance clusters │ │    │  - Update (dashboards)           │
│  ├───────────────────────────┤ │    │  - Complete (small aggs)         │
│  │ 5. Graph Analytics        │ │    │                                  │
│  │    - Student collaboration│ │    │  Exactly-once semantics          │
│  │    - Course dependencies  │ │    │  Checkpointing enabled           │
│  └───────────────────────────┘ │    │                                  │
│                                 │    │                                  │
│  Schedule: Daily @ 2 AM         │    │  Micro-batch: 10 seconds         │
│                                 │    │                                  │
│  ▼                              │    │  ▼                               │
│  ┌──────────────────────────┐  │    │  ┌──────────────────────────┐   │
│  │ HDFS Batch Views         │  │    │  │ Speed Views (Temp)       │   │
│  │ /views/batch/            │  │    │  │ In-memory state          │   │
│  │  - student_gpa           │  │    │  │ Checkpointed to HDFS     │   │
│  │  - course_analytics      │  │    │  └──────────────────────────┘   │
│  │  - faculty_performance   │  │    │                                  │
│  └──────────────────────────┘  │    │                                  │
└─────────────────────────────────┘    └──────────────────────────────────┘
                │                                        │
                │                                        │
                ▼                                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      SERVING LAYER                                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │              Query Merge & Serving Engine                       │    │
│  │                                                                  │    │
│  │  Logic: batch_view + speed_view (last 24h)                     │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─────────────────────┐         ┌──────────────────────────────┐      │
│  │  MongoDB             │         │  PostgreSQL/Cassandra        │      │
│  │  (Operational DB)    │         │  (Analytical Store)          │      │
│  │                      │         │                              │      │
│  │  Collections:        │         │  Tables:                     │      │
│  │  - current_students  │         │  - historical_grades         │      │
│  │  - live_sessions     │         │  - aggregated_analytics      │      │
│  │  - real_time_alerts  │         │  - ml_predictions            │      │
│  │  - activity_logs     │         │  - performance_metrics       │      │
│  └─────────────────────┘         └──────────────────────────────┘      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      PRESENTATION LAYER                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────────┐   │
│  │  Grafana/Kibana  │  │  Custom Web App  │  │  Jupyter Notebooks │   │
│  │  (Dashboards)    │  │  (Student Portal)│  │  (Ad-hoc Analysis) │   │
│  │                  │  │                  │  │                    │   │
│  │  - Live metrics  │  │  - My grades     │  │  - Research        │   │
│  │  - Admin views   │  │  - Attendance    │  │  - Data science    │   │
│  └──────────────────┘  └──────────────────┘  └────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Component Specifications

### 4.1 Ingestion Layer

#### 4.1.1 Batch Ingestion
**Purpose**: Load historical and periodic data

**Tools**:
- **Primary**: Apache NiFi (visual flow management)
- **Alternative**: Custom Python scripts + Airflow scheduler
- **Legacy Systems**: Apache Sqoop (if source is RDBMS)

**Data Sources**:
```
students.csv       → HDFS:/raw/students/YYYY-MM-DD/
teachers.csv       → HDFS:/raw/teachers/YYYY-MM-DD/
courses.csv        → HDFS:/raw/courses/YYYY-MM-DD/
classes.csv        → HDFS:/raw/classes/YYYY-MM-DD/
enrollments.csv    → HDFS:/raw/enrollments/YYYY-MM-DD/
grades.csv         → HDFS:/raw/grades/YYYY-MM-DD/
sessions.csv       → HDFS:/raw/sessions/YYYY-MM-DD/
```

**Schedule**: Daily incremental loads @ 2 AM

**Data Validation**:
- Schema validation
- Referential integrity checks
- Duplicate detection

#### 4.1.2 Streaming Ingestion
**Purpose**: Capture real-time student activities

**Tool**: Apache Kafka

**Configuration**:
```yaml
Topics:
  student-activities:
    partitions: 10
    replication-factor: 3
    retention: 168h (7 days)
    
  real-time-attendance:
    partitions: 5
    replication-factor: 3
    retention: 72h
    
  assignment-submissions:
    partitions: 8
    replication-factor: 3
    retention: 168h
    
  quiz-attempts:
    partitions: 6
    replication-factor: 3
    retention: 168h
```

**Producers**:
- LMS (Learning Management System) webhooks
- Mobile app events
- Attendance kiosks
- Web application clickstream

**Message Format** (JSON):
```json
{
  "event_id": "E00012345",
  "student_id": "S20220123",
  "class_id": "CL0045",
  "activity_type": "view_video",
  "timestamp": "2024-10-20T14:32:15",
  "duration_sec": 1850,
  "metadata": {
    "video_id": "vid_calculus_ch3",
    "completion_rate": 0.85
  }
}
```

---

### 4.2 Batch Layer (Historical Truth)

#### 4.2.1 Storage: HDFS
**Structure**:
```
/edu-analytics/
  ├── raw/                    # Raw ingested data
  │   ├── students/
  │   ├── grades/
  │   └── enrollments/
  │
  ├── processed/              # Cleaned & validated
  │   ├── students_clean/
  │   └── grades_validated/
  │
  ├── views/                  # Pre-computed views
  │   ├── batch/
  │   │   ├── student_gpa/
  │   │   ├── course_stats/
  │   │   └── faculty_analytics/
  │   └── ml_models/
  │       ├── gpa_predictor/
  │       └── dropout_risk/
  │
  └── checkpoints/            # Spark checkpoints
```

#### 4.2.2 Processing: Apache Spark (Batch)

**Key Operations**:

##### 1. Data Cleaning & Validation
```python
# Deduplication using window functions
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window = Window.partitionBy("student_id", "class_id").orderBy(col("timestamp").desc())
df_clean = df.withColumn("rn", row_number().over(window)) \
             .filter(col("rn") == 1) \
             .drop("rn")
```

##### 2. Complex Joins
```python
# Broadcast join for small dimension tables
from pyspark.sql.functions import broadcast

student_performance = grades_df \
    .join(broadcast(students_df), "student_id") \
    .join(broadcast(courses_df), "course_id") \
    .join(enrollments_df, ["student_id", "class_id"])
```

##### 3. Advanced Aggregations
```python
# Window functions for ranking
from pyspark.sql.functions import rank, percent_rank, ntile

ranked = grades_df.withColumn(
    "class_rank",
    rank().over(Window.partitionBy("class_id").orderBy(col("total_score").desc()))
).withColumn(
    "percentile",
    percent_rank().over(Window.partitionBy("class_id").orderBy("total_score"))
)
```

##### 4. Custom Aggregations
```python
# Custom UDAF for weighted GPA calculation
from pyspark.sql.functions import sum, when

gpa_df = grades_df \
    .join(courses_df, "course_id") \
    .groupBy("student_id", "semester") \
    .agg(
        (sum(col("total_score") * col("credits")) / sum("credits")).alias("gpa"),
        sum("credits").alias("total_credits")
    )
```

##### 5. Machine Learning
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor

# GPA Prediction Model
features = ["midterm_score", "attendance_rate", "activity_count", "previous_gpa"]
assembler = VectorAssembler(inputCols=features, outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="final_gpa")

pipeline = Pipeline(stages=[assembler, scaler, rf])
model = pipeline.fit(training_data)
```

##### 6. Graph Analytics (GraphFrames)
```python
from graphframes import GraphFrame

# Student collaboration network (forum posts, group projects)
vertices = students_df.select("student_id", "faculty")
edges = collaborations_df.select(
    col("student1_id").alias("src"),
    col("student2_id").alias("dst"),
    "interaction_count"
)

graph = GraphFrame(vertices, edges)
pagerank = graph.pageRank(resetProbability=0.15, maxIter=10)
communities = graph.labelPropagation(maxIter=5)
```

**Optimization Techniques**:
- **Partitioning**: By semester, faculty
- **Bucketing**: Grades table by student_id
- **Caching**: Frequently accessed dimension tables
- **Partition Pruning**: Filter by semester early
- **Predicate Pushdown**: Filter in data source
- **Broadcast Joins**: For dimension tables < 10MB

**Schedule**: Daily batch jobs via Apache Airflow

---

### 4.3 Speed Layer (Real-time Processing)

#### 4.3.1 Processing: Spark Structured Streaming

**Key Features**:
- **Micro-batch interval**: 10 seconds
- **Watermarking**: 10 minutes (handle late arrivals)
- **State management**: Checkpointed to HDFS
- **Exactly-once semantics**: Idempotent writes

**Streaming Jobs**:

##### 1. Real-time Engagement Monitoring
```python
from pyspark.sql.functions import window, count, avg

engagement_stream = kafka_stream \
    .filter(col("activity_type").isin(["login", "view_video", "submit_assignment"])) \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes", "1 minute"),
        "class_id"
    ).agg(
        count("*").alias("activity_count"),
        countDistinct("student_id").alias("active_students")
    )

query = engagement_stream.writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("checkpointLocation", "/checkpoints/engagement") \
    .start()
```

##### 2. Attendance Alerts
```python
# Detect students missing classes
attendance_stream = kafka_stream \
    .filter(col("activity_type") == "attendance") \
    .groupBy("student_id", "class_id") \
    .agg(
        sum(when(col("status") == "absent", 1).otherwise(0)).alias("absent_count")
    ) \
    .filter(col("absent_count") >= 3)  # Alert threshold

# Trigger alerts
alert_query = attendance_stream.writeStream \
    .foreach(send_alert_to_advisor)  # Custom ForeachWriter
    .start()
```

##### 3. Stateful Session Tracking
```python
from pyspark.sql.functions import session_window

# Track video watching sessions
video_sessions = kafka_stream \
    .filter(col("activity_type") == "view_video") \
    .withWatermark("timestamp", "30 minutes") \
    .groupBy(
        "student_id",
        "class_id",
        session_window("timestamp", "15 minutes")  # Gap duration
    ).agg(
        sum("duration_sec").alias("total_watch_time"),
        count("*").alias("video_segments")
    )
```

##### 4. Stream-Stream Joins
```python
# Join activity stream with attendance stream
activities = kafka_stream_activities.alias("act")
attendance = kafka_stream_attendance.alias("att")

joined = activities \
    .withWatermark("timestamp", "10 minutes") \
    .join(
        attendance.withWatermark("timestamp", "10 minutes"),
        expr("""
            act.student_id = att.student_id AND
            act.class_id = att.class_id AND
            att.timestamp >= act.timestamp - interval 2 hours AND
            att.timestamp <= act.timestamp + interval 2 hours
        """)
    )
```

**Output Modes**:
- **Append**: For immutable events (alerts, logs)
- **Update**: For dashboards (current metrics)
- **Complete**: For small aggregations (top 10 lists)

---

### 4.4 Serving Layer

#### 4.4.1 Query Merging Logic
```python
def get_student_gpa(student_id, semester):
    """
    Merge batch and speed views
    """
    # Batch view (up to yesterday)
    batch_gpa = query_hdfs_view(f"/views/batch/student_gpa/{semester}")
    
    # Speed view (today's updates)
    speed_updates = query_mongodb("current_grades", {
        "student_id": student_id,
        "timestamp": {"$gte": today_midnight}
    })
    
    # Merge: speed overrides batch
    return merge_views(batch_gpa, speed_updates)
```

#### 4.4.2 Storage Systems

##### MongoDB (NoSQL)
**Purpose**: Operational queries, real-time data

**Collections**:
```javascript
// Real-time student activities
db.student_activities {
  _id: ObjectId,
  student_id: "S20220123",
  class_id: "CL0045",
  activity_type: "view_video",
  timestamp: ISODate("2024-10-20T14:32:15Z"),
  metadata: {...}
}

// Live engagement metrics
db.live_engagement {
  _id: "CL0045_2024-10-20T14:30:00",
  class_id: "CL0045",
  window_start: ISODate(...),
  active_students: 45,
  activity_count: 230
}

// Alerts
db.alerts {
  _id: ObjectId,
  student_id: "S20220123",
  alert_type: "low_attendance",
  severity: "high",
  timestamp: ISODate(...),
  acknowledged: false
}
```

**Indexes**:
```javascript
db.student_activities.createIndex({ "student_id": 1, "timestamp": -1 })
db.student_activities.createIndex({ "class_id": 1, "activity_type": 1 })
db.live_engagement.createIndex({ "class_id": 1, "window_start": -1 })
```

##### PostgreSQL/Cassandra
**Purpose**: Analytical queries, historical aggregates

**Schema**:
```sql
-- Historical GPA records
CREATE TABLE student_gpa (
    student_id VARCHAR(20),
    semester VARCHAR(10),
    gpa DECIMAL(3,2),
    total_credits INT,
    class_rank INT,
    calculated_at TIMESTAMP,
    PRIMARY KEY (student_id, semester)
);

-- Course analytics
CREATE TABLE course_statistics (
    course_id VARCHAR(20),
    semester VARCHAR(10),
    avg_score DECIMAL(4,2),
    pass_rate DECIMAL(4,2),
    enrollment_count INT,
    dropout_count INT,
    PRIMARY KEY (course_id, semester)
);

-- ML predictions
CREATE TABLE dropout_predictions (
    student_id VARCHAR(20),
    prediction_date DATE,
    risk_score DECIMAL(4,3),
    contributing_factors JSONB,
    PRIMARY KEY (student_id, prediction_date)
);
```

---

### 4.5 Deployment (Kubernetes)

#### 4.5.1 Cluster Architecture
```yaml
Kubernetes Cluster:
  Master Nodes: 3 (HA)
  Worker Nodes: 
    - Spark Driver: 2 nodes (4 CPU, 16GB RAM)
    - Spark Executors: 10 nodes (8 CPU, 32GB RAM)
    - Kafka Brokers: 3 nodes (4 CPU, 16GB RAM)
    - Database: 3 nodes (8 CPU, 64GB RAM)
    - Monitoring: 2 nodes (2 CPU, 8GB RAM)
```

#### 4.5.2 Key Components

**Spark on K8s**:
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: spark-batch-job
spec:
  containers:
  - name: spark-driver
    image: spark:3.5.0-python3
    resources:
      requests:
        memory: "4Gi"
        cpu: "2"
      limits:
        memory: "8Gi"
        cpu: "4"
    env:
    - name: SPARK_EXECUTOR_INSTANCES
      value: "10"
    - name: SPARK_EXECUTOR_MEMORY
      value: "8g"
    - name: SPARK_EXECUTOR_CORES
      value: "4"
```

**Kafka StatefulSet**:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  serviceName: kafka-headless
  replicas: 3
  selector:
    matchLabels:
      app: kafka
  template:
    spec:
      containers:
      - name: kafka
        image: confluentinc/cp-kafka:7.5.0
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
        volumeMounts:
        - name: kafka-data
          mountPath: /var/lib/kafka/data
  volumeClaimTemplates:
  - metadata:
      name: kafka-data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 100Gi
```

**HDFS on K8s** (using operator):
```yaml
apiVersion: hdfs.stackable.tech/v1alpha1
kind: HdfsCluster
metadata:
  name: edu-hdfs
spec:
  nameNodes:
    replicas: 2
    resources:
      memory: 4Gi
      cpu: 2
  dataNodes:
    replicas: 3
    resources:
      memory: 8Gi
      cpu: 4
    storage:
      capacity: 500Gi
```

---

## 5. Data Flow Diagrams

### 5.1 Batch Processing Flow

```
┌─────────────┐
│ Source CSVs │
│ (Daily dump)│
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│ Apache NiFi     │
│ - GetFile       │
│ - ValidateCSV   │
│ - PutHDFS       │
└──────┬──────────┘
       │
       ▼
┌─────────────────────────────────────┐
│ HDFS: /raw/students/2024-10-20/     │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│ Spark Batch Job (Scheduled 2 AM)   │
│                                     │
│ 1. Load data from /raw              │
│ 2. Clean & validate                 │
│    - Remove duplicates              │
│    - Fix data types                 │
│    - Handle nulls                   │
│                                     │
│ 3. Complex transformations          │
│    - Join students + enrollments    │
│    - Join grades + courses          │
│    - Calculate GPA with weights     │
│    - Rank students per class        │
│                                     │
│ 4. Aggregations                     │
│    - Semester statistics            │
│    - Faculty performance            │
│    - Course difficulty metrics      │
│                                     │
│ 5. ML training                      │
│    - Feature engineering            │
│    - Train GPA predictor            │
│    - Train dropout classifier       │
│    - Model persistence              │
│                                     │
│ 6. Write batch views                │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│ HDFS: /views/batch/                 │
│  - student_gpa/                     │
│  - course_analytics/                │
│  - ml_predictions/                  │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│ Load to Serving Databases           │
│  - PostgreSQL (analytics)           │
│  - MongoDB (operational)            │
└─────────────────────────────────────┘
```

### 5.2 Streaming Processing Flow

```
┌──────────────┐
│ LMS Platform │
│ (Real-time   │
│  events)     │
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────┐
│ Kafka Producer (LMS Connector)  │
│ - Serialize to JSON             │
│ - Add metadata                  │
│ - Send to topic                 │
└──────┬──────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ Kafka Topic: student-activities         │
│ Partitions: 10 (by student_id hash)     │
│ Replication: 3                          │
└──────┬──────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ Spark Structured Streaming              │
│                                         │
│ Micro-batch: 10 seconds                 │
│                                         │
│ 1. Read from Kafka                      │
│    - Subscribe to topics                │
│    - Parse JSON                         │
│    - Add watermark (10 min)             │
│                                         │
│ 2. Transformations                      │
│    - Filter valid events                │
│    - Enrich with reference data         │
│    - Calculate derived metrics          │
│                                         │
│ 3. Windowed Aggregations                │
│    - 5-min tumbling window              │
│    - Activity counts by class           │
│    - Engagement scores                  │
│    - Session durations                  │
│                                         │
│ 4. Stateful Processing                  │
│    - Track user sessions                │
│    - Cumulative metrics                 │
│    - Pattern detection                  │
│                                         │
│ 5. Join with batch data                 │
│    - Enrich with student profile        │
│    - Add course information             │
│    - Historical context                 │
│                                         │
│ 6. Alert generation                     │
│    - Low engagement detection           │
│    - Attendance warnings                │
│    - Performance anomalies              │
└──────┬──────────────────────────────────┘
       │
       ├─────────────────────┬──────────────────┐
       ▼                     ▼                  ▼
┌──────────────┐   ┌──────────────────┐  ┌──────────────┐
│ MongoDB      │   │ Kafka (Alerts)   │  │ HDFS         │
│ (Live views) │   │ (Notifications)  │  │ (Checkpoint) │
└──────────────┘   └──────────────────┘  └──────────────┘
```

### 5.3 End-to-End Query Flow

```
┌─────────────────┐
│ User Request:   │
│ "Show my GPA    │
│  for 2024-2"    │
└────────┬────────┘
         │
         ▼
┌────────────────────────────────────┐
│ API Gateway / Web Application      │
└────────┬───────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────┐
│ Serving Layer Query Engine                     │
│                                                │
│ Logic:                                         │
│  1. Check if data in speed layer (last 24h)   │
│  2. Fetch batch view (historical truth)       │
│  3. Merge: speed overrides batch              │
│  4. Return combined result                    │
└────────┬───────────────────────────────────────┘
         │
         ├──────────────────┬──────────────────┐
         ▼                  ▼                  ▼
┌─────────────────┐  ┌─────────────┐  ┌──────────────┐
│ Query MongoDB   │  │ Query HDFS  │  │ Query        │
│ (Real-time)     │  │ (Batch view)│  │ PostgreSQL   │
│                 │  │             │  │ (Aggregates) │
│ Collection:     │  │ Path:       │  │              │
│ current_grades  │  │ /views/     │  │ Table:       │
│                 │  │ batch/      │  │ student_gpa  │
│ Filter:         │  │ student_gpa/│  │              │
│ {student_id,    │  │             │  │              │
│  semester,      │  │             │  │              │
│  timestamp >    │  │             │  │              │
│  yesterday}     │  │             │  │              │
└─────────────────┘  └─────────────┘  └──────────────┘
         │                  │                  │
         └──────────────────┴──────────────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Merge Results         │
                │                       │
                │ student_gpa = {       │
                │   batch: 3.45,        │
                │   speed: +0.15,       │
                │   current: 3.60       │
                │ }                     │
                └───────────┬───────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Return to User        │
                │                       │
                │ "Your GPA: 3.60"      │
                │ "Rank: 25/120"        │
                │ "Credits: 18"         │
                └───────────────────────┘
```

---

## 6. Component Interactions

### 6.1 Interaction Matrix

| Component | Interacts With | Protocol/Interface | Purpose |
|-----------|---------------|-------------------|---------|
| **NiFi** | HDFS | WebHDFS REST API | Upload raw CSV files |
| **Kafka** | Spark Streaming | Kafka Consumer API | Stream events |
| **Spark Batch** | HDFS | Hadoop FileSystem API | Read/write data |
| **Spark Batch** | PostgreSQL | JDBC | Write batch views |
| **Spark Streaming** | Kafka | Structured Streaming API | Consume messages |
| **Spark Streaming** | MongoDB | Mongo Spark Connector | Write speed views |
| **Spark Streaming** | HDFS | HDFS API | Checkpointing |
| **API Gateway** | MongoDB | MongoDB Driver | Query real-time data |
| **API Gateway** | PostgreSQL | JDBC/ORM | Query aggregates |
| **Grafana** | PostgreSQL | PostgreSQL Data Source | Visualize analytics |
| **Web App** | API Gateway | REST/GraphQL | User queries |

### 6.2 Data Consistency Model

```
┌─────────────────────────────────────────────────┐
│ Consistency Guarantees                          │
├─────────────────────────────────────────────────┤
│                                                 │
│ Batch Layer:                                    │
│  ✓ Strong consistency (recomputed from source) │
│  ✓ Eventually consistent with sources          │
│  ✓ Idempotent processing                        │
│                                                 │
│ Speed Layer:                                    │
│  ✓ Exactly-once semantics (Spark checkpoints)  │
│  ✓ At-least-once delivery (Kafka)              │
│  ✓ Watermarking for late data                  │
│                                                 │
│ Serving Layer:                                  │
│  ✓ Read-your-writes (within session)           │
│  ✓ Eventual consistency (batch + speed merge)  │
│  ✓ Speed view has priority over batch           │
│                                                 │
│ Trade-off:                                      │
│  - Batch: Accurate but delayed (1-24h)         │
│  - Speed: Fast but approximate                  │
│  - Combined: Best of both worlds                │
└─────────────────────────────────────────────────┘
```

---

## 7. Advanced Spark Demonstrations

### 7.1 Complex Aggregations

```python
# Example: Multi-dimensional analysis with CUBE
from pyspark.sql.functions import cube

analytics = grades_df \
    .join(students_df, "student_id") \
    .join(courses_df, "course_id") \
    .cube("semester", "faculty", "course_name") \
    .agg(
        avg("total_score").alias("avg_score"),
        count("*").alias("student_count"),
        sum(when(col("passed") == True, 1).otherwise(0)).alias("pass_count")
    ) \
    .withColumn("pass_rate", col("pass_count") / col("student_count"))
```

### 7.2 Window Functions

```python
# Rank students within each class and calculate moving averages
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, lag, lead, avg

window_class = Window.partitionBy("class_id").orderBy(col("total_score").desc())
window_student_time = Window.partitionBy("student_id").orderBy("semester").rowsBetween(-1, 1)

result = grades_df \
    .withColumn("rank", rank().over(window_class)) \
    .withColumn("dense_rank", dense_rank().over(window_class)) \
    .withColumn("prev_score", lag("total_score", 1).over(window_student_time)) \
    .withColumn("next_score", lead("total_score", 1).over(window_student_time)) \
    .withColumn("moving_avg_3sem", avg("total_score").over(window_student_time))
```

### 7.3 Pivot/Unpivot

```python
# Pivot: Create semester-wise score matrix
pivoted = grades_df \
    .groupBy("student_id") \
    .pivot("semester", ["2023-1", "2023-2", "2024-1", "2024-2"]) \
    .agg(avg("total_score"))

# Unpivot (stack)
from pyspark.sql.functions import expr

unpivoted = pivoted.select(
    "student_id",
    expr("stack(4, '2023-1', `2023-1`, '2023-2', `2023-2`, '2024-1', `2024-1`, '2024-2', `2024-2`) as (semester, score)")
)
```

### 7.4 Custom UDFs

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Custom grade classification
def classify_performance(gpa, attendance_rate):
    if gpa >= 3.6 and attendance_rate >= 0.9:
        return "Excellent"
    elif gpa >= 3.0 and attendance_rate >= 0.8:
        return "Good"
    elif gpa >= 2.0:
        return "Average"
    else:
        return "At Risk"

classify_udf = udf(classify_performance, StringType())

students_classified = student_metrics.withColumn(
    "performance_category",
    classify_udf(col("gpa"), col("attendance_rate"))
)
```

### 7.5 Broadcast Joins

```python
from pyspark.sql.functions import broadcast

# Efficiently join large grades table with small courses table
result = grades_df.join(
    broadcast(courses_df),
    "course_id"
)

# Broadcast hash join is automatically chosen for tables < 10MB
# Override with: spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
```

### 7.6 Performance Optimization

```python
# Partition pruning
partitioned = grades_df.write \
    .partitionBy("semester", "faculty") \
    .parquet("/data/grades_partitioned")

# Query with partition filter (scans only relevant partitions)
filtered = spark.read.parquet("/data/grades_partitioned") \
    .filter((col("semester") == "2024-2") & (col("faculty") == "IT1"))

# Bucketing for join optimization
grades_df.write \
    .bucketBy(50, "student_id") \
    .sortBy("student_id") \
    .saveAsTable("grades_bucketed")

enrollments_df.write \
    .bucketBy(50, "student_id") \
    .sortBy("student_id") \
    .saveAsTable("enrollments_bucketed")

# Join on bucketed tables avoids shuffle
result = spark.table("grades_bucketed").join(
    spark.table("enrollments_bucketed"),
    "student_id"
)
```

---

## 8. Monitoring and Observability

### 8.1 Monitoring Stack

```yaml
Components:
  - Prometheus: Metrics collection
  - Grafana: Visualization
  - ELK Stack: Log aggregation
  - Jaeger: Distributed tracing
```

### 8.2 Key Metrics

**Batch Layer**:
- Job execution time
- Records processed per job
- Data quality metrics (null rates, duplicates)
- Shuffle read/write sizes
- Task skew

**Speed Layer**:
- End-to-end latency (Kafka → MongoDB)
- Records processed per second
- Watermark delay
- State store size
- Checkpoint duration

**Infrastructure**:
- Kafka consumer lag
- HDFS storage utilization
- Database query latency
- Kubernetes pod health

---

## 9. Security Considerations

```
Authentication:
  - LDAP/Active Directory integration
  - OAuth 2.0 for APIs
  
Authorization:
  - RBAC (Students, Teachers, Admins)
  - Row-level security (students see own data only)
  
Encryption:
  - Data at rest: HDFS encryption zones
  - Data in transit: TLS/SSL
  - Kafka: SSL/SASL authentication
  
Data Privacy:
  - GDPR compliance (right to be forgotten)
  - PII anonymization in analytics
  - Audit logging
```

---

## 10. Summary

### 10.1 Architecture Highlights

✅ **Lambda Architecture** chosen for:
- Dual processing (batch + streaming)
- High accuracy (batch) + low latency (speed)
- Reprocessing capability
- Perfect fit for educational analytics

✅ **Technology Stack**:
- **Storage**: HDFS (distributed), MongoDB (NoSQL), PostgreSQL (RDBMS)
- **Processing**: Spark (batch + streaming)
- **Messaging**: Kafka (reliable, scalable)
- **Deployment**: Kubernetes (production-ready)

✅ **Spark Capabilities Demonstrated**:
- Complex joins (broadcast, sort-merge)
- Window functions & ranking
- Pivot/unpivot operations
- Custom UDFs and UDAFs
- ML pipelines (GPA prediction, dropout risk)
- Graph analytics (student networks)
- Streaming with watermarking
- Performance optimization (partitioning, bucketing, caching)

✅ **Production-Ready Features**:
- Exactly-once semantics
- Fault tolerance (checkpointing)
- Monitoring & alerting
- Security & privacy
- Scalability (Kubernetes auto-scaling)

### 10.2 Next Steps

1. **Phase 1**: Set up infrastructure (HDFS, Kafka, K8s)
2. **Phase 2**: Implement batch pipeline
3. **Phase 3**: Implement streaming pipeline
4. **Phase 4**: Build serving layer & API
5. **Phase 5**: Create dashboards & ML models
6. **Phase 6**: Testing, optimization, documentation

---

**Document Version**: 1.0  
**Last Updated**: 2025-10-20  
**Authors**: [Your Team Name]
