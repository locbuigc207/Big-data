# Kafka Streaming System for Student Activity Analytics

A complete Kafka-based streaming system that simulates and processes real-time student activity events for educational analytics.

## 🎯 Overview

This system demonstrates a production-ready Kafka streaming architecture for processing student learning activities in real-time. It includes:

- **Signal Simulator (Producer)**: Generates realistic student activity events
- **Stream Processor (Consumer)**: Consumes and analyzes events in real-time
- **Kafka Infrastructure**: Complete Docker-based Kafka ecosystem with monitoring

## 📊 System Architecture

```
┌─────────────────┐
│ Signal Simulator│
│   (Producer)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│  Kafka Broker   │◄────►│   Zookeeper     │
│  (localhost:9092)│      │  (localhost:2181)│
└────────┬────────┘      └─────────────────┘
         │                        ▲
         │                        │
         │                ┌───────┴────────┐
         │                │   Kafka UI     │
         │                │(localhost:8080)│
         │                └────────────────┘
         ▼
┌─────────────────┐
│Stream Consumer  │
│  (Processor)    │
└─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (running)
- Python 3.8+
- PowerShell (Windows)

### 1. Start the System

```powershell
cd kafka
.\start.ps1
```

This will:

- Start Zookeeper and Kafka broker
- Start Kafka UI for monitoring
- Create necessary topics
- Install Python dependencies

### 2. Run Signal Simulator

Open a new terminal:

```powershell
cd kafka
python signal_simulator.py
```

Options:

```powershell
# Continuous mode (default) - 5 events/second
python signal_simulator.py --mode continuous --rate 5.0

# Burst mode - send 1000 events quickly
python signal_simulator.py --mode burst --events 1000

# Run for specific duration (60 seconds)
python signal_simulator.py --mode continuous --rate 10 --duration 60
```

### 3. Run Stream Consumer

Open another terminal:

```powershell
cd kafka
python stream_consumer.py
```

Options:

```powershell
# Consume from specific topics
python stream_consumer.py --topics student-activities student-attendance

# Use different consumer group
python stream_consumer.py --group my-analytics-group

# Change statistics interval
python stream_consumer.py --stats-interval 50
```

### 4. Monitor with Kafka UI

Open your browser:

```
http://localhost:8080
```

### 5. Run Quick Test

```powershell
.\test.ps1
```

### 6. Stop the System

```powershell
.\stop.ps1
```

## 📦 Event Types

The simulator generates the following event types:

### 1. LOGIN Events

Student login activities with device and location info.

```json
{
  "event_id": "uuid",
  "event_type": "LOGIN",
  "student_id": "STU00123",
  "timestamp": "2025-11-04T10:30:00",
  "device_type": "Desktop",
  "ip_address": "192.168.1.100",
  "session_id": "uuid",
  "browser": "Chrome"
}
```

### 2. VIDEO_VIEW Events

Video watching activities with completion tracking.

```json
{
  "event_id": "uuid",
  "event_type": "VIDEO_VIEW",
  "student_id": "STU00123",
  "course_id": "CS101",
  "timestamp": "2025-11-04T10:35:00",
  "video_id": "VID0042",
  "video_title": "Lecture 5",
  "duration_seconds": 1800,
  "watch_time_seconds": 1650,
  "completion_rate": 91.67,
  "quality": "1080p",
  "playback_speed": 1.25
}
```

### 3. ASSIGNMENT_SUBMIT Events

Assignment submission tracking.

```json
{
  "event_id": "uuid",
  "event_type": "ASSIGNMENT_SUBMIT",
  "student_id": "STU00123",
  "course_id": "CS101",
  "timestamp": "2025-11-04T11:00:00",
  "assignment_id": "ASG042",
  "assignment_title": "Assignment 3",
  "submission_status": "On Time",
  "file_count": 2,
  "file_size_mb": 5.3,
  "attempt_number": 1,
  "time_spent_minutes": 120
}
```

### 4. QUIZ_ATTEMPT Events

Quiz and test attempts with scores.

```json
{
  "event_id": "uuid",
  "event_type": "QUIZ_ATTEMPT",
  "student_id": "STU00123",
  "course_id": "CS101",
  "timestamp": "2025-11-04T14:00:00",
  "quiz_id": "QUZ023",
  "quiz_title": "Quiz 4",
  "total_questions": 20,
  "correct_answers": 18,
  "score": 90.0,
  "time_taken_minutes": 25,
  "attempt_number": 1
}
```

### 5. FORUM_POST Events

Forum interaction tracking.

```json
{
  "event_id": "uuid",
  "event_type": "FORUM_POST",
  "student_id": "STU00123",
  "course_id": "CS101",
  "timestamp": "2025-11-04T15:30:00",
  "thread_id": "THR0156",
  "post_type": "Answer",
  "word_count": 250,
  "has_attachments": true,
  "is_helpful": true
}
```

### 6. ATTENDANCE Events

Class attendance check-in/check-out.

```json
{
  "event_id": "uuid",
  "event_type": "ATTENDANCE",
  "student_id": "STU00123",
  "course_id": "CS101",
  "timestamp": "2025-11-04T08:00:00",
  "session_id": "SES0042",
  "session_type": "Lecture",
  "check_type": "CHECK_IN",
  "location": "Room A101",
  "teacher_id": "TCH005",
  "is_late": false,
  "minutes_late": 0
}
```

## 🎛️ Kafka Topics

The system uses the following topics:

| Topic                      | Partitions | Retention | Description                           |
| -------------------------- | ---------- | --------- | ------------------------------------- |
| `student-activities`       | 3          | 7 days    | All student learning activities       |
| `student-attendance`       | 2          | 7 days    | Attendance check-in/out events        |
| `student-grades`           | 2          | 30 days   | Grade records                         |
| `student-enrollments`      | 1          | 30 days   | Course enrollment events              |
| `student-engagement-score` | 2          | 7 days    | Calculated engagement scores          |
| `alerts`                   | 1          | 7 days    | Real-time alerts for at-risk students |

## 📈 Consumer Features

The stream consumer provides:

### Real-time Metrics

- Total activities per student
- Video watch time
- Assignment submissions
- Quiz performance
- Forum participation

### Engagement Scoring

Calculates a 0-100 engagement score based on:

- Login frequency (20 points)
- Video views (25 points)
- Assignment submissions (30 points)
- Quiz attempts (15 points)
- Forum posts (10 points)

### Alerting System

Generates alerts for:

- Low video completion rates (< 20%)
- Late assignment submissions
- Low quiz scores (< 40%)
- Frequent late arrivals (> 15 minutes)

### Statistics

- Top 5 most active students
- Event type distribution
- Processing throughput

## 🛠️ Advanced Configuration

### Environment Variables

Create a `.env` file from `.env.example`:

```powershell
copy .env.example .env
```

Edit values as needed:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONSUMER_GROUP=student-analytics-group
SIMULATOR_EVENTS_PER_SECOND=5.0
LOG_LEVEL=INFO
```

### Configuration File

Edit `config.ini` for detailed settings:

```ini
[kafka]
bootstrap_servers = localhost:9092
consumer_group_id = student-analytics-group

[simulator]
events_per_second = 5.0
num_students = 3000
num_courses = 40
```

## 🔍 Monitoring & Debugging

### Kafka UI

Access the web interface at `http://localhost:8080` to:

- View topics and messages
- Monitor consumer groups
- Check broker health
- View partition distribution

### Docker Logs

```powershell
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka
docker-compose logs -f zookeeper
docker-compose logs -f kafka-ui
```

### List Topics

```powershell
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### View Topic Messages

```powershell
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic student-activities \
  --from-beginning \
  --max-messages 10
```

### Check Consumer Groups

```powershell
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group student-analytics-group \
  --describe
```

## 📚 Project Structure

```
kafka/
├── docker-compose.yml          # Kafka infrastructure
├── signal_simulator.py         # Event producer
├── stream_consumer.py          # Event consumer
├── requirements.txt            # Python dependencies
├── config.ini                  # Configuration file
├── .env.example               # Environment template
├── start.ps1                  # Startup script
├── stop.ps1                   # Shutdown script
├── test.ps1                   # Quick test script
└── README.md                  # This file
```

## 🎓 Learning Resources

### Kafka Concepts

1. **Topics**: Named streams of records (e.g., `student-activities`)
2. **Partitions**: Topics split into partitions for parallelism
3. **Producers**: Applications that publish records to topics
4. **Consumers**: Applications that read records from topics
5. **Consumer Groups**: Multiple consumers working together
6. **Offsets**: Position of consumer in partition

### Key Features Demonstrated

- ✅ Topic creation and configuration
- ✅ Producer with key-based partitioning
- ✅ Consumer group coordination
- ✅ JSON serialization/deserialization
- ✅ Error handling and retries
- ✅ Graceful shutdown
- ✅ Real-time processing
- ✅ Monitoring and observability

## 🚨 Troubleshooting

### Kafka won't start

```powershell
# Check if ports are in use
netstat -ano | findstr "9092"
netstat -ano | findstr "2181"

# Remove old volumes and restart
docker-compose down -v
.\start.ps1
```

### Consumer not receiving messages

```powershell
# Check if producer is running
# Verify topics exist
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Reset consumer offset
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group student-analytics-group \
  --reset-offsets --to-earliest --execute \
  --topic student-activities
```

### Python dependency issues

```powershell
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

## 🎯 Next Steps

1. **Extend the Producer**: Add more event types (grades, materials, etc.)
2. **Add Spark Streaming**: Process with Spark Structured Streaming
3. **Add MongoDB**: Store processed results in MongoDB
4. **Add HDFS**: Batch processing with Hadoop
5. **Add Machine Learning**: Build prediction models with Spark MLlib
6. **Add Dashboards**: Visualize metrics with Grafana

## 📞 Support

For questions or issues:

1. Check the logs: `docker-compose logs -f`
2. Review Kafka UI: `http://localhost:8080`
3. Check documentation in `../architecture-design.md`

## 📄 License

This project is for educational purposes as part of BTL Big Data Course.

---

**Built with ❤️ by Group 30**
