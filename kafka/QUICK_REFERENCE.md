# Kafka Streaming System - Quick Reference

## 🚀 Common Commands

### Start/Stop System

```powershell
.\start.ps1              # Start everything
.\stop.ps1               # Stop everything
.\test.ps1               # Run quick test
```

### Run Producer

```powershell
# Continuous mode (5 events/sec)
python signal_simulator.py

# Custom rate
python signal_simulator.py --rate 10

# Burst mode
python signal_simulator.py --mode burst --events 1000

# Limited duration (60 seconds)
python signal_simulator.py --duration 60
```

### Run Consumer

```powershell
# Default (all topics)
python stream_consumer.py

# Specific topics
python stream_consumer.py --topics student-activities

# Custom consumer group
python stream_consumer.py --group my-group

# Show stats every 50 messages
python stream_consumer.py --stats-interval 50
```

### Docker Commands

```powershell
# View logs
docker-compose logs -f
docker-compose logs -f kafka

# Restart service
docker-compose restart kafka

# Stop and remove volumes (fresh start)
docker-compose down -v
```

### Kafka CLI Commands

#### Topics

```powershell
# List all topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topic
docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092 --topic student-activities

# Delete topic
docker exec kafka kafka-topics --delete --bootstrap-server localhost:9092 --topic my-topic
```

#### Consumer Groups

```powershell
# List consumer groups
docker exec kafka kafka-consumer-groups --list --bootstrap-server localhost:9092

# Describe group
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group student-analytics-group --describe

# Reset offsets to beginning
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group student-analytics-group --reset-offsets --to-earliest --execute --topic student-activities
```

#### Messages

```powershell
# Consume messages from beginning
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic student-activities --from-beginning --max-messages 10

# Consume with keys
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic student-activities --property print.key=true --from-beginning
```

## 🔍 Monitoring

### Kafka UI

```
http://localhost:8080
```

### Health Checks

```powershell
# Check if Kafka is healthy
docker ps --filter "name=kafka"
docker inspect --format='{{.State.Health.Status}}' kafka

# Check topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group student-analytics-group --describe
```

## 📊 Event Statistics

### Producer

- Default rate: 5 events/second
- Event types: LOGIN, VIDEO_VIEW, ASSIGNMENT_SUBMIT, QUIZ_ATTEMPT, FORUM_POST, MATERIAL_DOWNLOAD
- Attendance events: ~10% of total

### Consumer

- Processes all event types
- Calculates engagement scores
- Generates alerts
- Tracks metrics per student

## 🐛 Troubleshooting

### Port Already in Use

```powershell
# Find process using port 9092
netstat -ano | findstr "9092"

# Kill process (replace PID)
taskkill /PID <PID> /F
```

### Consumer Not Receiving Messages

1. Check if producer is running
2. Verify topics exist: `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092`
3. Reset consumer offsets (see above)
4. Check consumer group: `docker exec kafka kafka-consumer-groups --list --bootstrap-server localhost:9092`

### Kafka Won't Start

```powershell
# Remove old data and restart
docker-compose down -v
docker system prune -f
.\start.ps1
```

### Python Import Errors

```powershell
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

## 📈 Performance Tuning

### Producer

- Increase `--rate` for more events/second
- Use `--mode burst` for maximum throughput
- Adjust `linger_ms` and `batch_size` in code

### Consumer

- Increase `max_poll_records` for larger batches
- Use multiple consumers in same group for parallelism
- Adjust `statistics_interval` to reduce log spam

### Topics

- Increase partitions for more parallelism
- Adjust retention period based on needs
- Use compression for large messages

## 🎯 Key Metrics to Monitor

1. **Producer Throughput**: Events/second
2. **Consumer Lag**: Messages behind
3. **Partition Distribution**: Even distribution
4. **Event Types**: Distribution across types
5. **Engagement Scores**: Student activity levels
6. **Alert Rate**: Number of alerts generated

## 📁 Important Files

| File                  | Purpose                   |
| --------------------- | ------------------------- |
| `docker-compose.yml`  | Infrastructure definition |
| `signal_simulator.py` | Event producer            |
| `stream_consumer.py`  | Event consumer            |
| `requirements.txt`    | Python dependencies       |
| `config.ini`          | System configuration      |
| `.env`                | Environment variables     |
| `README.md`           | Full documentation        |

## 🔗 Useful Links

- Kafka UI: http://localhost:8080
- Kafka Broker: localhost:9092
- Zookeeper: localhost:2181

## 💡 Tips

1. Always start Docker before running `start.ps1`
2. Run producer and consumer in separate terminals
3. Use Kafka UI to visually inspect messages
4. Monitor consumer lag to ensure processing keeps up
5. Check logs if something isn't working
6. Use `test.ps1` to verify system is working

---

For detailed documentation, see `README.md`
