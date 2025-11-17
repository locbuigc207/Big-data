# BTLbigdata-group30

Hệ thống thu thập, lưu trữ, phân tích và xử lý kết quả học tập của sinh viên để dự đoán điểm số

## 📋 Tuần 5 - Phân chia công việc

### Ingestion Layer

- **Kafka streaming**: Thịnh, Phú, Tiến
- **Batch ingestion to HDFS**: Lâm, Lộc

**Mục tiêu**: Trong 1 tuần phải xong ingestion layer

---

## 📚 Kafka Learning Resources

### For Streaming Team (Thịnh, Phú, Tiến)

**Start Here**: [`kafka/README.md`](kafka/README.md)

**Learning Path** (1 week):

1. **Day 1-2**: Understand Kafka basics (`kafka/README.md` sections 1-2)
2. **Day 3-4**: Complete tutorials (`kafka/01-basic-producer-consumer/`, `kafka/02-json-messages/`)
3. **Day 5-6**: Implement project examples (`kafka/project-examples/`)
4. **Day 7**: Integration testing & documentation

**Key Files**:

- 📖 `kafka/README.md` - Complete learning guide
- 🎯 `kafka/01-basic-producer-consumer/` - Your first Kafka app
- 📊 `kafka/02-json-messages/` - Working with structured data
- 🚀 `kafka/project-examples/` - Production-ready code for our project

**What You'll Build**:

- Student activity producer (send events to Kafka)
- Spark Structured Streaming consumer (process events in real-time)
- Integration with MongoDB (store processed data)

---

## 🗂️ Project Structure

```
BTLbigdata-group30/
├── kafka/                          # Kafka learning & examples (NEW!)
│   ├── README.md                   # Complete Kafka guide
│   ├── 01-basic-producer-consumer/ # Tutorial 1
│   ├── 02-json-messages/           # Tutorial 2
│   ├── 03-partitions/              # Tutorial 3 (coming soon)
│   ├── 04-consumer-groups/         # Tutorial 4 (coming soon)
│   └── project-examples/           # Production code
│       ├── student_activity_producer.py
│       ├── attendance_producer.py
│       └── spark_streaming_consumer.py
│
├── generate_fake_data/             # Data generation (existing)
├── problem-definition.md           # Project requirements
├── architecture-design.md          # System architecture
├── deployment-guide.md             # Setup instructions
└── docker-compose.yml              # Local development environment
```

---

## 🚀 Quick Start for Kafka Team

```powershell
# 1. Start Kafka
docker-compose up -d zookeeper kafka

# 2. Install Python dependencies
pip install kafka-python pyspark

# 3. Run your first Kafka app
cd kafka/01-basic-producer-consumer
python producer.py  # Terminal 1
python consumer.py  # Terminal 2

# 4. See messages flowing!
```

---

## 📞 Support

- **Questions about Kafka?** → Check `kafka/README.md` or ask in team chat
- **Stuck on a tutorial?** → Review the code comments (detailed explanations)
- **Need help?** → Contact team leads

---

**Next Milestone**: Ingestion layer complete by end of Week 5! 🎯
