from kafka import KafkaConsumer
import json

# Khởi tạo consumer
consumer = KafkaConsumer(
    'student-activity',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # đọc từ đầu topic
    enable_auto_commit=True,
    group_id='test-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("🔎 Đang chờ dữ liệu...")

for message in consumer:
    data = message.value
    print(f"📥 Nhận được: {data}")
