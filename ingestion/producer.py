from kafka import KafkaProducer
import json
import time
import os

# Đường dẫn đến file JSON
DATA_PATH = "D:/ma_nguon/BTLbigdata-group30/generate_fake_data/activities.json"

# Khởi tạo producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Đọc file JSON
with open(DATA_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Gửi từng bản ghi
for record in data:
    producer.send('student-activity', value=record)
    print(f"📤 Sent: {record}")
    time.sleep(0.5)  # cho dễ quan sát

producer.flush()
print("✅ Gửi dữ liệu xong.")
