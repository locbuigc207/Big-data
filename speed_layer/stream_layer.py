from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Khởi tạo Spark Session hỗ trợ Kafka
spark = SparkSession.builder \
    .appName("SpeedLayer_RealTimeProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Tắt log nhiễu
spark.sparkContext.setLogLevel("WARN")

# Định nghĩa Schema cho dữ liệu JSON từ Kafka (Superset schema cho các loại event)
common_schema = StructType([
    StructField("event_category", StringType()),
    StructField("user_id", StringType()),
    StructField("course_id", StringType()),
    StructField("video_id", StringType()),
    StructField("action", StringType()),
    StructField("timestamp", StringType())
])

# 1. PROCESS STREAM (Đọc từ Kafka)
# Đọc topic 'course_topic', 'video_topic', 'auth_topic'
# Use "localhost:9092" for local execution, "kafka:29092" for Docker execution
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "course_topic,video_topic,auth_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON từ cột 'value' của Kafka
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), common_schema).alias("data")) \
    .select("data.*")

# Chuyển đổi timestamp string sang timestamp type để dùng window
df_clean = df_parsed.withColumn("timestamp", col("timestamp").cast("timestamp"))

# 2. INCREMENT VIEWS (Tính toán thời gian thực)
# Query 1: Đếm số lượng view khóa học trong mỗi cửa sổ 1 phút
realtime_course_view = df_clean \
    .filter(col("action") == "COURSE_VIEW") \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minutes"),
        col("course_id")
    ) \
    .count() \
    .withColumnRenamed("count", "realtime_views")

# Query 2: Đếm số lượng view video trong mỗi cửa sổ 1 phút
realtime_video_view = df_clean \
    .filter(col("event_category") == "VIDEO") \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minutes"),
        col("video_id")
    ) \
    .count() \
    .withColumnRenamed("count", "video_views")

# Query 3: Đếm số lượng user active (Login) trong mỗi cửa sổ 1 phút
realtime_active_users = df_clean \
    .filter(col("action") == "LOGIN") \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minutes")
    ) \
    .agg(count("user_id").alias("active_users_count"))

# 3. OUTPUT REAL-TIME VIEWS
# Ghi ra Console

query1 = realtime_course_view.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='10 seconds') \
    .start()

query2 = realtime_video_view.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='10 seconds') \
    .start()

query3 = realtime_active_users.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='10 seconds') \
    .start()

# Chờ stream chạy
print("Đang chạy Real-time Stream Processing...")
spark.streams.awaitAnyTermination()