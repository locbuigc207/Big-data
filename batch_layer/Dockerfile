FROM python:3.9-slim-bookworm

# Install Java (Required for PySpark) and curl (for downloading JARs)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

# Install Python dependencies
# Install Python dependencies
RUN pip install pyspark==3.5.0

# Download Spark S3 support JARs (Baked into image to avoid runtime downloads)
RUN mkdir -p /app/jars && \
    curl -o /app/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /app/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Configure Spark to use these JARs
ENV SPARK_EXTRA_CLASSPATH="/app/jars/hadoop-aws-3.3.4.jar:/app/jars/aws-java-sdk-bundle-1.12.262.jar"

# Copy application code
COPY batch_layer/ /app/batch_layer/

# Default command
CMD ["python", "batch_layer/run_batch_jobs.py"]
