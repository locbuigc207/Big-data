FROM python:3.9-slim

WORKDIR /app
ENV PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# Install Python dependencies
COPY serving_layer/requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY serving_layer/ /app/serving_layer/

# Expose ports
EXPOSE 8000
EXPOSE 8501

# Default command (overridden by K8s)
CMD ["uvicorn", "serving_layer.serving_layer:app", "--host", "0.0.0.0", "--port", "8000"]
