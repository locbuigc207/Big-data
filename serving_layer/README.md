# Serving Layer

The Serving Layer provides a Unified Query Interface merging Batch and Speed layers.
It consists of a REST API (FastAPI) and a Dashboard (Streamlit).

## Components

- **`serving_layer.py`**: FastAPI backend to query MinIO.
- **`dashboard.py`**: Streamlit frontend for visualization.
- **`Dockerfile`**: Multi-use image for both components.
- **`deployment.yaml`**: Kubernetes deployment/service.

## Architecture

```
User (Browser) 
  ↓
Serving Layer UI (Streamlit :8501)
  ↓
Serving Layer API (FastAPI :8000)
  ↓
MinIO (s3a://bucket-0)
  ├── batch_views/ (Historical)
  └── speed_views/ (Real-time)
```

## Running Locally (Docker)

```bash
# Build
docker build -t serving-layer ./serving_layer

# Run API
docker run -p 8000:8000 --env MINIO_ENDPOINT=http://host.docker.internal:9000 serving-layer uvicorn serving_layer:app --host 0.0.0.0

# Run UI
docker run -p 8501:8501 --env API_URL=http://host.docker.internal:8000 serving-layer streamlit run dashboard.py
```

## Running on Kubernetes

```bash
kubectl apply -f serving_layer/deployment.yaml
```

Access the Dashboard at NodePort 30001 (e.g., `http://<minikube-ip>:30001`).
