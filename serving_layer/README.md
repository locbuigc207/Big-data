# Serving Layer (Unified Query Interface)

## Overview

The serving layer provides a unified query interface that merges batch views (historical, high-latency) with speed views (recent, low-latency) to answer queries with complete, up-to-date results.

## Status

⏳ **TO BE IMPLEMENTED**

## Planned Components

### 1. Query Service (`serving_layer.py`)
- Unified query interface
- Merges batch + speed views
- REST API endpoints

### 2. View Merger
- Combines precomputed batch views
- Adds incremental speed layer updates
- Handles overlapping data

### 3. Query Optimization
- Cache frequently accessed views
- Lazy loading strategies
- Query result caching

## Architecture

```
User Query → Serving Layer → ┌─ Batch Views (MinIO)
                             └─ Speed Views (In-memory)
                                      ↓
                              Merged Result → Response
```

## Query Patterns

### Example: Student Video Engagement

```python
# Batch view: Historical data (up to yesterday)
batch_video = read_parquet("s3a://bucket-0/batch_views/video_total_watch_time")

# Speed view: Today's data (real-time)
speed_video = read_stream("kafka://video_topic")

# Merge: Complete view
total_engagement = batch_video.union(speed_video).groupBy("user_id").sum("watch_time")
```

## API Endpoints (Planned)

- `GET /api/v1/student/{id}/engagement` - Student engagement metrics
- `GET /api/v1/video/{id}/analytics` - Video analytics
- `GET /api/v1/course/{id}/statistics` - Course statistics
- `GET /api/v1/dashboard/overview` - System-wide dashboard

## Technologies

- Apache Spark (query engine)
- Flask/FastAPI (REST API)
- Redis (caching)
- MinIO (batch view storage)

## Next Steps

1. Implement view merger logic
2. Create REST API service
3. Add caching layer
4. Build dashboard UI
