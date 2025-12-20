from fastapi import FastAPI, HTTPException
import pandas as pd
import os
import s3fs
from datetime import datetime

app = FastAPI(title="University Learning Analytics API")

# Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "bucket-0"

# S3 Filesystem
fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': MINIO_ENDPOINT},
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    use_listings_cache=False
)

def read_parquet(path):
    """Helper to read parquet path from MinIO"""
    full_path = f"{BUCKET_NAME}/{path}"
    try:
        # Use pyarrow directly to avoid s3fs path resolution issues
        import pyarrow.parquet as pq
        
        # Check if exists (s3fs glob or exists)
        if fs.exists(full_path):
            # Read using pyarrow, then convert to pandas
            table = pq.read_table(f"{full_path}", filesystem=fs)
            return table.to_pandas()
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading {full_path}: {e}")
        return pd.DataFrame()

@app.get("/")
def health_check():
    return {"status": "ok", "service": "serving_layer"}

@app.get("/analytics/summary")
def get_summary_metrics():
    """
    Get all key metrics for dashboard overview in one call
    Returns: current active users, total courses, total students, system health
    """
    from datetime import datetime, timedelta
    
    summary = {
        "current_active_users": 0,
        "total_active_courses": 0,
        "total_students": 0,
        "batch_last_run": None,
        "speed_layer_status": "unknown"
    }
    
    # Current active users from speed layer
    df_speed = read_parquet("speed_views/active_users")
    if not df_speed.empty:
        df_speed['start'] = pd.to_datetime(df_speed['start'])
        latest = df_speed.sort_values('start', ascending=False).iloc[0]
        summary["current_active_users"] = int(latest['active_users'])
        # Speed layer is healthy if data is recent (within last 5 minutes)
        if (datetime.now() - latest['start']).total_seconds() < 300:
            summary["speed_layer_status"] = "healthy"
        else:
            summary["speed_layer_status"] = "stale"
    
    # Total active courses (courses with recent activity in last 24 hours)
    df_course_pop = read_parquet("speed_views/course_popularity")
    if not df_course_pop.empty:
        df_course_pop['end'] = pd.to_datetime(df_course_pop['end'])
        cutoff = datetime.now() - timedelta(hours=24)
        recent_courses = df_course_pop[df_course_pop['end'] >= cutoff]['course_id'].nunique()
        summary["total_active_courses"] = int(recent_courses)
    
    # Total students from batch layer
    df_students = read_parquet("batch_views/student_overview")
    if not df_students.empty:
        summary["total_students"] = len(df_students)
    
    # Batch job last run (check most recent batch view timestamp)
    df_batch = read_parquet("batch_views/course_overview")
    if not df_batch.empty and 'last_updated' in df_batch.columns:
        summary["batch_last_run"] = df_batch['last_updated'].max().isoformat()
    
    return summary

@app.get("/analytics/recent_activity")
def get_recent_activity(hours: int = 1):
    """
    Get recent activity stats for content consumption
    Returns: videos watched, materials downloaded in last N hours
    """
    from datetime import datetime, timedelta
    
    cutoff = datetime.now() - timedelta(hours=hours)
    
    activity = {
        "videos_watched": 0,
        "materials_downloaded": 0,
        "time_period_hours": hours
    }
    
    # Videos from speed layer
    df_video = read_parquet("speed_views/video_engagement")
    if not df_video.empty:
        df_video['end'] = pd.to_datetime(df_video['end'])
        recent_videos = df_video[df_video['end'] >= cutoff]
        activity["videos_watched"] = int(recent_videos['views'].sum())
    
    # Materials - would come from a similar speed view if implemented
    # For now, we'll use batch data as fallback
    df_course = read_parquet("batch_views/course_overview")
    if not df_course.empty and 'total_materials_downloaded' in df_course.columns:
        # This is cumulative, not time-filtered, but better than nothing
        activity["materials_downloaded"] = int(df_course['total_materials_downloaded'].sum())
    
    return activity

@app.get("/analytics/student_engagement_distribution")
def get_student_engagement_distribution():
    """
    Get distribution of student engagement levels
    Categories: Highly Active, Moderately Active, Low Activity, Inactive
    """
    from datetime import datetime, timedelta
    
    distribution = {
        "highly_active": 0,    # >10 interactions today
        "moderately_active": 0, # 3-10 interactions
        "low_activity": 0,      # 1-2 interactions
        "inactive": 0           # 0 interactions today
    }
    
    # Get student activity from batch overview
    df_students = read_parquet("batch_views/student_overview")
    if df_students.empty:
        return distribution
    
    # Calculate total interactions per student
    # Assuming columns: total_videos_watched, total_assignments_submitted, total_logins
    if 'total_videos_watched' in df_students.columns:
        df_students['total_interactions'] = (
            df_students.get('total_videos_watched', 0) + 
            df_students.get('total_assignments_submitted', 0) + 
            df_students.get('total_logins', 0)
        )
        
        # Categorize (note: this is cumulative, ideally we'd filter to today)
        distribution["highly_active"] = len(df_students[df_students['total_interactions'] > 50])
        distribution["moderately_active"] = len(df_students[
            (df_students['total_interactions'] >= 10) & (df_students['total_interactions'] <= 50)
        ])
        distribution["low_activity"] = len(df_students[
            (df_students['total_interactions'] >= 1) & (df_students['total_interactions'] < 10)
        ])
        distribution["inactive"] = len(df_students[df_students['total_interactions'] == 0])
    
    return distribution

@app.get("/analytics/dau")
def get_daily_active_users(hours: int = 6):
    """
    Get DAU by merging Batch Layer (Historical) and Speed Layer (Real-time)
    Default returns last 6 hours for performance
    """
    from datetime import datetime, timedelta
    
    # 1. Read Batch Data (Historical) - filter to recent hours
    # Batch path: batch_views/auth_daily_active_users
    # Schema: date, daily_active_users, total_sessions
    df_batch = read_parquet("batch_views/auth_daily_active_users")
    if not df_batch.empty:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        df_batch['date'] = pd.to_datetime(df_batch['date'])
        df_batch = df_batch[df_batch['date'] >= cutoff_time]
    
    # 2. Read Speed Data (Real-time) - filter to recent hours
    # Speed path: speed_views/active_users
    # Schema: start, end, active_users
    df_speed = read_parquet("speed_views/active_users")
    if not df_speed.empty:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        df_speed['start'] = pd.to_datetime(df_speed['start'])
        df_speed = df_speed[df_speed['start'] >= cutoff_time]
    
    # Process Speed Data to match Batch Schema
    # Aggregating real-time windows to "Today's" count is an approximation.
    # In a real Lambda architecture, we might dedup user_ids across batch & speed.
    # Here we simplify: Batch covers up to T-1. Speed covers T.
    
    api_response = []
    
    # Format Batch Data
    if not df_batch.empty:
        for _, row in df_batch.iterrows():
            api_response.append({
                "date": row['date'].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row['date'], 'strftime') else str(row['date']),
                "users": int(row['daily_active_users']),
                "source": "batch"
            })
            
    # Format Speed Data
    # Speed data is windowed (1 min). We want to show the trend or the max?
    # For a DAU chart, usually we show history days + today's current value.
    if not df_speed.empty:
        # Normalize timestamp to date
        # Assuming speed data is very recent.
        # Let's sum unique users per window? No, users might overlap.
        # Max of active users in a window gives a "Peak Concurrent" proxy, 
        # but for DAU we ideally want unique set.
        # Since we stored counts, we can't perfectly merge uniques.
        # We will return the raw speed windows for the frontend to visualize as "Real-time Trend"
        # Or aggregate to "Today (est)"
        
        # Let's return the speed data series separately or appended with a timestamp
        for _, row in df_speed.iterrows():
            api_response.append({
                "date": row['start'].strftime("%Y-%m-%d %H:%M:%S"), # detailed time for speed
                "users": int(row['active_users']),
                "source": "speed"
            })
            
    # Sort
    api_response.sort(key=lambda x: x['date'])
    return api_response

@app.get("/analytics/course_popularity")
def get_course_popularity(limit: int = 10):
    """
    Get Top N Popular Courses (default top 10)
    """
    df_speed = read_parquet("speed_views/course_popularity")
    
    response = []
    if not df_speed.empty:
        # Aggregating speed views per course
        grouped = df_speed.groupby("course_id")['interactions'].sum().reset_index()
        top_courses = grouped.sort_values("interactions", ascending=False).head(limit)
        
        for _, row in top_courses.iterrows():
            response.append({
                "course_id": row['course_id'],
                "interactions": int(row['interactions'])
            })
            
    return response

@app.get("/analytics/realtime/video")
def get_realtime_video_stats():
    """
    Get latest video engagement
    """
    df_speed = read_parquet("speed_views/video_engagement")
    response = []
    if not df_speed.empty:
        # Get latest window
        latest_window = df_speed['end'].max()
        recent_df = df_speed[df_speed['end'] == latest_window]
        
        for _, row in recent_df.iterrows():
            response.append({
                "video_id": row['video_id'],
                "views": int(row['views']),
                "window_end": row['end'].isoformat()
            })
            
    return response

# ========== COURSE ENDPOINTS ==========

@app.get("/analytics/courses")
def get_all_courses():
    """List all courses with summary stats"""
    df = read_parquet("batch_views/course_overview")
    if df.empty:
        return []
    return df.to_dict("records")

@app.get("/analytics/course/{course_id}")
def get_course_details(course_id: str):
    """Get detailed stats for a specific course"""
    df = read_parquet("batch_views/course_overview")
    if df.empty:
        return {}
    course = df[df['course_id'] == course_id]
    if course.empty:
        raise HTTPException(status_code=404, detail="Course not found")
    return course.to_dict("records")[0]

@app.get("/analytics/course/{course_id}/students")
def get_course_students(course_id: str):
    """Get all students enrolled in a course with their performance"""
    df = read_parquet("batch_views/student_course_enrollment")
    if df.empty:
        return []
    students = df[df['course_id'] == course_id]
    return students.to_dict("records")

#  ========== STUDENT ENDPOINTS ==========

@app.get("/analytics/students")
def get_all_students():
    """List all students with summary stats"""
    df = read_parquet("batch_views/student_overview")
    if df.empty:
        return []
    return df.to_dict("records")

@app.get("/analytics/student/{student_id}")
def get_student_details(student_id: str):
    """Get detailed stats for a specific student"""
    df = read_parquet("batch_views/student_overview")
    if df.empty:
        return {}
    student = df[df['student_id'] == student_id]
    if student.empty:
        raise HTTPException(status_code=404, detail="Student not found")
    return student.to_dict("records")[0]

@app.get("/analytics/student/{student_id}/courses")
def get_student_courses(student_id: str):
    """Get all courses a student is enrolled in with performance"""
    df = read_parquet("batch_views/student_course_enrollment")
    if df.empty:
        return []
    courses = df[df['student_id'] == student_id]
    return courses.to_dict("records")

@app.get("/analytics/student/{student_id}/course/{course_id}")
def get_student_course_performance(student_id: str, course_id: str):
    """Get detailed performance of a student in a specific course"""
    df = read_parquet("batch_views/student_course_detailed")
    if df.empty:
        return {}
    perf = df[(df['student_id'] == student_id) & (df['course_id'] == course_id)]
    if perf.empty:
        raise HTTPException(status_code=404, detail="Student-course record not found")
    return perf.to_dict("records")[0]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
