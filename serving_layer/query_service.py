from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uvicorn
from datetime import datetime
import logging

from view_merger import ViewMerger
from config import serving_config

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Lambda Architecture Serving Layer",
    description="Unified query interface for student learning analytics",
    version="1.0.0"
)

# Initialize ViewMerger (singleton)
view_merger = None


def get_view_merger() -> ViewMerger:
    """Get or create ViewMerger instance"""
    global view_merger
    if view_merger is None:
        view_merger = ViewMerger()
    return view_merger


# Response Models
class HealthResponse(BaseModel):
    status: str
    timestamp: str
    spark_active: bool


class QueryResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]]
    count: int
    query_time_ms: float
    source: str


# API Endpoints

@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API information"""
    return {
        "service": "Lambda Architecture Serving Layer",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "auth": "/api/v1/auth/*",
            "video": "/api/v1/video/*",
            "assessment": "/api/v1/assessment/*",
            "course": "/api/v1/course/*",
            "student": "/api/v1/student/*"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    merger = get_view_merger()
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        spark_active=merger.spark.sparkContext._jsc is not None
    )


@app.get("/api/v1/auth/active-users", tags=["Authentication"])
async def get_active_users(
    minutes: int = Query(60, description="Time window in minutes")
):
    """
    Get active users in the specified time window
    Merges batch daily stats with real-time minute-by-minute data
    """
    try:
        start_time = datetime.now()
        merger = get_view_merger()
        
        df = merger.get_active_users(minutes=minutes)
        
        if df is None:
            raise HTTPException(status_code=404, detail="No data available")
        
        # Convert to JSON
        data = df.limit(100).toPandas().to_dict(orient='records')
        
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return QueryResponse(
            success=True,
            data=data,
            count=len(data),
            query_time_ms=query_time,
            source="batch+realtime"
        )
    
    except Exception as e:
        logger.error(f"Error in get_active_users: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/auth/login-rate", tags=["Authentication"])
async def get_login_rate(minutes: int = Query(60, description="Time window")):
    """Get login rate metrics (success/failure)"""
    try:
        start_time = datetime.now()
        merger = get_view_merger()
        
        # Read realtime login rate
        df = merger.read_realtime_view("auth_realtime_login_rate", window_minutes=minutes)
        
        if df is None:
            raise HTTPException(status_code=404, detail="No login rate data")
        
        data = df.limit(100).toPandas().to_dict(orient='records')
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return QueryResponse(
            success=True,
            data=data,
            count=len(data),
            query_time_ms=query_time,
            source="realtime"
        )
    
    except Exception as e:
        logger.error(f"Error in get_login_rate: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/video/engagement/{video_id}", tags=["Video"])
async def get_video_engagement(video_id: str):
    """
    Get comprehensive video engagement metrics
    Combines historical watch time with real-time viewer data
    """
    try:
        start_time = datetime.now()
        merger = get_view_merger()
        
        df = merger.get_video_engagement(video_id=video_id)
        
        if df is None or df.count() == 0:
            raise HTTPException(status_code=404, detail=f"No data for video {video_id}")
        
        data = df.toPandas().to_dict(orient='records')
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return QueryResponse(
            success=True,
            data=data,
            count=len(data),
            query_time_ms=query_time,
            source="batch+realtime"
        )
    
    except Exception as e:
        logger.error(f"Error in get_video_engagement: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/video/popular", tags=["Video"])
async def get_popular_videos(
    limit: int = Query(10, description="Number of videos"),
    minutes: int = Query(60, description="Time window")
):
    """Get most popular videos in real-time"""
    try:
        start_time = datetime.now()
        merger = get_view_merger()
        
        df = merger.read_realtime_view("video_realtime_popular_videos", window_minutes=minutes)
        
        if df is None:
            raise HTTPException(status_code=404, detail="No video popularity data")
        
        # Sort by unique viewers and limit
        from pyspark.sql.functions import col
        df_sorted = df.orderBy(col("unique_viewers").desc()).limit(limit)
        
        data = df_sorted.toPandas().to_dict(orient='records')
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return QueryResponse(
            success=True,
            data=data,
            count=len(data),
            query_time_ms=query_time,
            source="realtime"
        )
    
    except Exception as e:
        logger.error(f"Error in get_popular_videos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/student/{student_id}/performance", tags=["Student"])
async def get_student_performance(student_id: str):
    """
    Get comprehensive student performance metrics
    Includes grades, quiz scores, and recent submission activity
    """
    try:
        start_time = datetime.now()
        merger = get_view_merger()
        
        result = merger.get_student_performance(student_id=student_id)
        
        # Convert each DataFrame to dict
        response_data = {}
        for key, df in result.items():
            if df is not None and df.count() > 0:
                response_data[key] = df.toPandas().to_dict(orient='records')
            else:
                response_data[key] = []
        
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return {
            "success": True,
            "student_id": student_id,
            "data": response_data,
            "query_time_ms": query_time,
            "source": "batch+realtime"
        }
    
    except Exception as e:
        logger.error(f"Error in get_student_performance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/course/{course_id}/metrics", tags=["Course"])
async def get_course_metrics(course_id: str):
    """
    Get comprehensive course metrics
    Includes enrollment, material access, and real-time activity
    """
    try:
        start_time = datetime.now()
        merger = get_view_merger()
        
        result = merger.get_course_metrics(course_id=course_id)
        
        # Convert to dict
        response_data = {}
        for key, df in result.items():
            if df is not None and df.count() > 0:
                response_data[key] = df.toPandas().to_dict(orient='records')
            else:
                response_data[key] = []
        
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return {
            "success": True,
            "course_id": course_id,
            "data": response_data,
            "query_time_ms": query_time,
            "source": "batch+realtime"
        }
    
    except Exception as e:
        logger.error(f"Error in get_course_metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/assessment/submissions", tags=["Assessment"])
async def get_recent_submissions(minutes: int = Query(60, description="Time window")):
    """Get recent assignment submissions"""
    try:
        start_time = datetime.now()
        merger = get_view_merger()
        
        df = merger.read_realtime_view("assessment_realtime_submissions_1min", window_minutes=minutes)
        
        if df is None:
            raise HTTPException(status_code=404, detail="No submission data")
        
        data = df.limit(100).toPandas().to_dict(orient='records')
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return QueryResponse(
            success=True,
            data=data,
            count=len(data),
            query_time_ms=query_time,
            source="realtime"
        )
    
    except Exception as e:
        logger.error(f"Error in get_recent_submissions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/assessment/grading-queue", tags=["Assessment"])
async def get_grading_queue(minutes: int = Query(60, description="Time window")):
    """Get pending grading queue"""
    try:
        start_time = datetime.now()
        merger = get_view_merger()
        
        df = merger.read_realtime_view("assessment_realtime_grading_queue", window_minutes=minutes)
        
        if df is None:
            raise HTTPException(status_code=404, detail="No grading queue data")
        
        data = df.limit(50).toPandas().to_dict(orient='records')
        query_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return QueryResponse(
            success=True,
            data=data,
            count=len(data),
            query_time_ms=query_time,
            source="realtime"
        )
    
    except Exception as e:
        logger.error(f"Error in get_grading_queue: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/cache/clear", tags=["Admin"])
async def clear_cache():
    """Clear query cache (admin endpoint)"""
    try:
        merger = get_view_merger()
        merger.clear_cache()
        return {"success": True, "message": "Cache cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/views/list", tags=["Admin"])
async def list_available_views():
    """List all available batch and realtime views"""
    return {
        "success": True,
        "views": serving_config["views"]
    }


# Startup/Shutdown Events

@app.on_event("startup")
async def startup_event():
    """Initialize resources on startup"""
    logger.info("Starting Serving Layer API...")
    logger.info(f"API listening on {serving_config['api']['host']}:{serving_config['api']['port']}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on shutdown"""
    global view_merger
    if view_merger:
        view_merger.stop()
    logger.info("Serving Layer API stopped")


# Main execution
if __name__ == "__main__":
    uvicorn.run(
        "query_service:app",
        host=serving_config["api"]["host"],
        port=serving_config["api"]["port"],
        reload=serving_config["api"]["reload"],
        log_level=serving_config["api"]["log_level"]
    )