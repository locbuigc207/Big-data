"""
Master Batch Job Runner
Orchestrates all batch processing jobs - can be run manually or via Oozie
"""

from pyspark.sql import SparkSession
import sys
import os
from datetime import datetime

# Import all batch jobs
sys.path.append(os.path.join(os.path.dirname(__file__), 'jobs'))

def create_spark_session():
    """Initialize Spark Session with MinIO configuration"""
    return SparkSession.builder \
        .appName("Master_Batch_Runner") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def run_batch_job(job_name, input_path, output_path):
    """Run a specific batch job"""
    print(f"\n{'='*80}")
    print(f"Starting Batch Job: {job_name}")
    print(f"Input Path: {input_path}")
    print(f"Output Path: {output_path}")
    print(f"Timestamp: {datetime.now()}")
    print(f"{'='*80}\n")
    
    try:
        if job_name == "auth":
            from auth_batch_job import main as auth_main
            auth_main(input_path, output_path)
        elif job_name == "assessment":
            from assessment_batch_job import main as assessment_main
            assessment_main(input_path, output_path)
        elif job_name == "video":
            from video_batch_job import main as video_main
            video_main(input_path, output_path)
        elif job_name == "course":
            from course_batch_job import main as course_main
            course_main(input_path, output_path)
        elif job_name == "profile_notification":
            from profile_notification_batch_job import main as profile_notif_main
            profile_notif_main(input_path, output_path)
        else:
            print(f"ERROR: Unknown job name '{job_name}'")
            return False
        
        print(f"\n{'='*80}")
        print(f"✅ Batch Job '{job_name}' completed successfully!")
        print(f"{'='*80}\n")
        return True
        
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ Batch Job '{job_name}' FAILED!")
        print(f"Error: {str(e)}")
        print(f"{'='*80}\n")
        return False

def run_all_jobs(input_path, output_path):
    """Run all batch jobs sequentially"""
    jobs = ["auth", "assessment", "video", "course", "profile_notification"]
    results = {}
    
    print(f"\n{'#'*80}")
    print(f"# MASTER BATCH JOB RUNNER")
    print(f"# Running {len(jobs)} batch jobs")
    print(f"# Start Time: {datetime.now()}")
    print(f"{'#'*80}\n")
    
    start_time = datetime.now()
    
    for job in jobs:
        success = run_batch_job(job, input_path, output_path)
        results[job] = "✅ SUCCESS" if success else "❌ FAILED"
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Print summary
    print(f"\n{'#'*80}")
    print(f"# BATCH JOB EXECUTION SUMMARY")
    print(f"{'#'*80}")
    print(f"Start Time: {start_time}")
    print(f"End Time: {end_time}")
    print(f"Total Duration: {duration}")
    print(f"\nJob Results:")
    for job, result in results.items():
        print(f"  {job:25s} : {result}")
    print(f"{'#'*80}\n")
    
    # Check if all succeeded
    all_success = all("SUCCESS" in r for r in results.values())
    if all_success:
        print("🎉 All batch jobs completed successfully!")
    else:
        print("⚠️  Some batch jobs failed. Check logs above.")
        sys.exit(1)

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Run all jobs: python run_batch_jobs.py <input_path> <output_path>")
        print("  Run specific job: python run_batch_jobs.py <job_name> <input_path> <output_path>")
        print("\nAvailable jobs: auth, assessment, video, course, profile_notification")
        print("\nExample:")
        print("  python run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        print("  python run_batch_jobs.py video s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
        sys.exit(1)
    
    # Check if first arg is a job name or path
    if sys.argv[1] in ["auth", "assessment", "video", "course", "profile_notification"]:
        # Run specific job
        if len(sys.argv) != 4:
            print("ERROR: When running a specific job, provide: job_name input_path output_path")
            sys.exit(1)
        job_name = sys.argv[1]
        input_path = sys.argv[2]
        output_path = sys.argv[3]
        run_batch_job(job_name, input_path, output_path)
    else:
        # Run all jobs
        if len(sys.argv) != 3:
            print("ERROR: When running all jobs, provide: input_path output_path")
            sys.exit(1)
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        run_all_jobs(input_path, output_path)

if __name__ == "__main__":
    main()
