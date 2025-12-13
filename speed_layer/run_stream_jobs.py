"""
Unified Stream Processor Runner
Manages and runs all speed layer streaming jobs

This script allows you to:
1. Run all streaming jobs simultaneously
2. Run specific jobs by name
3. Monitor job health and performance

Usage:
    # Run all streaming jobs
    python run_stream_jobs.py
    
    # Run specific job
    python run_stream_jobs.py auth
    
    # Run multiple specific jobs
    python run_stream_jobs.py auth assessment video
"""

import sys
import os
import subprocess
import time
from datetime import datetime

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import speed_config


# Available streaming jobs
AVAILABLE_JOBS = {
    "auth": {
        "script": "jobs/auth_stream_job.py",
        "description": "Authentication events streaming",
        "views": speed_config["realtime_views"]["auth"]
    },
    "assessment": {
        "script": "jobs/assessment_stream_job.py",
        "description": "Assessment/assignment events streaming",
        "views": speed_config["realtime_views"]["assessment"]
    },
    "video": {
        "script": "jobs/video_stream_job.py",
        "description": "Video interaction events streaming",
        "views": speed_config["realtime_views"]["video"]
    },
    "course": {
        "script": "jobs/course_stream_job.py",
        "description": "Course interaction events streaming",
        "views": speed_config["realtime_views"]["course"]
    },
    "profile_notification": {
        "script": "jobs/profile_notification_stream_job.py",
        "description": "Profile and notification events streaming",
        "views": speed_config["realtime_views"]["profile_notification"]
    }
}


def print_header():
    """Print header information"""
    print("=" * 80)
    print("Speed Layer - Unified Stream Processor Runner")
    print("Real-Time Stream Processing for Lambda Architecture")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Kafka Brokers: {speed_config['kafka']['bootstrap_servers']}")
    print(f"Output Path: {speed_config['paths']['output']}")
    print(f"Checkpoint Path: {speed_config['paths']['checkpoint']}")
    print("=" * 80)
    print()


def print_job_info(job_name, job_config):
    """Print information about a specific job"""
    print(f"\n{job_name.upper()} Streaming Job")
    print("-" * 60)
    print(f"Description: {job_config['description']}")
    print(f"Script: {job_config['script']}")
    print(f"Real-time Views ({len(job_config['views'])}):")
    for view in job_config['views']:
        print(f"  - {view}")
    print("-" * 60)


def run_job_subprocess(job_name, job_config):
    """
    Run a streaming job as a subprocess
    Returns the subprocess object
    """
    script_path = os.path.join(os.path.dirname(__file__), job_config['script'])
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting {job_name} streaming job...")
    
    # Use spark-submit for production or python for development
    # Uncomment the appropriate command based on your environment
    
    # For development (direct Python execution)
    cmd = [sys.executable, script_path]
    
    # For production (Spark submit)
    # cmd = [
    #     "spark-submit",
    #     "--master", "yarn",  # or "local[*]" for local mode
    #     "--deploy-mode", "client",
    #     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    #                   "org.apache.hadoop:hadoop-aws:3.3.4,"
    #                   "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    #     "--conf", f"spark.executor.memory={speed_config['spark']['executor_memory']}",
    #     "--conf", f"spark.executor.cores={speed_config['spark']['executor_cores']}",
    #     script_path
    # ]
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {job_name} job started with PID: {process.pid}")
        return process
    except Exception as e:
        print(f"[ERROR] Failed to start {job_name} job: {str(e)}")
        return None


def run_job_spark_submit(job_name, job_config):
    """
    Run a streaming job using spark-submit
    This is the recommended way for production
    """
    script_path = os.path.join(os.path.dirname(__file__), job_config['script'])
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Submitting {job_name} to Spark...")
    
    cmd = [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--name", f"SpeedLayer_{job_name}",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                      "org.apache.hadoop:hadoop-aws:3.3.4,"
                      "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "--conf", f"spark.executor.memory={speed_config['spark']['executor_memory']}",
        "--conf", f"spark.executor.cores={speed_config['spark']['executor_cores']}",
        "--conf", f"spark.executor.instances={speed_config['spark']['num_executors']}",
        "--conf", f"spark.driver.memory={speed_config['spark']['driver_memory']}",
        "--conf", f"spark.sql.shuffle.partitions={speed_config['spark']['shuffle_partitions']}",
        script_path
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {job_name} job submitted successfully")
        else:
            print(f"[ERROR] {job_name} job submission failed:")
            print(result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"[ERROR] Failed to submit {job_name} job: {str(e)}")
        return False


def monitor_jobs(processes):
    """
    Monitor running jobs
    Print status updates and handle termination
    """
    print("\n" + "=" * 80)
    print("Monitoring streaming jobs (Press Ctrl+C to stop all jobs)")
    print("=" * 80)
    
    try:
        while True:
            time.sleep(10)  # Check every 10 seconds
            
            running_count = 0
            for job_name, process in processes.items():
                if process and process.poll() is None:
                    running_count += 1
                elif process and process.poll() is not None:
                    print(f"\n[WARNING] {job_name} job terminated with code {process.poll()}")
                    # Optionally restart failed jobs
                    # processes[job_name] = run_job_subprocess(job_name, AVAILABLE_JOBS[job_name])
            
            if running_count == 0:
                print("\n[INFO] All jobs have terminated")
                break
            
            timestamp = datetime.now().strftime('%H:%M:%S')
            print(f"[{timestamp}] Status: {running_count}/{len(processes)} jobs running")
    
    except KeyboardInterrupt:
        print("\n\n[INFO] Received interrupt signal, stopping all jobs...")
        for job_name, process in processes.items():
            if process and process.poll() is None:
                print(f"[INFO] Stopping {job_name} job (PID: {process.pid})...")
                process.terminate()
                try:
                    process.wait(timeout=30)
                    print(f"[INFO] {job_name} job stopped")
                except subprocess.TimeoutExpired:
                    print(f"[WARNING] Force killing {job_name} job...")
                    process.kill()
        print("\n[INFO] All jobs stopped")


def list_available_jobs():
    """List all available streaming jobs"""
    print("\nAvailable Streaming Jobs:")
    print("=" * 80)
    for job_name, job_config in AVAILABLE_JOBS.items():
        print(f"\n{job_name}")
        print(f"  Description: {job_config['description']}")
        print(f"  Views: {len(job_config['views'])}")
    print("\n" + "=" * 80)


def main():
    """Main execution function"""
    print_header()
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--list":
            list_available_jobs()
            return
        
        # Run specific jobs
        job_names = sys.argv[1:]
        jobs_to_run = {}
        
        for job_name in job_names:
            if job_name in AVAILABLE_JOBS:
                jobs_to_run[job_name] = AVAILABLE_JOBS[job_name]
            else:
                print(f"[WARNING] Unknown job: {job_name}, skipping...")
        
        if not jobs_to_run:
            print("[ERROR] No valid jobs specified")
            list_available_jobs()
            return
    else:
        # Run all jobs
        jobs_to_run = AVAILABLE_JOBS
    
    # Print job information
    print(f"\nStarting {len(jobs_to_run)} streaming job(s):")
    for job_name, job_config in jobs_to_run.items():
        print_job_info(job_name, job_config)
    
    print("\n" + "=" * 80)
    print("Starting jobs...")
    print("=" * 80)
    
    # Start all jobs
    processes = {}
    for job_name, job_config in jobs_to_run.items():
        process = run_job_subprocess(job_name, job_config)
        if process:
            processes[job_name] = process
        time.sleep(2)  # Stagger job starts
    
    if not processes:
        print("\n[ERROR] No jobs were started successfully")
        return
    
    print(f"\n[INFO] Successfully started {len(processes)} job(s)")
    
    # Monitor jobs
    monitor_jobs(processes)
    
    print("\n" + "=" * 80)
    print("Stream processing session ended")
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


if __name__ == "__main__":
    main()
