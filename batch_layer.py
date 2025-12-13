"""
DEPRECATED: This file has been replaced by the new batch_layer/ directory structure.

The batch layer has been completely refactored with:
- 5 separate batch jobs (auth, assessment, video, course, profile_notification)
- 37 precomputed batch views
- Oozie workflow orchestration for parallel execution
- MinIO (S3-compatible) storage instead of HDFS
- Better organization and maintainability

NEW LOCATION: batch_layer/
├── jobs/                     # Individual batch processing jobs
├── oozie/                    # Oozie workflow definitions
├── config.py                 # Centralized configuration
├── run_batch_jobs.py         # Manual runner
└── README.md                 # Comprehensive documentation

TO RUN BATCH JOBS:
------------------

1. Run all jobs manually:
   python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views

2. Run specific job:
   python batch_layer/run_batch_jobs.py video s3a://bucket-0/master_dataset s3a://bucket-0/batch_views

3. Deploy to Oozie (production):
   cd batch_layer
   ./deploy_oozie.sh  (Linux/Mac)
   .\deploy_oozie.ps1 (Windows)
   
   Then submit coordinator:
   oozie job -oozie http://localhost:11000/oozie -config batch_layer/oozie/job.properties -run

For more information, see:
- batch_layer/README.md - Complete documentation
- batch_layer/QUICKSTART.md - Quick start guide
"""

import sys
print(__doc__)
print("\n⚠️  Please use the new batch layer structure in the batch_layer/ directory")
print("   Run: python batch_layer/run_batch_jobs.py s3a://bucket-0/master_dataset s3a://bucket-0/batch_views")
sys.exit(1)