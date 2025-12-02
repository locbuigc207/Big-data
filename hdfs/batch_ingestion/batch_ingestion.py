"""
Batch Ingestion for Kubernetes Environment
Uploads CSV files to HDFS with date partitioning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchIngestionK8s:
    """Batch data ingestion for Kubernetes HDFS"""
    
    def __init__(self, 
                 hdfs_namenode="hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000",
                 local_data_path="/data"):
        """
        Initialize batch ingestion
        
        Args:
            hdfs_namenode: HDFS namenode URI in K8s
            local_data_path: Local path with CSV files
        """
        self.hdfs_namenode = hdfs_namenode
        self.local_data_path = local_data_path
        self.ingestion_date = datetime.now().strftime("%Y-%m-%d")
        
        # Initialize Spark with HDFS config
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-BatchIngestion-K8s") \
            .config("spark.master", "k8s://https://kubernetes.default.svc:443") \
            .config("spark.kubernetes.namespace", "bigdata") \
            .config("spark.hadoop.fs.defaultFS", hdfs_namenode) \
            .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.instances", "3") \
            .config("spark.sql.shuffle.partitions", "10") \
            .getOrCreate()
        
        logger.info(f"Spark session initialized: {self.spark.version}")
        logger.info(f"HDFS Namenode: {hdfs_namenode}")
        logger.info(f"Ingestion date: {self.ingestion_date}")
    
    def get_schema(self, table_name):
        """Define schemas for each table"""
        
        schemas = {
            'students': StructType([
                StructField("student_id", StringType(), False),
                StructField("full_name", StringType(), False),
                StructField("dob", StringType(), True),
                StructField("faculty", StringType(), False),
                StructField("email", StringType(), False),
                StructField("password", StringType(), True)
            ]),
            
            'teachers': StructType([
                StructField("teacher_id", StringType(), False),
                StructField("full_name", StringType(), False),
                StructField("specialty", StringType(), True),
                StructField("email", StringType(), False),
                StructField("password", StringType(), True)
            ]),
            
            'courses': StructType([
                StructField("course_id", StringType(), False),
                StructField("course_name", StringType(), False),
                StructField("credits", IntegerType(), False),
                StructField("teacher_id", StringType(), True)
            ]),
            
            'classes': StructType([
                StructField("class_id", StringType(), False),
                StructField("course_id", StringType(), False),
                StructField("semester", StringType(), False),
                StructField("year", StringType(), False),
                StructField("teacher_id", StringType(), False),
                StructField("capacity", IntegerType(), True)
            ]),
            
            'enrollments': StructType([
                StructField("student_id", StringType(), False),
                StructField("class_id", StringType(), False),
                StructField("semester", StringType(), False),
                StructField("enroll_date", StringType(), True)
            ]),
            
            'grades': StructType([
                StructField("student_id", StringType(), False),
                StructField("class_id", StringType(), False),
                StructField("semester", StringType(), False),
                StructField("midterm_score", DoubleType(), True),
                StructField("final_score", DoubleType(), True),
                StructField("weight_mid", DoubleType(), True),
                StructField("weight_final", DoubleType(), True),
                StructField("total_score", DoubleType(), True),
                StructField("passed", BooleanType(), True)
            ]),
            
            'sessions': StructType([
                StructField("session_id", StringType(), False),
                StructField("class_id", StringType(), False),
                StructField("date", StringType(), False)
            ]),
            
            'attendance': StructType([
                StructField("session_id", StringType(), False),
                StructField("student_id", StringType(), False),
                StructField("status", StringType(), False)
            ])
        }
        
        return schemas.get(table_name)
    
    def validate_data(self, df, table_name):
        """Validate data quality and return metrics"""
        
        logger.info(f"Validating {table_name}...")
        
        # Basic stats
        total_count = df.count()
        
        # Check for nulls
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                null_counts[col_name] = null_count
                logger.warning(f"{table_name}.{col_name}: {null_count} nulls")
        
        # Check duplicates on first column (usually ID)
        id_column = df.columns[0]
        duplicate_count = df.groupBy(id_column).count() \
            .filter(col("count") > 1).count()
        
        if duplicate_count > 0:
            logger.warning(f"{table_name}: {duplicate_count} duplicate IDs")
        
        metrics = {
            'table': table_name,
            'total_records': total_count,
            'null_counts': null_counts,
            'duplicate_ids': duplicate_count,
            'ingestion_date': self.ingestion_date
        }
        
        logger.info(f"✓ {table_name}: {total_count} records validated")
        
        return df, metrics
    
    def ingest_table(self, table_name):
        """
        Ingest a single table from CSV to HDFS
        
        Args:
            table_name: Name of the table (e.g., 'students')
        """
        csv_file = f"{self.local_data_path}/{table_name}.csv"
        
        if not os.path.exists(csv_file):
            logger.error(f"File not found: {csv_file}")
            return None
        
        logger.info(f"Processing {table_name} from {csv_file}")
        
        # Read CSV with schema
        schema = self.get_schema(table_name)
        df = self.spark.read \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .schema(schema) \
            .csv(csv_file)
        
        # Validate
        df_validated, metrics = self.validate_data(df, table_name)
        
        # Add metadata
        df_final = df_validated \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", lit(self.ingestion_date))
        
        # Write to HDFS with date partitioning
        hdfs_path = f"{self.hdfs_namenode}/raw/{table_name}/{self.ingestion_date}"
        
        try:
            df_final.write \
                .mode("overwrite") \
                .parquet(hdfs_path)
            
            logger.info(f"✓ Written to HDFS: {hdfs_path}")
            
            # Save quality metrics
            metrics_path = f"{self.hdfs_namenode}/quality_metrics/{table_name}/{self.ingestion_date}"
            metrics_df = self.spark.createDataFrame([metrics])
            metrics_df.write \
                .mode("overwrite") \
                .json(metrics_path)
            
            return df_final
            
        except Exception as e:
            logger.error(f"Failed to write {table_name}: {str(e)}")
            raise
    
    def ingest_all(self):
        """Ingest all tables"""
        
        tables = [
            'students', 'teachers', 'courses', 'classes',
            'enrollments', 'grades', 'sessions', 'attendance'
        ]
        
        results = {}
        success_count = 0
        
        logger.info("=" * 70)
        logger.info("BATCH INGESTION - Starting")
        logger.info("=" * 70)
        
        for table in tables:
            try:
                df = self.ingest_table(table)
                if df is not None:
                    results[table] = df
                    success_count += 1
            except Exception as e:
                logger.error(f"Failed to ingest {table}: {str(e)}")
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 70)
        
        for table, df in results.items():
            count = df.count()
            logger.info(f"{table:15s}: {count:10,d} records")
        
        logger.info("=" * 70)
        logger.info(f"✓ Completed: {success_count}/{len(tables)} tables ingested")
        logger.info("=" * 70)
        
        return results
    
    def create_hive_tables(self):
        """Create Hive external tables pointing to HDFS data"""
        
        logger.info("Creating Hive external tables...")
        
        tables = [
            'students', 'teachers', 'courses', 'classes',
            'enrollments', 'grades', 'sessions', 'attendance'
        ]
        
        for table in tables:
            try:
                hdfs_path = f"{self.hdfs_namenode}/raw/{table}"
                
                self.spark.sql(f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {table}
                    USING parquet
                    LOCATION '{hdfs_path}'
                """)
                
                logger.info(f"✓ Created external table: {table}")
                
            except Exception as e:
                logger.error(f"Failed to create table {table}: {str(e)}")
    
    def verify_ingestion(self):
        """Verify data was written correctly to HDFS"""
        
        logger.info("Verifying ingestion...")
        
        tables = [
            'students', 'teachers', 'courses', 'classes',
            'enrollments', 'grades', 'sessions', 'attendance'
        ]
        
        for table in tables:
            hdfs_path = f"{self.hdfs_namenode}/raw/{table}/{self.ingestion_date}"
            
            try:
                df = self.spark.read.parquet(hdfs_path)
                count = df.count()
                logger.info(f"✓ {table}: {count:,} records in HDFS")
            except Exception as e:
                logger.error(f"✗ {table}: Failed to read from HDFS - {str(e)}")
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main execution"""
    
    # Get config from environment (set in K8s ConfigMap)
    HDFS_NAMENODE = os.getenv(
        "HDFS_NAMENODE", 
        "hdfs://hdfs-namenode.bigdata.svc.cluster.local:9000"
    )
    DATA_PATH = os.getenv("DATA_PATH", "/data")
    
    try:
        ingestion = BatchIngestionK8s(
            hdfs_namenode=HDFS_NAMENODE,
            local_data_path=DATA_PATH
        )
        
        # Ingest all tables
        ingestion.ingest_all()
        
        # Create Hive tables
        ingestion.create_hive_tables()
        
        # Verify
        ingestion.verify_ingestion()
        
        logger.info("✓ Batch ingestion completed successfully")
        
    except Exception as e:
        logger.error(f"Batch ingestion failed: {str(e)}")
        sys.exit(1)
    finally:
        ingestion.cleanup()


if __name__ == "__main__":
    main()