"""
Batch Layer - Data Ingestion Module
Loads CSV/JSON data into HDFS and performs initial validation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchDataIngestion:
    """Handles batch data ingestion from CSV/JSON to HDFS"""
    
    def __init__(self, hdfs_uri="hdfs://namenode:9000", local_data_path="./data"):
        """
        Initialize batch ingestion
        
        Args:
            hdfs_uri: HDFS namenode URI
            local_data_path: Local path containing CSV/JSON files
        """
        self.hdfs_uri = hdfs_uri
        self.local_data_path = local_data_path
        
        # Initialize Spark session with HDFS support
        self.spark = SparkSession.builder \
            .appName("EduAnalytics-BatchIngestion") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        logger.info(f"Spark session initialized: {self.spark.version}")
        logger.info(f"HDFS URI: {hdfs_uri}")
        
    def define_schemas(self):
        """Define schemas for all data sources"""
        
        self.schemas = {
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
        
        logger.info("Data schemas defined")
    
    def validate_data(self, df, table_name):
        """
        Validate data quality
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table
            
        Returns:
            Validated DataFrame with quality metrics
        """
        logger.info(f"Validating {table_name}...")
        
        # Count records
        total_count = df.count()
        
        # Check for nulls in key columns
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                null_counts[col_name] = null_count
                logger.warning(f"{table_name}.{col_name}: {null_count} null values")
        
        # Check for duplicates (based on first column - usually ID)
        id_column = df.columns[0]
        duplicate_count = df.groupBy(id_column).count() \
            .filter(col("count") > 1).count()
        
        if duplicate_count > 0:
            logger.warning(f"{table_name}: {duplicate_count} duplicate IDs found")
        
        # Quality metrics
        quality_metrics = {
            'table': table_name,
            'total_records': total_count,
            'null_counts': null_counts,
            'duplicate_ids': duplicate_count,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"{table_name} validation complete: {total_count} records")
        
        return df, quality_metrics
    
    def ingest_csv_to_hdfs(self, table_name, csv_path):
        """
        Ingest CSV file to HDFS with validation
        
        Args:
            table_name: Name of the table
            csv_path: Path to CSV file
            
        Returns:
            Validated DataFrame
        """
        logger.info(f"Ingesting {table_name} from {csv_path}")
        
        try:
            # Read CSV with schema
            df = self.spark.read \
                .option("header", "true") \
                .option("encoding", "UTF-8") \
                .option("inferSchema", "false") \
                .schema(self.schemas[table_name]) \
                .csv(csv_path)
            
            # Validate data
            df_validated, metrics = self.validate_data(df, table_name)
            
            # Add ingestion metadata
            df_final = df_validated \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit(csv_path))
            
            # Write to HDFS in parquet format (partitioned by semester if applicable)
            hdfs_path = f"{self.hdfs_uri}/edu-analytics/raw/{table_name}"
            
            if 'semester' in df_final.columns:
                df_final.write \
                    .mode("overwrite") \
                    .partitionBy("semester") \
                    .parquet(hdfs_path)
                logger.info(f"Written to HDFS (partitioned by semester): {hdfs_path}")
            else:
                df_final.write \
                    .mode("overwrite") \
                    .parquet(hdfs_path)
                logger.info(f"Written to HDFS: {hdfs_path}")
            
            # Log quality metrics to HDFS
            metrics_path = f"{self.hdfs_uri}/edu-analytics/quality_metrics/{table_name}"
            metrics_df = self.spark.createDataFrame([metrics])
            metrics_df.write \
                .mode("append") \
                .json(metrics_path)
            
            return df_final
            
        except Exception as e:
            logger.error(f"Error ingesting {table_name}: {str(e)}")
            raise
    
    def ingest_all_tables(self):
        """Ingest all CSV files to HDFS"""
        
        self.define_schemas()
        
        tables = [
            'students', 'teachers', 'courses', 'classes',
            'enrollments', 'grades', 'sessions', 'attendance'
        ]
        
        ingested_data = {}
        
        for table in tables:
            csv_path = f"{self.local_data_path}/{table}.csv"
            
            if os.path.exists(csv_path):
                try:
                    df = self.ingest_csv_to_hdfs(table, csv_path)
                    ingested_data[table] = df
                    logger.info(f"✓ {table} ingested successfully")
                except Exception as e:
                    logger.error(f"✗ Failed to ingest {table}: {str(e)}")
            else:
                logger.warning(f"CSV file not found: {csv_path}")
        
        logger.info(f"Batch ingestion complete: {len(ingested_data)}/{len(tables)} tables")
        
        return ingested_data
    
    def read_from_hdfs(self, table_name):
        """
        Read data from HDFS
        
        Args:
            table_name: Name of the table to read
            
        Returns:
            DataFrame
        """
        hdfs_path = f"{self.hdfs_uri}/edu-analytics/raw/{table_name}"
        
        try:
            df = self.spark.read.parquet(hdfs_path)
            logger.info(f"Read {df.count()} records from {hdfs_path}")
            return df
        except Exception as e:
            logger.error(f"Error reading {table_name} from HDFS: {str(e)}")
            raise
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main function for batch ingestion"""
    
    # Configuration
    HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:9000")
    DATA_PATH = os.getenv("DATA_PATH", "./data")
    
    logger.info("=" * 70)
    logger.info("BATCH DATA INGESTION - Starting")
    logger.info("=" * 70)
    
    try:
        # Initialize ingestion
        ingestion = BatchDataIngestion(
            hdfs_uri=HDFS_URI,
            local_data_path=DATA_PATH
        )
        
        # Ingest all tables
        ingested_data = ingestion.ingest_all_tables()
        
        # Show summary
        logger.info("\n" + "=" * 70)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 70)
        
        for table_name, df in ingested_data.items():
            count = df.count()
            columns = len(df.columns)
            logger.info(f"{table_name:15s}: {count:8,d} rows, {columns:3d} columns")
        
        logger.info("=" * 70)
        logger.info("✓ Batch ingestion completed successfully")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Batch ingestion failed: {str(e)}")
        raise
    finally:
        ingestion.cleanup()


if __name__ == "__main__":
    main()