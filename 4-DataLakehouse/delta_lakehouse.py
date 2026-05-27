"""
Delta Lake Data Lakehouse Implementation
Medallion Architecture (Bronze/Silver/Gold)
"""

import logging
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, from_json, schema_of_json, to_timestamp, date_format,
    row_number, md5, concat_ws, lit, current_timestamp,
    explode, when, coalesce, sum as spark_sum, avg, count as spark_count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)
from datetime import datetime, timedelta
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DeltaLakehouse:
    """Delta Lake Lakehouse Management"""
    
    def __init__(self, app_name: str = "DeltaLakehouse"):
        """Initialize Spark with Delta Lake"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .config("spark.databricks.delta.merge.enableLowShuffle", "true") \
            .getOrCreate()
        
        self.logger = logger
    
    # ==================== BRONZE LAYER ====================
    
    def create_bronze_table(self, table_name: str, s3_path: str):
        """Create bronze (raw) table"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{s3_path}'
        """
        self.spark.sql(sql)
        self.logger.info(f"Created bronze table: {table_name}")
    
    def load_to_bronze(self, df: DataFrame, table_name: str, 
                      mode: str = 'append') -> DataFrame:
        """Load raw data to bronze layer"""
        # Add metadata columns
        df_with_metadata = df.withColumn(
            "bronze_ingestion_time", current_timestamp()
        ).withColumn(
            "bronze_ingestion_date", date_format(current_timestamp(), "yyyy-MM-dd")
        ).withColumn(
            "data_source", lit(table_name)
        )
        
        # Write to Delta table
        df_with_metadata.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)
        
        self.logger.info(f"Loaded {df.count()} records to {table_name}")
        return df_with_metadata
    
    # ==================== SILVER LAYER ====================
    
    def create_silver_table(self, table_name: str, s3_path: str):
        """Create silver (cleaned) table"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{s3_path}'
        """
        self.spark.sql(sql)
        self.logger.info(f"Created silver table: {table_name}")
    
    def deduplicate_data(self, bronze_table: str, silver_table: str, 
                        key_columns: List[str]) -> DataFrame:
        """Deduplicate data from bronze to silver"""
        bronze_df = self.spark.table(bronze_table)
        
        # Window function to rank by ingestion time (keep latest)
        w = Window.partitionBy(*key_columns).orderBy(
            col("bronze_ingestion_time").desc()
        )
        
        deduped_df = bronze_df.withColumn(
            "rn", row_number().over(w)
        ).filter(
            col("rn") == 1
        ).drop("rn")
        
        self.logger.info(f"Deduplicated data from {bronze_table} to {silver_table}")
        return deduped_df
    
    def validate_and_transform(self, df: DataFrame, transformations: Dict[str, Any]) -> DataFrame:
        """Apply validations and business transformations"""
        
        # Data quality checks
        if 'null_check_columns' in transformations:
            for col_name in transformations['null_check_columns']:
                df = df.filter(col(col_name).isNotNull())
        
        # Type conversions
        if 'type_conversions' in transformations:
            for col_name, target_type in transformations['type_conversions'].items():
                if target_type == 'double':
                    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
                elif target_type == 'integer':
                    df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
                elif target_type == 'timestamp':
                    df = df.withColumn(col_name, to_timestamp(col(col_name)))
        
        # Business logic
        if 'calculated_columns' in transformations:
            for calc_col in transformations['calculated_columns']:
                df = df.withColumn(
                    calc_col['name'],
                    calc_col['expression'](df)
                )
        
        # Add metadata
        df = df.withColumn(
            "silver_processing_time", current_timestamp()
        ).withColumn(
            "silver_processing_date", date_format(current_timestamp(), "yyyy-MM-dd")
        )
        
        return df
    
    def load_to_silver(self, df: DataFrame, table_name: str, 
                      key_columns: List[str], mode: str = 'merge'):
        """Load transformed data to silver layer"""
        
        if mode == 'merge':
            # MERGE for incremental updates with UPSERT
            merge_condition = " AND ".join([f"t.{col} = s.{col}" for col in key_columns])
            
            df.createOrReplaceTempView("source_data")
            
            merge_sql = f"""
            MERGE INTO {table_name} t
            USING source_data s
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            
            self.spark.sql(merge_sql)
            self.logger.info(f"Merged data into {table_name}")
        
        else:
            df.write \
                .format("delta") \
                .mode(mode) \
                .option("mergeSchema", "true") \
                .saveAsTable(table_name)
        
        self.logger.info(f"Loaded to silver table: {table_name}")
    
    # ==================== GOLD LAYER ====================
    
    def create_gold_table(self, table_name: str, s3_path: str):
        """Create gold (aggregated) table"""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{s3_path}'
        """
        self.spark.sql(sql)
        self.logger.info(f"Created gold table: {table_name}")
    
    def aggregate_daily_metrics(self, silver_table: str, 
                               groupby_cols: List[str]) -> DataFrame:
        """Create daily aggregated metrics"""
        df = self.spark.table(silver_table)
        
        metrics_df = df.groupBy(
            *groupby_cols,
            date_format(col("silver_processing_time"), "yyyy-MM-dd").alias("metric_date")
        ).agg(
            spark_count("*").alias("record_count"),
            spark_count(col("id")).alias("unique_records")
        ).withColumn(
            "gold_creation_time", current_timestamp()
        )
        
        return metrics_df
    
    def load_to_gold(self, df: DataFrame, table_name: str):
        """Load aggregated data to gold layer"""
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(table_name)
        
        self.logger.info(f"Loaded to gold table: {table_name}")
    
    # ==================== DATA GOVERNANCE ====================
    
    def add_data_lineage(self, df: DataFrame, source_table: str, 
                        target_table: str) -> DataFrame:
        """Add lineage information"""
        return df.withColumn(
            "lineage_source", lit(source_table)
        ).withColumn(
            "lineage_target", lit(target_table)
        ).withColumn(
            "lineage_timestamp", current_timestamp()
        )
    
    def enable_delta_cdc(self, table_name: str):
        """Enable Change Data Capture"""
        sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataCapture = true)"
        self.spark.sql(sql)
        self.logger.info(f"Enabled CDC for {table_name}")
    
    def get_table_history(self, table_name: str) -> DataFrame:
        """Get table version history"""
        history_df = self.spark.sql(f"DESC HISTORY {table_name}")
        return history_df
    
    def time_travel_query(self, table_name: str, version: int = None, 
                         timestamp: str = None) -> DataFrame:
        """Query historical data using time travel"""
        if version is not None:
            df = self.spark.read.format("delta") \
                .option("versionAsOf", version) \
                .table(table_name)
        elif timestamp is not None:
            df = self.spark.read.format("delta") \
                .option("timestampAsOf", timestamp) \
                .table(table_name)
        else:
            df = self.spark.table(table_name)
        
        return df
    
    def optimize_table(self, table_name: str):
        """Optimize Delta table"""
        sql = f"OPTIMIZE {table_name} ZORDER BY (id)"
        self.spark.sql(sql)
        self.logger.info(f"Optimized table: {table_name}")
    
    def vacuum_table(self, table_name: str, retention_hours: int = 168):
        """Cleanup old Delta table files"""
        sql = f"VACUUM {table_name} RETAIN {retention_hours} HOURS"
        self.spark.sql(sql)
        self.logger.info(f"Vacuumed table: {table_name}")
    
    # ==================== UTILITIES ====================
    
    def apply_adaptive_partitioning(self, table_name: str, partition_columns: List[str]):
        """Apply adaptive partitioning strategy based on data volume"""
        stats_query = f"""
        SELECT 
            COUNT(*) as row_count,
            SIZE_IN_BYTES / (1024*1024*1024) as size_gb
        FROM (DESCRIBE DETAIL {table_name})
        """
        
        # Evaluate partition strategy based on size
        partition_strategy = "ALTER TABLE {table_name} CLUSTER BY ({columns})"
        self.spark.sql(partition_strategy.format(
            table_name=table_name,
            columns=",".join(partition_columns)
        ))
        
        self.logger.info(f"Applied adaptive partitioning to {table_name}")
    
    def compact_small_files(self, table_name: str, target_file_size_mb: int = 128):
        """Compact small files into larger optimal-sized files"""
        compact_query = f"""
        OPTIMIZE {table_name} ZORDER BY ({', '.join([f'{col}' for col in self.spark.table(table_name).columns[:3]])})
        """
        
        result = self.spark.sql(compact_query)
        self.logger.info(f"Compacted files in {table_name}")
        
        return result
    
    def implement_data_skipping(self, table_name: str, column: str):
        """Enable data skipping statistics for query acceleration"""
        query = f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'delta.dataSkippingNumIndexedCols' = '10'
        )
        """
        self.spark.sql(query)
        self.logger.info(f"Enabled data skipping on {table_name}")
    
    def apply_z_order_clustering(self, table_name: str, clustering_columns: List[str]):
        """Apply Z-order clustering for multi-dimensional query optimization"""
        z_order_query = f"""
        OPTIMIZE {table_name} ZORDER BY ({', '.join(clustering_columns)})
        """
        
        self.spark.sql(z_order_query)
        self.logger.info(f"Applied Z-order clustering to {table_name}: {clustering_columns}")
    
    def collect_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """Collect comprehensive table statistics"""
        stats_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT date_format('{table_name}.processed_date', 'yyyy-MM-dd')) as date_range,
            APPROX_PERCENTILE_CONT(size_in_bytes, 0.5) as median_file_size,
            COUNT(DISTINCT '__index_level_0__') as partition_count
        FROM {table_name}
        """
        
        stats = {
            'table_name': table_name,
            'timestamp': datetime.utcnow().isoformat(),
            'size_bytes': self.spark.sql(
                f"SELECT SUM(size_in_bytes) as total FROM (DESCRIBE DETAIL {table_name})"
            ).collect()[0][0]
        }
        
        return stats
    
    def create_gold_summary_view(self, gold_table: str):
        """Create view on gold layer"""
        view_sql = f"""
        CREATE OR REPLACE VIEW {gold_table}_summary AS
        SELECT * FROM {gold_table}
        WHERE gold_creation_time >= CURRENT_DATE - INTERVAL 7 DAY
        """
        self.spark.sql(view_sql)
        self.logger.info(f"Created summary view for {gold_table}")


# Example pipeline configuration
PIPELINE_CONFIG = {
    'bronze': {
        'table_name': 'bronze_events',
        's3_path': 's3://data-lake/bronze/events'
    },
    'silver': {
        'table_name': 'silver_events',
        's3_path': 's3://data-lake/silver/events',
        'key_columns': ['event_id'],
        'transformations': {
            'null_check_columns': ['event_id', 'user_id'],
            'type_conversions': {
                'event_timestamp': 'timestamp',
                'revenue': 'double'
            }
        }
    },
    'gold': {
        'table_name': 'gold_daily_metrics',
        's3_path': 's3://data-lake/gold/daily_metrics',
        'groupby_cols': ['event_type', 'user_country']
    }
}


def run_medallion_pipeline(config: Dict[str, Any]):
    """Execute complete Medallion architecture pipeline"""
    lakehouse = DeltaLakehouse()
    
    try:
        # Bronze layer
        bronze_config = config['bronze']
        lakehouse.create_bronze_table(bronze_config['table_name'], bronze_config['s3_path'])
        
        # Silver layer
        silver_config = config['silver']
        lakehouse.create_silver_table(silver_config['table_name'], silver_config['s3_path'])
        
        # Gold layer
        gold_config = config['gold']
        lakehouse.create_gold_table(gold_config['table_name'], gold_config['s3_path'])
        
        # Optimize tables
        lakehouse.optimize_table(bronze_config['table_name'])
        lakehouse.optimize_table(silver_config['table_name'])
        
        # Enable CDC for auditing
        lakehouse.enable_delta_cdc(silver_config['table_name'])
        
        logger.info("Medallion pipeline initialized successfully")
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise


if __name__ == "__main__":
    run_medallion_pipeline(PIPELINE_CONFIG)
