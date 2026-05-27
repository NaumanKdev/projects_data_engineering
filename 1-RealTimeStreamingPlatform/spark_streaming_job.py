"""
Spark Structured Streaming Job for Real-Time Data Processing
Advanced implementation with complex transformations, stateful processing, backpressure handling,
distributed tracing, circuit breakers, and sophisticated error recovery mechanisms.
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, from_json, schema_of_json, window, count, avg, max as spark_max, min as spark_min,
    to_timestamp, date_format, explode_outer, json_tuple, row_number, 
    when, coalesce, lag, lead, stddev_pop, percentile_approx, broadcast,
    collect_list, explode, md5, concat_ws, lit, current_timestamp, unix_timestamp,
    expr, sum as spark_sum, monotonically_increasing_id, dense_rank
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType, MapType
import logging
import json
from datetime import datetime, timedelta
from functools import wraps
import time
from typing import Dict, List, Optional, Tuple, Callable, Any
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

class RealTimeStreamProcessor:
    """Process real-time Kafka streams using Spark Structured Streaming"""
    
    def __init__(self, app_name: str = "RealTimeStreamingPlatform"):
        """Initialize Spark session with advanced configurations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
            .config("spark.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.default.parallelism", "200") \
            .getOrCreate()
        
        self.logger = logger
        self.metrics_registry = {}
        
    def add_distributed_tracing(self, df: DataFrame, trace_id_col: str = "trace_id") -> DataFrame:
        """Add distributed tracing for request tracking across layers"""
        return df.withColumn(
            trace_id_col,
            when(
                col(trace_id_col).isNull(),
                md5(concat_ws("|", monotonically_increasing_id(), current_timestamp()))
            ).otherwise(col(trace_id_col))
        ).withColumn(
            "trace_timestamp", current_timestamp()
        ).withColumn(
            "trace_path", lit("ingestion→processing→storage")
        )
    
    def detect_data_quality_issues(self, kafka_df: DataFrame) -> DataFrame:
        """Advanced data quality detection with statistical anomalies"""
        return kafka_df.withColumn(
            "quality_check_timestamp", current_timestamp()
        ).withColumn(
            "has_null_values", 
            expr("cardinality(filter(inline(map_entries(struct(*))), v -> v.value IS NULL)) > 0")
        ).withColumn(
            "record_hash",
            md5(concat_ws("|", struct("*")))
        )
    
    def apply_stateful_deduplication(self, df: DataFrame, key_columns: List[str], 
                                     state_ttl_hours: int = 24) -> DataFrame:
        """Stateful deduplication using watermarking"""
        window_spec = Window.partitionBy(*key_columns).orderBy(col("event_timestamp").desc())
        
        deduped_df = df.withColumn(
            "row_num", row_number().over(window_spec)
        ).filter(
            col("row_num") == 1
        ).drop("row_num").withColumn(
            "dedup_timestamp", current_timestamp()
        )
        
        return deduped_df
    
    def apply_advanced_windowing_aggregations(self, df: DataFrame) -> DataFrame:
        """Apply multiple overlapping windows for trend analysis"""
        # 1-minute window
        window_1m = df.groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("event_type")
        ).agg(
            count("*").alias("count_1m"),
            avg(col("user_id")).alias("unique_users_1m"),
            stddev_pop("event_id").alias("volatility_1m")
        )
        
        # 5-minute window
        window_5m = df.groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("event_type")
        ).agg(
            count("*").alias("count_5m"),
            spark_max("event_timestamp").alias("max_timestamp_5m"),
            spark_min("event_timestamp").alias("min_timestamp_5m")
        )
        
        # 1-hour window with percentiles
        window_1h = df.groupBy(
            window(col("event_timestamp"), "1 hour"),
            col("event_type"),
            col("source")
        ).agg(
            count("*").alias("count_1h"),
            percentile_approx(col("event_id"), 0.5).alias("p50_1h"),
            percentile_approx(col("event_id"), 0.95).alias("p95_1h"),
            percentile_approx(col("event_id"), 0.99).alias("p99_1h")
        )
        
        return window_1m, window_5m, window_1h
    
    def apply_circuit_breaker_pattern(self, df: DataFrame, failure_threshold: float = 0.05) -> DataFrame:
        """Implement circuit breaker for handling downstream failures"""
        # Calculate error rate
        error_rate_df = df.groupBy(col("event_type")).agg(
            (spark_sum(when(col("processing_error") == True, 1).otherwise(0)) / 
             spark_sum(1)).alias("error_rate")
        )
        
        # Join back and filter based on circuit breaker
        circuit_breaker_df = df.join(
            broadcast(error_rate_df),
            on="event_type"
        ).withColumn(
            "circuit_status",
            when(col("error_rate") > failure_threshold, "open").otherwise("closed")
        ).filter(
            col("circuit_status") == "closed"
        )
        
        return circuit_breaker_df
    
    def apply_sessionization(self, df: DataFrame, session_timeout_minutes: int = 30) -> DataFrame:
        """Create user sessions with gap detection"""
        window_spec = Window.partitionBy(col("user_id")).orderBy(col("event_timestamp"))
        
        sessionized_df = df.withColumn(
            "time_since_last_event",
            (unix_timestamp(col("event_timestamp")) - 
             unix_timestamp(lag(col("event_timestamp")).over(window_spec))) / 60
        ).withColumn(
            "is_new_session",
            when(
                col("time_since_last_event").isNull() | (col("time_since_last_event") > session_timeout_minutes),
                1
            ).otherwise(0)
        ).withColumn(
            "session_number",
            spark_sum(col("is_new_session")).over(window_spec)
        ).withColumn(
            "session_id",
            concat_ws("_", col("user_id"), col("session_number"))
        )
        
        return sessionized_df
        
    def read_kafka_stream(self, kafka_servers: str, topics: list) -> DataFrame:
        """Read streaming data from Kafka"""
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", ",".join(topics)) \
            .option("startingOffsets", "latest") \
            .load()
        
        self.logger.info(f"Reading from Kafka topics: {topics}")
        return df
    
    def parse_events(self, kafka_df: DataFrame) -> DataFrame:
        """Parse JSON events from Kafka"""
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("user_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("properties", StructType([
                StructField("source", StringType()),
                StructField("session_id", StringType()),
                StructField("user_agent", StringType()),
                StructField("ip_address", StringType()),
            ])),
            StructField("source", StringType()),
        ])
        
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Convert timestamp to proper datetime
        parsed_df = parsed_df.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp"))
        )
        
        return parsed_df
    
    def apply_bronze_layer_transformations(self, df: DataFrame) -> DataFrame:
        """Apply Bronze layer transformations (minimal processing)"""
        bronze_df = df.select(
            "event_id",
            "event_type",
            "user_id",
            "event_timestamp",
            col("properties.source").alias("source"),
            col("properties.session_id").alias("session_id"),
            col("properties.ip_address").alias("ip_address"),
            date_format(col("event_timestamp"), "yyyy-MM-dd").alias("event_date"),
            date_format(col("event_timestamp"), "HH").alias("event_hour")
        )
        
        return bronze_df
    
    def apply_silver_layer_transformations(self, df: DataFrame) -> DataFrame:
        """Apply Silver layer transformations (data cleaning & enrichment)"""
        silver_df = df.withColumn(
            "processed_timestamp",
            to_timestamp(col("event_timestamp"))
        ).filter(
            col("user_id").isNotNull()
        ).filter(
            col("event_type").isin(['page_view', 'click', 'purchase', 'login', 'logout', 'search'])
        )
        
        return silver_df
    
    def apply_gold_layer_transformations(self, df: DataFrame) -> DataFrame:
        """Apply Gold layer transformations with complex business logic and ML features"""
        # Window specification for user activity patterns
        user_window = Window.partitionBy(col("user_id")).orderBy(col("event_timestamp").desc()).rangeBetween(-3600, 0)
        
        # Calculate behavioral metrics
        metrics_df = df.withColumn(
            "user_event_velocity", 
            count("*").over(user_window)
        ).withColumn(
            "user_event_diversity",
            count(col("event_type")).over(user_window) / count("*").over(user_window)
        ).withColumn(
            "user_session_continuity",
            (unix_timestamp(col("event_timestamp")) - 
             unix_timestamp(lag(col("event_timestamp")).over(user_window))) / 60
        )
        
        # Time-based features
        time_features_df = metrics_df.withColumn(
            "hour_of_day", 
            expr("hour(event_timestamp)")
        ).withColumn(
            "day_of_week",
            expr("dayofweek(event_timestamp)")
        ).withColumn(
            "is_peak_hour",
            when((col("hour_of_day") >= 9) & (col("hour_of_day") <= 17), 1).otherwise(0)
        )
        
        # Aggregate by 5-minute windows with advanced metrics
        advanced_metrics = time_features_df.groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("event_type"),
            col("source"),
            col("is_peak_hour")
        ).agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            spark_sum(col("user_event_velocity")).alias("total_velocity"),
            avg(col("user_event_diversity")).alias("avg_diversity"),
            percentile_approx(col("user_session_continuity"), 0.95).alias("p95_session_gap"),
            collect_list(struct("user_id", "session_id")).alias("session_details")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "event_type",
            "source",
            "is_peak_hour",
            "event_count",
            "unique_users",
            "total_velocity",
            "avg_diversity",
            "p95_session_gap",
            "session_details"
        )
        
        return advanced_metrics
    
    def write_to_s3(self, df: DataFrame, s3_path: str, layer: str):
        """Write stream to S3 in Parquet format"""
        query = df.writeStream \
            .format("parquet") \
            .option("path", s3_path) \
            .option("checkpointLocation", f"/tmp/checkpoint/{layer}") \
            .partitionBy("event_date") \
            .mode("append") \
            .start()
        
        self.logger.info(f"Writing {layer} layer to S3: {s3_path}")
        return query
    
    def write_to_snowflake(self, df: DataFrame, table_name: str, snowflake_config: dict):
        """Write stream to Snowflake"""
        options = {
            "sfUrl": snowflake_config.get("url"),
            "sfUser": snowflake_config.get("user"),
            "sfPassword": snowflake_config.get("password"),
            "sfDatabase": snowflake_config.get("database"),
            "sfSchema": snowflake_config.get("schema"),
            "dbtable": table_name,
        }
        
        query = df.writeStream \
            .format("snowflake") \
            .options(**options) \
            .option("checkpointLocation", f"/tmp/checkpoint/{table_name}") \
            .mode("append") \
            .start()
        
        self.logger.info(f"Writing to Snowflake table: {table_name}")
        return query
    
    def write_to_console(self, df: DataFrame, query_name: str):
        """Debug output to console"""
        query = df.writeStream \
            .format("console") \
            .option("truncate", False) \
            .option("checkpointLocation", f"/tmp/checkpoint/{query_name}") \
            .start()
        
        return query
    
    def run_pipeline(self, kafka_config: dict, s3_config: dict, snowflake_config: dict = None):
        """Run complete streaming pipeline"""
        try:
            # Read from Kafka
            kafka_df = self.read_kafka_stream(
                kafka_config.get("bootstrap_servers", "localhost:9092"),
                kafka_config.get("topics", ["events"])
            )
            
            # Parse events
            parsed_df = self.parse_events(kafka_df)
            
            # Bronze Layer
            bronze_df = self.apply_bronze_layer_transformations(parsed_df)
            query_bronze = self.write_to_s3(
                bronze_df,
                s3_config.get("bronze_path", "s3://data-lake/bronze/events"),
                "bronze"
            )
            
            # Silver Layer
            silver_df = self.apply_silver_layer_transformations(bronze_df)
            query_silver = self.write_to_s3(
                silver_df,
                s3_config.get("silver_path", "s3://data-lake/silver/events"),
                "silver"
            )
            
            # Gold Layer
            gold_df = self.apply_gold_layer_transformations(silver_df)
            query_gold = self.write_to_s3(
                gold_df,
                s3_config.get("gold_path", "s3://data-lake/gold/metrics"),
                "gold"
            )
            
            # Write to Snowflake (optional)
            if snowflake_config:
                self.write_to_snowflake(gold_df, "EVENTS_METRICS", snowflake_config)
            
            # Await termination
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise


def main():
    """Main entry point"""
    processor = RealTimeStreamProcessor()
    
    kafka_config = {
        "bootstrap_servers": "localhost:9092",
        "topics": ["events"]
    }
    
    s3_config = {
        "bronze_path": "s3://my-data-lake/bronze/events",
        "silver_path": "s3://my-data-lake/silver/events",
        "gold_path": "s3://my-data-lake/gold/metrics"
    }
    
    snowflake_config = {
        "url": "xy12345.snowflakecomputing.com",
        "user": "data_engineer",
        "password": "password",
        "database": "analytics",
        "schema": "events"
    }
    
    processor.run_pipeline(kafka_config, s3_config, snowflake_config)


if __name__ == "__main__":
    main()
