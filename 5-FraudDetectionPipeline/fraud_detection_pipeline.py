"""
Real-Time Fraud Detection Streaming Pipeline
Orchestrates data ingestion, feature engineering, and model scoring
"""

import logging
from typing import Dict, Any, Optional
import json
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    udf, struct, when, lit
)
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType
import xgboost as xgb
from fraud_detection_features import (
    TransactionFeatureEngineering,
    RuleBasedFraudDetector,
    FeatureStore
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealTimeFraudDetectionPipeline:
    """Real-time fraud detection streaming pipeline"""
    
    def __init__(self, model_path: str = None):
        """Initialize pipeline"""
        self.spark = SparkSession.builder \
            .appName("FraudDetectionPipeline") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()
        
        self.model_path = model_path
        self.model = None
        self.feature_store = FeatureStore()
        self.logger = logger
    
    def read_transactions_stream(self, kafka_servers: str, 
                                topic: str = "transactions") -> DataFrame:
        """Read transaction stream from Kafka"""
        transactions_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON
        schema = StructType([
            StructField("transaction_id", StringType()),
            StructField("user_id", StringType()),
            StructField("merchant_id", StringType()),
            StructField("amount", DoubleType()),
            StructField("timestamp", StringType()),
            StructField("merchant_category", StringType()),
            StructField("location", StringType()),
            StructField("device_type", StringType())
        ])
        
        parsed_df = transactions_df.select(
            from_json(col("value").cast("string"), schema).alias("transaction")
        ).select("transaction.*") \
        .withColumn("transaction_timestamp", to_timestamp(col("timestamp")))
        
        self.logger.info(f"Reading transaction stream from Kafka topic: {topic}")
        return parsed_df
    
    def apply_feature_engineering(self, transactions_df: DataFrame) -> DataFrame:
        """Apply feature engineering"""
        
        # Define UDF for feature extraction
        def extract_features(user_id: str, merchant_id: str, amount: float) -> str:
            try:
                # Get cached stats
                user_stats = self.feature_store.get_feature(f"user_stats_{user_id}")
                merchant_stats = self.feature_store.get_feature(f"merchant_stats_{merchant_id}")
                
                if not user_stats or not merchant_stats:
                    # Return default features
                    return json.dumps({
                        'transaction_amount': float(amount),
                        'user_transaction_count': 0,
                        'merchant_avg_amount': 0
                    })
                
                transaction = {
                    'amount': amount,
                    'timestamp': datetime.now().isoformat()
                }
                
                features = TransactionFeatureEngineering.extract_transaction_features(
                    transaction, user_stats, merchant_stats
                )
                
                return json.dumps(features)
            except Exception as e:
                logger.error(f"Feature extraction error: {e}")
                return json.dumps({'error': str(e)})
        
        # Register UDF
        extract_features_udf = udf(extract_features, StringType())
        
        # Apply feature engineering
        features_df = transactions_df.withColumn(
            "features_json",
            extract_features_udf(col("user_id"), col("merchant_id"), col("amount"))
        )
        
        self.logger.info("Feature engineering applied")
        return features_df
    
    def apply_rule_based_detection(self, features_df: DataFrame) -> DataFrame:
        """Apply rule-based fraud detection"""
        
        def apply_rules(user_id: str, merchant_id: str, amount: float, category: str) -> str:
            try:
                rules_triggered = []
                
                # Get user stats from cache
                user_stats = self.feature_store.get_feature(f"user_stats_{user_id}")
                
                if user_stats:
                    transaction = {
                        'amount': amount,
                        'merchant_category': category
                    }
                    rules = RuleBasedFraudDetector.apply_rules(transaction, user_stats)
                    if rules:
                        rules_triggered.append(rules)
                
                return '|'.join(rules_triggered) if rules_triggered else 'none'
            except Exception as e:
                logger.error(f"Rule detection error: {e}")
                return 'error'
        
        # Register UDF
        apply_rules_udf = udf(apply_rules, StringType())
        
        # Apply rule-based detection
        rules_df = features_df.withColumn(
            "rules_triggered",
            apply_rules_udf(col("user_id"), col("merchant_id"), col("amount"), col("merchant_category"))
        ).withColumn(
            "rule_based_fraud_flag",
            when(col("rules_triggered") != "none", 1).otherwise(0)
        )
        
        self.logger.info("Rule-based detection applied")
        return rules_df
    
    def apply_ml_model(self, rules_df: DataFrame) -> DataFrame:
        """Apply machine learning model"""
        
        # Load model (assuming XGBoost)
        try:
            self.model = xgb.Booster()
            self.model.load_model(self.model_path)
            self.logger.info(f"Loaded ML model from {self.model_path}")
        except Exception as e:
            self.logger.warning(f"Could not load ML model: {e}")
            self.model = None
        
        def score_transaction(features_json: str, rules_flag: int) -> float:
            try:
                if not self.model:
                    return 0.5
                
                features = json.loads(features_json)
                # Prepare features array for model
                feature_vector = [
                    features.get('transaction_amount', 0),
                    features.get('user_transaction_count', 0),
                    features.get('merchant_avg_amount', 0),
                    features.get('amount_vs_user_avg', 0),
                    features.get('transaction_velocity', 0),
                    features.get('time_of_day', 12)
                ]
                
                # Score with model
                import xgboost as xgb
                dmatrix = xgb.DMatrix([feature_vector])
                prediction = self.model.predict(dmatrix)[0]
                
                # Boost score if rules triggered
                if rules_flag == 1:
                    prediction = min(prediction + 0.2, 1.0)
                
                return float(prediction)
            except Exception as e:
                logger.error(f"ML scoring error: {e}")
                return 0.5 if rules_flag == 0 else 0.7
        
        # Register UDF
        score_udf = udf(score_transaction, DoubleType())
        
        # Apply model scoring
        scored_df = rules_df.withColumn(
            "fraud_probability",
            score_udf(col("features_json"), col("rule_based_fraud_flag"))
        ).withColumn(
            "is_fraud",
            when(col("fraud_probability") > 0.5, 1).otherwise(0)
        ).withColumn(
            "risk_level",
            when(col("fraud_probability") < 0.3, "low")
            .when(col("fraud_probability") < 0.7, "medium")
            .otherwise("high")
        )
        
        self.logger.info("ML model scoring applied")
        return scored_df
    
    def enrich_predictions(self, scored_df: DataFrame) -> DataFrame:
        """Enrich fraud predictions with metadata"""
        
        enriched_df = scored_df.withColumn(
            "processing_timestamp", current_timestamp()
        ).withColumn(
            "model_version", lit("1.0.0")
        ).withColumn(
            "prediction_id",
            col("transaction_id")
        )
        
        return enriched_df
    
    def write_predictions(self, df: DataFrame, output_mode: str = "append"):
        """Write predictions to outputs"""
        
        # Write to Kafka
        kafka_query = df.select(
            struct(
                col("transaction_id"),
                col("user_id"),
                col("merchant_id"),
                col("amount"),
                col("is_fraud"),
                col("fraud_probability"),
                col("risk_level"),
                col("rules_triggered"),
                col("processing_timestamp")
            ).cast("string").alias("value")
        ).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "fraud_predictions") \
        .option("checkpointLocation", "/tmp/fraud_checkpoint") \
        .option("mode", output_mode) \
        .start()
        
        # Write to PostgreSQL for analytics
        def write_to_postgres(batch_df, batch_id):
            try:
                batch_df.write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://localhost:5432/fraud_db") \
                    .option("dbtable", "fraud_predictions") \
                    .option("user", "fraud_user") \
                    .option("password", "password") \
                    .mode("append") \
                    .save()
                
                logger.info(f"Batch {batch_id} written to PostgreSQL")
            except Exception as e:
                logger.error(f"Failed to write batch {batch_id}: {e}")
        
        postgres_query = df.writeStream \
            .foreachBatch(write_to_postgres) \
            .option("checkpointLocation", "/tmp/fraud_postgres_checkpoint") \
            .start()
        
        # Write to console for monitoring
        console_query = df.writeStream \
            .format("console") \
            .option("truncate", False) \
            .option("checkpointLocation", "/tmp/fraud_console_checkpoint") \
            .start()
        
        self.logger.info("Streaming queries started")
        
        return [kafka_query, postgres_query, console_query]
    
    def run_pipeline(self, kafka_servers: str = "localhost:9092"):
        """Execute complete fraud detection pipeline"""
        try:
            # Read stream
            transactions_df = self.read_transactions_stream(kafka_servers)
            
            # Feature engineering
            features_df = self.apply_feature_engineering(transactions_df)
            
            # Rule-based detection
            rules_df = self.apply_rule_based_detection(features_df)
            
            # ML model scoring
            scored_df = self.apply_ml_model(rules_df)
            
            # Enrich predictions
            enriched_df = self.enrich_predictions(scored_df)
            
            # Write predictions
            queries = self.write_predictions(enriched_df, output_mode="append")
            
            # Await termination
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise


if __name__ == "__main__":
    pipeline = RealTimeFraudDetectionPipeline(
        model_path="models/fraud_detector_v1.0.0"
    )
    
    pipeline.run_pipeline(kafka_servers="localhost:9092")
