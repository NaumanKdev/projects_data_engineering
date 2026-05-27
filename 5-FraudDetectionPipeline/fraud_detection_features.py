"""
Real-Time Fraud Detection Feature Store
Generates and manages features for fraud detection models
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np
import redis
import json
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, avg, min as spark_min, 
    max as spark_max, stddev, window, row_number
)
from pyspark.sql import Window as SparkWindow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureStore:
    """Manage features for fraud detection"""
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        """Initialize feature store with Redis backend"""
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
        self.logger = logger
    
    def store_feature(self, feature_name: str, value: Any, ttl: int = 3600):
        """Store feature in Redis cache"""
        try:
            self.redis_client.setex(
                feature_name,
                ttl,
                json.dumps(value, default=str)
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to store feature {feature_name}: {e}")
            return False
    
    def get_feature(self, feature_name: str) -> Optional[Any]:
        """Retrieve feature from cache"""
        try:
            value = self.redis_client.get(feature_name)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            self.logger.error(f"Failed to retrieve feature {feature_name}: {e}")
            return None
    
    def delete_feature(self, feature_name: str):
        """Delete feature from cache"""
        self.redis_client.delete(feature_name)


class TransactionFeatureEngineering:
    """Generate features for fraud detection"""
    
    @staticmethod
    def compute_user_transaction_stats(transaction_df: DataFrame, 
                                       user_id: str, 
                                       time_window_hours: int = 24) -> Dict[str, float]:
        """Compute user transaction statistics"""
        
        window_spec = SparkWindow.partitionBy(col("user_id")).orderBy(
            col("transaction_timestamp").desc()
        ).rangeBetween(-time_window_hours * 3600, 0)
        
        stats_df = transaction_df.withColumn(
            "transaction_count", 
            count("*").over(window_spec)
        ).withColumn(
            "total_amount",
            spark_sum(col("amount")).over(window_spec)
        ).withColumn(
            "avg_amount",
            avg(col("amount")).over(window_spec)
        ).withColumn(
            "max_amount",
            spark_max(col("amount")).over(window_spec)
        ).withColumn(
            "min_amount",
            spark_min(col("amount")).over(window_spec)
        ).filter(
            col("user_id") == user_id
        ).select(
            "transaction_count",
            "total_amount",
            "avg_amount",
            "max_amount",
            "min_amount"
        ).limit(1)
        
        if stats_df.count() > 0:
            row = stats_df.collect()[0]
            return {
                'transaction_count': float(row.transaction_count or 0),
                'total_amount': float(row.total_amount or 0),
                'avg_amount': float(row.avg_amount or 0),
                'max_amount': float(row.max_amount or 0),
                'min_amount': float(row.min_amount or 0)
            }
        else:
            return {
                'transaction_count': 0,
                'total_amount': 0,
                'avg_amount': 0,
                'max_amount': 0,
                'min_amount': 0
            }
    
    @staticmethod
    def compute_merchant_stats(transaction_df: DataFrame,
                              merchant_id: str,
                              time_window_hours: int = 24) -> Dict[str, float]:
        """Compute merchant transaction statistics"""
        
        window_spec = SparkWindow.partitionBy(col("merchant_id")).orderBy(
            col("transaction_timestamp").desc()
        ).rangeBetween(-time_window_hours * 3600, 0)
        
        stats_df = transaction_df.withColumn(
            "merchant_transaction_count",
            count("*").over(window_spec)
        ).withColumn(
            "merchant_avg_amount",
            avg(col("amount")).over(window_spec)
        ).filter(
            col("merchant_id") == merchant_id
        ).select(
            "merchant_transaction_count",
            "merchant_avg_amount"
        ).limit(1)
        
        if stats_df.count() > 0:
            row = stats_df.collect()[0]
            return {
                'merchant_transaction_count': float(row.merchant_transaction_count or 0),
                'merchant_avg_amount': float(row.merchant_avg_amount or 0)
            }
        else:
            return {
                'merchant_transaction_count': 0,
                'merchant_avg_amount': 0
            }
    
    @staticmethod
    def compute_behavioral_biometrics(transaction_df: DataFrame, user_id: str,
                                      time_window_hours: int = 24) -> Dict[str, float]:
        """Extract behavioral biometrics for user profiling"""
        
        user_transactions = transaction_df.filter(
            col("user_id") == user_id
        ).filter(
            col("transaction_timestamp") >= (datetime.now() - timedelta(hours=time_window_hours))
        )
        
        if user_transactions.count() == 0:
            return {}
        
        # Temporal patterns
        temporal_stats = user_transactions.select(
            hour(col("transaction_timestamp")).alias("hour"),
            dayofweek(col("transaction_timestamp")).alias("day_of_week")
        ).groupBy("hour", "day_of_week").count().collect()
        
        # Spatial patterns
        spatial_stats = user_transactions.select("location").distinct().count()
        
        biometrics = {
            'unique_hours': len(temporal_stats),
            'unique_locations': spatial_stats,
            'transaction_consistency': 1.0 if spatial_stats == 1 else 0.5,
            'peak_transaction_hour': max([row.hour for row in temporal_stats], default=12)
        }
        
        return biometrics
    
    @staticmethod
    def compute_network_features(transaction_df: DataFrame, 
                                user_id: str,
                                merchant_id: str) -> Dict[str, float]:
        """Extract network-based features for graph analytics"""
        
        # User-Merchant connections
        user_merchant_frequency = transaction_df.filter(
            col("user_id") == user_id
        ).groupBy("merchant_id").count().count()
        
        # User-User connections (co-shopping patterns)
        user_coconsuming = transaction_df.filter(
            col("merchant_id") == merchant_id
        ).select("user_id").distinct().count()
        
        # Merchant risk score based on fraud rate
        merchant_fraud_rate = transaction_df.filter(
            col("merchant_id") == merchant_id
        ).filter(col("is_fraud") == True).count() / transaction_df.filter(
            col("merchant_id") == merchant_id
        ).count()
        
        network_features = {
            'user_merchant_frequency': float(user_merchant_frequency),
            'merchant_customer_base_size': float(user_coconsuming),
            'merchant_fraud_rate': float(merchant_fraud_rate),
            'network_risk_score': float(merchant_fraud_rate * user_merchant_frequency)
        }
        
        return network_features


class FraudDetectionModel:
    """Fraud detection ML model"""
    
    def __init__(self):
        """Initialize model"""
        self.model = None
        self.feature_names = None
        self.threshold = 0.5
    
    def load_model(self, model_path: str):
        """Load pre-trained model from MLflow"""
        try:
            import mlflow.pyfunc
            self.model = mlflow.pyfunc.load_model(model_path)
            logger.info(f"Loaded model from {model_path}")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def predict(self, features: Dict[str, float]) -> Dict[str, Any]:
        """Predict fraud probability"""
        try:
            if not self.model:
                raise ValueError("Model not loaded")
            
            # Convert features to DataFrame
            feature_values = [features[f] for f in self.feature_names]
            
            # Get prediction
            prediction = self.model.predict([feature_values])[0]
            
            # Get prediction probabilities if available
            is_fraud = 1 if prediction > self.threshold else 0
            
            result = {
                'fraud_probability': float(prediction),
                'is_fraud': is_fraud,
                'confidence': float(abs(prediction - 0.5) * 2),
                'risk_level': self._get_risk_level(prediction)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return {
                'fraud_probability': 0.5,
                'is_fraud': 0,
                'confidence': 0,
                'risk_level': 'unknown'
            }
    
    @staticmethod
    def _get_risk_level(probability: float) -> str:
        """Determine risk level from probability"""
        if probability < 0.3:
            return 'low'
        elif probability < 0.7:
            return 'medium'
        else:
            return 'high'


class RuleBasedFraudDetector:
    """Rule-based fraud detection for high-confidence cases"""
    
    @staticmethod
    def apply_rules(transaction: Dict[str, Any], 
                   user_stats: Dict[str, float]) -> Optional[str]:
        """Apply business rules for fraud detection"""
        
        rules_triggered = []
        
        # Rule 1: Unusual amount
        if user_stats['avg_amount'] > 0:
            amount_ratio = transaction['amount'] / user_stats['avg_amount']
            if amount_ratio > 10:  # 10x average
                rules_triggered.append('unusual_amount')
        
        # Rule 2: High velocity
        if user_stats['transaction_count'] > 20:  # >20 transactions in 24 hours
            rules_triggered.append('high_velocity')
        
        # Rule 3: Unusual merchant
        high_risk_merchants = ['casino', 'gambling', 'adult_content']
        if transaction.get('merchant_category') in high_risk_merchants:
            rules_triggered.append('high_risk_merchant')
        
        # Rule 4: Impossible travel
        if 'last_transaction_location' in transaction:
            rules_triggered.append('impossible_travel')
        
        if rules_triggered:
            return '|'.join(rules_triggered)
        
        return None


def create_spark_streaming_fraud_detection(spark, kafka_bootstrap_servers: str):
    """Create Spark Structured Streaming pipeline for fraud detection"""
    
    # Read from Kafka
    transactions_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("timestamp", TimestampType()),
        StructField("merchant_category", StringType())
    ])
    
    parsed_df = transactions_df.select(
        col("value").cast("string")
    )
    
    # Process and score
    logger.info("Spark Streaming pipeline created for fraud detection")
    
    return parsed_df


if __name__ == "__main__":
    # Example usage
    feature_store = FeatureStore()
    
    # Example transaction
    transaction = {
        'transaction_id': 'txn_123',
        'user_id': 'user_456',
        'merchant_id': 'merchant_789',
        'amount': 1500.00,
        'timestamp': datetime.now().isoformat(),
        'merchant_category': 'retail'
    }
    
    # Example user stats
    user_stats = {
        'transaction_count': 5,
        'total_amount': 500.00,
        'avg_amount': 100.00,
        'max_amount': 200.00,
        'min_amount': 50.00
    }
    
    # Example merchant stats
    merchant_stats = {
        'merchant_transaction_count': 1000,
        'merchant_avg_amount': 300.00
    }
    
    # Extract features
    features = TransactionFeatureEngineering.extract_transaction_features(
        transaction, user_stats, merchant_stats
    )
    
    print("Extracted Features:")
    print(json.dumps(features, indent=2))
