"""
Data Quality Framework using Great Expectations
Comprehensive validation suite for data pipelines
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
from great_expectations.data_context import DataContext
from great_expectations.dataset.pandas_dataset import PandasDataset
from datetime import datetime, timedelta
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityValidator:
    """Comprehensive data quality validation"""
    
    def __init__(self, context_dir: str = "gx"):
        """Initialize Great Expectations context"""
        self.context = DataContext(context_root_dir=context_dir)
        self.validations_run = []
    
    def validate_schema(self, df: pd.DataFrame, expected_schema: Dict[str, str]) -> Dict[str, Any]:
        """Validate dataframe schema"""
        result = {
            'validation': 'schema',
            'passed': True,
            'errors': []
        }
        
        for col, dtype in expected_schema.items():
            if col not in df.columns:
                result['passed'] = False
                result['errors'].append(f"Missing column: {col}")
            elif not self._check_dtype(df[col].dtype, dtype):
                result['passed'] = False
                result['errors'].append(
                    f"Column {col}: expected {dtype}, got {df[col].dtype}"
                )
        
        return result
    
    @staticmethod
    def _check_dtype(actual, expected) -> bool:
        """Check if data type matches expectation"""
        dtype_map = {
            'string': 'object',
            'integer': 'int',
            'float': 'float',
            'datetime': 'datetime64',
            'boolean': 'bool'
        }
        return str(actual).startswith(dtype_map.get(expected, expected))
    
    def validate_completeness(self, df: pd.DataFrame, column: str, 
                            threshold: float = 0.95) -> Dict[str, Any]:
        """Validate column completeness (non-null percentage)"""
        non_null_count = df[column].notna().sum()
        total_count = len(df)
        completeness = non_null_count / total_count if total_count > 0 else 0
        
        result = {
            'validation': 'completeness',
            'column': column,
            'passed': completeness >= threshold,
            'completeness_percentage': round(completeness * 100, 2),
            'threshold': threshold * 100,
            'null_count': total_count - non_null_count
        }
        
        return result
    
    def validate_uniqueness(self, df: pd.DataFrame, column: str, 
                           threshold: float = 1.0) -> Dict[str, Any]:
        """Validate column uniqueness"""
        unique_count = df[column].nunique()
        total_count = len(df)
        uniqueness = unique_count / total_count if total_count > 0 else 0
        
        result = {
            'validation': 'uniqueness',
            'column': column,
            'passed': uniqueness >= threshold,
            'uniqueness_percentage': round(uniqueness * 100, 2),
            'unique_values': unique_count,
            'total_values': total_count,
            'duplicates': total_count - unique_count
        }
        
        return result
    
    def validate_range(self, df: pd.DataFrame, column: str, 
                      min_val: float = None, max_val: float = None) -> Dict[str, Any]:
        """Validate numeric column range"""
        out_of_range = 0
        
        if min_val is not None:
            out_of_range += (df[column] < min_val).sum()
        if max_val is not None:
            out_of_range += (df[column] > max_val).sum()
        
        result = {
            'validation': 'range',
            'column': column,
            'passed': out_of_range == 0,
            'min_value': min_val,
            'max_value': max_val,
            'out_of_range_count': out_of_range,
            'actual_min': df[column].min(),
            'actual_max': df[column].max()
        }
        
        return result
    
    def validate_regex_pattern(self, df: pd.DataFrame, column: str, 
                              pattern: str) -> Dict[str, Any]:
        """Validate column values against regex pattern"""
        import re
        regex = re.compile(pattern)
        non_matching = df[~df[column].astype(str).apply(lambda x: regex.match(x))].shape[0]
        
        result = {
            'validation': 'regex_pattern',
            'column': column,
            'passed': non_matching == 0,
            'pattern': pattern,
            'non_matching_count': non_matching,
            'matching_count': len(df) - non_matching
        }
        
        return result
    
    def validate_foreign_key(self, df: pd.DataFrame, fk_column: str,
                           reference_df: pd.DataFrame, ref_column: str) -> Dict[str, Any]:
        """Validate foreign key constraint"""
        invalid_fks = df[~df[fk_column].isin(reference_df[ref_column])].shape[0]
        
        result = {
            'validation': 'foreign_key',
            'column': fk_column,
            'passed': invalid_fks == 0,
            'invalid_foreign_keys': invalid_fks,
            'total_rows': len(df),
            'reference_table_size': len(reference_df)
        }
        
        return result
    
    def detect_outliers_iqr(self, df: pd.DataFrame, column: str, 
                           multiplier: float = 1.5) -> Dict[str, Any]:
        """Detect outliers using Interquartile Range method"""
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        
        outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
        
        result = {
            'validation': 'outlier_detection_iqr',
            'column': column,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'outlier_count': len(outliers),
            'outlier_percentage': (len(outliers) / len(df)) * 100
        }
        
        return result
    
    def detect_statistical_anomalies(self, df: pd.DataFrame, column: str,
                                     z_score_threshold: float = 3.0) -> Dict[str, Any]:
        """Detect anomalies using Z-score statistical method"""
        from scipy import stats
        
        numeric_df = pd.to_numeric(df[column], errors='coerce')
        z_scores = np.abs(stats.zscore(numeric_df.dropna()))
        
        anomaly_count = (z_scores > z_score_threshold).sum()
        
        result = {
            'validation': 'statistical_anomaly',
            'column': column,
            'z_score_threshold': z_score_threshold,
            'anomaly_count': int(anomaly_count),
            'anomaly_percentage': (anomaly_count / len(numeric_df.dropna())) * 100
        }
        
        return result
    
    def detect_duplicate_patterns(self, df: pd.DataFrame, 
                                 columns: list = None) -> Dict[str, Any]:
        """Detect duplicate records and patterns"""
        if columns is None:
            columns = df.columns.tolist()
        
        duplicates = df.duplicated(subset=columns, keep=False)
        duplicate_count = duplicates.sum()
        
        # Find most common duplicates
        duplicate_groups = df[duplicates].groupby(columns).size().reset_index(name='count')
        duplicate_groups = duplicate_groups.sort_values('count', ascending=False)
        
        result = {
            'validation': 'duplicate_pattern',
            'columns': columns,
            'total_duplicates': duplicate_count,
            'duplicate_percentage': (duplicate_count / len(df)) * 100,
            'top_duplicate_groups': duplicate_groups.head(5).to_dict('records')
        }
        
        return result
    
    def run_comprehensive_validation(self, df: pd.DataFrame, 
                                    config: Dict[str, Any]) -> Dict[str, Any]:
        """Run all validations based on configuration"""
        results = {
            'timestamp': datetime.utcnow().isoformat(),
            'table_name': config.get('table_name'),
            'total_rows': len(df),
            'validations': [],
            'overall_passed': True
        }
        
        # Schema validation
        if 'expected_schema' in config:
            schema_result = self.validate_schema(df, config['expected_schema'])
            results['validations'].append(schema_result)
            results['overall_passed'] &= schema_result['passed']
        
        # Column validations
        for col_config in config.get('column_validations', []):
            col = col_config['column']
            
            # Completeness
            if 'completeness_threshold' in col_config:
                result = self.validate_completeness(df, col, col_config['completeness_threshold'])
                results['validations'].append(result)
                results['overall_passed'] &= result['passed']
            
            # Uniqueness
            if 'uniqueness_threshold' in col_config:
                result = self.validate_uniqueness(df, col, col_config['uniqueness_threshold'])
                results['validations'].append(result)
                results['overall_passed'] &= result['passed']
            
            # Range
            if 'min_value' in col_config or 'max_value' in col_config:
                result = self.validate_range(
                    df, col,
                    col_config.get('min_value'),
                    col_config.get('max_value')
                )
                results['validations'].append(result)
                results['overall_passed'] &= result['passed']
            
            # Regex pattern
            if 'regex_pattern' in col_config:
                result = self.validate_regex_pattern(df, col, col_config['regex_pattern'])
                results['validations'].append(result)
                results['overall_passed'] &= result['passed']
        
        # Business logic validations
        for logic_config in config.get('business_logic_validations', []):
            rule_name = logic_config['rule_name']
            # Condition should be a callable that returns a boolean Series
            condition = logic_config['condition'](df)
            result = self.validate_business_logic(df, rule_name, condition)
            results['validations'].append(result)
            results['overall_passed'] &= result['passed']
        
        self.validations_run.append(results)
        return results


class DataProfiler:
    """Profile data for anomaly detection with advanced statistical methods"""
    
    @staticmethod
    def profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive data profile with advanced metrics"""
        profile = {
            'timestamp': datetime.utcnow().isoformat(),
            'shape': {'rows': len(df), 'columns': len(df.columns)},
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'duplicate_rows': df.duplicated().sum(),
            'columns': {}
        }
        
        for col in df.columns:
            col_profile = {
                'dtype': str(df[col].dtype),
                'non_null_count': df[col].notna().sum(),
                'null_count': df[col].isna().sum(),
                'null_percentage': (df[col].isna().sum() / len(df)) * 100,
                'unique_count': df[col].nunique(),
                'memory_usage_kb': df[col].memory_usage(deep=True) / 1024,
            }
            
            # Numeric columns
            if df[col].dtype in ['int64', 'float64']:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                col_profile.update({
                    'min': float(numeric_col.min()),
                    'max': float(numeric_col.max()),
                    'mean': float(numeric_col.mean()),
                    'median': float(numeric_col.median()),
                    'std': float(numeric_col.std()),
                    'variance': float(numeric_col.var()),
                    'skewness': float(numeric_col.skew()),
                    'kurtosis': float(numeric_col.kurtosis()),
                    'q25': float(numeric_col.quantile(0.25)),
                    'q75': float(numeric_col.quantile(0.75)),
                    'iqr': float(numeric_col.quantile(0.75) - numeric_col.quantile(0.25))
                })
            
            # String columns
            elif df[col].dtype == 'object':
                str_lengths = df[col].astype(str).str.len()
                col_profile.update({
                    'min_length': int(str_lengths.min()),
                    'max_length': int(str_lengths.max()),
                    'avg_length': float(str_lengths.mean()),
                    'most_common_value': str(df[col].mode()[0]) if len(df[col].mode()) > 0 else None,
                    'most_common_count': int(df[col].value_counts().iloc[0]) if len(df[col].value_counts()) > 0 else 0
                })
            
            profile['columns'][col] = col_profile
        
        return profile
    
    @staticmethod
    def detect_anomalies(df: pd.DataFrame, profile: Dict[str, Any], 
                        std_threshold: float = 3.0,
                        iqr_multiplier: float = 1.5) -> Dict[str, Any]:
        """Detect anomalies using multiple methods"""
        from scipy import stats
        
        anomalies = {
            'timestamp': datetime.utcnow().isoformat(),
            'anomalies_zscore': [],
            'anomalies_iqr': [],
            'anomalies_isolation_forest': []
        }
        
        for col in df.columns:
            col_profile = profile['columns'][col]
            
            if df[col].dtype in ['int64', 'float64']:
                numeric_col = pd.to_numeric(df[col], errors='coerce').dropna()
                
                # Z-score method
                z_scores = np.abs(stats.zscore(numeric_col))
                z_anomaly_count = (z_scores > std_threshold).sum()
                
                if z_anomaly_count > 0:
                    anomalies['anomalies_zscore'].append({
                        'column': col,
                        'method': 'z_score',
                        'threshold': std_threshold,
                        'anomaly_count': int(z_anomaly_count),
                        'anomaly_percentage': (z_anomaly_count / len(numeric_col)) * 100
                    })
                
                # IQR method
                Q1 = numeric_col.quantile(0.25)
                Q3 = numeric_col.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - iqr_multiplier * IQR
                upper_bound = Q3 + iqr_multiplier * IQR
                
                iqr_anomaly_count = ((numeric_col < lower_bound) | (numeric_col > upper_bound)).sum()
                
                if iqr_anomaly_count > 0:
                    anomalies['anomalies_iqr'].append({
                        'column': col,
                        'method': 'iqr',
                        'lower_bound': float(lower_bound),
                        'upper_bound': float(upper_bound),
                        'anomaly_count': int(iqr_anomaly_count),
                        'anomaly_percentage': (iqr_anomaly_count / len(numeric_col)) * 100
                    })
        
        # Isolation Forest for multivariate anomalies
        from sklearn.ensemble import IsolationForest
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        
        if len(numeric_cols) > 0:
            iso_forest = IsolationForest(contamination=0.05, random_state=42)
            predictions = iso_forest.fit_predict(df[numeric_cols].fillna(df[numeric_cols].mean()))
            anomaly_count = (predictions == -1).sum()
            
            if anomaly_count > 0:
                anomalies['anomalies_isolation_forest'].append({
                    'method': 'isolation_forest',
                    'columns': numeric_cols,
                    'contamination': 0.05,
                    'anomaly_count': int(anomaly_count),
                    'anomaly_percentage': (anomaly_count / len(df)) * 100
                })
        
        return anomalies


def example_validation_config() -> Dict[str, Any]:
    """Example data quality configuration"""
    return {
        'table_name': 'customers',
        'expected_schema': {
            'customer_id': 'string',
            'email': 'string',
            'age': 'integer',
            'created_at': 'datetime'
        },
        'column_validations': [
            {
                'column': 'customer_id',
                'uniqueness_threshold': 1.0,
                'completeness_threshold': 1.0
            },
            {
                'column': 'email',
                'completeness_threshold': 0.95,
                'regex_pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            },
            {
                'column': 'age',
                'min_value': 0,
                'max_value': 150
            }
        ],
        'business_logic_validations': [
            {
                'rule_name': 'email_must_not_be_null',
                'condition': lambda df: df['email'].notna()
            }
        ]
    }


if __name__ == "__main__":
    # Example usage
    validator = DataQualityValidator()
    profiler = DataProfiler()
    
    # Sample data
    sample_df = pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C003'],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'age': [25, 30, 35],
        'created_at': [datetime.now()] * 3
    })
    
    config = example_validation_config()
    results = validator.run_comprehensive_validation(sample_df, config)
    
    print("Validation Results:")
    print(json.dumps(results, indent=2, default=str))
    
    # Profile data
    profile = profiler.profile_dataframe(sample_df)
    print("\nData Profile:")
    print(json.dumps(profile, indent=2))
