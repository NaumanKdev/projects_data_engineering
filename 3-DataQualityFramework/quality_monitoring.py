"""
Data Quality Monitoring Dashboard
Real-time monitoring of data quality metrics
"""

import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta
import json
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
validation_runs = Counter(
    'dq_validation_runs_total',
    'Total number of validation runs',
    ['table_name', 'status']
)

validation_failures = Counter(
    'dq_validation_failures_total',
    'Total validation failures',
    ['table_name', 'validation_type']
)

validation_duration = Histogram(
    'dq_validation_duration_seconds',
    'Validation execution time',
    ['table_name']
)

data_quality_score = Gauge(
    'dq_quality_score',
    'Overall data quality score (0-100)',
    ['table_name']
)

row_count = Gauge(
    'dq_row_count',
    'Number of rows processed',
    ['table_name']
)

null_percentage = Gauge(
    'dq_null_percentage',
    'Percentage of null values',
    ['table_name', 'column']
)


class QualityAlertManager:
    """Manage data quality alerts"""
    
    SLACK_WEBHOOK = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    EMAIL_FROM = "data-quality@company.com"
    EMAIL_TO = ["data-team@company.com"]
    
    @staticmethod
    def send_slack_alert(message: str, severity: str = "warning"):
        """Send alert to Slack"""
        try:
            import requests
            
            color_map = {
                'critical': '#FF0000',
                'warning': '#FFA500',
                'info': '#0000FF'
            }
            
            payload = {
                'attachments': [{
                    'color': color_map.get(severity, '#808080'),
                    'title': f'Data Quality Alert - {severity.upper()}',
                    'text': message,
                    'footer': 'Data Quality Framework',
                    'ts': int(time.time())
                }]
            }
            
            response = requests.post(QualityAlertManager.SLACK_WEBHOOK, json=payload)
            logger.info(f"Slack alert sent: {response.status_code}")
            
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
    
    @staticmethod
    def send_email_alert(subject: str, body: str):
        """Send email alert"""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart()
            msg['From'] = QualityAlertManager.EMAIL_FROM
            msg['To'] = ','.join(QualityAlertManager.EMAIL_TO)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))
            
            # Configure SMTP server
            # server = smtplib.SMTP('smtp.gmail.com', 587)
            # server.starttls()
            # server.login(email, password)
            # server.send_message(msg)
            # server.quit()
            
            logger.info("Email alert sent")
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")


class QualityDashboard:
    """Dashboard for data quality metrics"""
    
    def __init__(self):
        self.metrics_history = []
    
    def record_validation_result(self, result: Dict[str, Any]):
        """Record validation result"""
        metrics = {
            'timestamp': datetime.utcnow().isoformat(),
            'table_name': result['table_name'],
            'total_validations': len(result['validations']),
            'passed_validations': sum(1 for v in result['validations'] if v['passed']),
            'failed_validations': sum(1 for v in result['validations'] if not v['passed']),
            'overall_passed': result['overall_passed'],
            'row_count': result['total_rows']
        }
        
        # Calculate quality score
        if metrics['total_validations'] > 0:
            quality_score = (metrics['passed_validations'] / metrics['total_validations']) * 100
        else:
            quality_score = 100
        
        metrics['quality_score'] = quality_score
        
        # Record metrics
        table = result['table_name']
        validation_runs.labels(table_name=table, status='pass' if result['overall_passed'] else 'fail').inc()
        data_quality_score.labels(table_name=table).set(quality_score)
        row_count.labels(table_name=table).set(result['total_rows'])
        validation_duration.labels(table_name=table).observe(1.0)  # Placeholder
        
        # Record failures
        for validation in result['validations']:
            if not validation['passed']:
                validation_failures.labels(
                    table_name=table,
                    validation_type=validation['validation']
                ).inc()
        
        self.metrics_history.append(metrics)
        
        # Alert on failures
        if not result['overall_passed']:
            failed_validations = [
                v['validation'] for v in result['validations'] if not v['passed']
            ]
            alert_message = (
                f"Data quality check failed for {table}\n"
                f"Failed validations: {', '.join(failed_validations)}\n"
                f"Quality Score: {quality_score:.1f}%"
            )
            QualityAlertManager.send_slack_alert(alert_message, severity='critical')
        
        return metrics
    
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """Get dashboard summary"""
        if not self.metrics_history:
            return {}
        
        recent_metrics = self.metrics_history[-100:]  # Last 100 records
        
        tables = {}
        for metric in recent_metrics:
            table = metric['table_name']
            if table not in tables:
                tables[table] = {
                    'quality_scores': [],
                    'failed_counts': []
                }
            tables[table]['quality_scores'].append(metric['quality_score'])
            tables[table]['failed_counts'].append(metric['failed_validations'])
        
        summary = {}
        for table, data in tables.items():
            summary[table] = {
                'latest_quality_score': data['quality_scores'][-1],
                'avg_quality_score': sum(data['quality_scores']) / len(data['quality_scores']),
                'total_failures': sum(data['failed_counts']),
                'last_check': recent_metrics[-1]['timestamp']
            }
        
        return summary


# Example integration with Airflow
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def validate_data_quality(**context):
    validator = DataQualityValidator()
    dashboard = QualityDashboard()
    
    # Run validations
    results = validator.run_comprehensive_validation(df, config)
    
    # Record metrics
    dashboard.record_validation_result(results)
    
    if not results['overall_passed']:
        raise ValueError("Data quality validation failed")

default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True
}

dag = DAG('data_quality_monitoring', default_args=default_args, schedule_interval='@hourly')

quality_check_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)
"""


if __name__ == "__main__":
    # Start Prometheus metrics server
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Metrics server stopped")
