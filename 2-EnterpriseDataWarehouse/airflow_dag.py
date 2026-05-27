# -*- coding: utf-8 -*-
"""
Apache Airflow DAG for Snowflake ETL Pipeline
Orchestrates data loading, transformation, and optimization
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# DAG configuration
default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email': ['data-team@company.com'],
}

dag = DAG(
    'snowflake_etl_pipeline',
    default_args=default_args,
    description='Enterprise Data Warehouse ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
)

# Snowflake connection config
SNOWFLAKE_CONN_ID = 'snowflake_warehouse'

# Load raw customers
load_raw_customers = SnowflakeOperator(
    task_id='load_raw_customers',
    sql="""
    CREATE TEMPORARY TABLE STG_CUSTOMERS_TEMP AS
    SELECT * FROM {{ external_function('load_customers') }};
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

# Transform customers to dimension
transform_customers = SnowflakeOperator(
    task_id='transform_customers',
    sql="""
    INSERT INTO DIM_CUSTOMER (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, COUNTRY, SEGMENT)
    SELECT 
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        COUNTRY,
        CASE 
            WHEN LIFETIME_VALUE > 100000 THEN 'VIP'
            WHEN LIFETIME_VALUE > 50000 THEN 'PREMIUM'
            ELSE 'STANDARD'
        END as SEGMENT
    FROM STG_CUSTOMERS_TEMP
    WHERE CUSTOMER_ID NOT IN (SELECT CUSTOMER_ID FROM DIM_CUSTOMER);
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

# Load raw sales
load_raw_sales = SnowflakeOperator(
    task_id='load_raw_sales',
    sql="""
    CREATE TEMPORARY TABLE STG_SALES_TEMP AS
    SELECT * FROM {{ external_function('load_sales') }};
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

# Transform sales to fact table
transform_sales = SnowflakeOperator(
    task_id='transform_sales',
    sql="""
    INSERT INTO FACT_SALES 
    SELECT 
        dc.CUSTOMER_KEY,
        dp.PRODUCT_KEY,
        dd.DATE_KEY,
        s.QUANTITY,
        s.UNIT_PRICE,
        s.QUANTITY * s.UNIT_PRICE as TOTAL_AMOUNT,
        s.DISCOUNT_AMOUNT,
        (s.QUANTITY * s.UNIT_PRICE) - s.DISCOUNT_AMOUNT as NET_AMOUNT,
        s.TAX_AMOUNT,
        s.ORDER_ID
    FROM STG_SALES_TEMP s
    LEFT JOIN DIM_CUSTOMER dc ON s.CUSTOMER_ID = dc.CUSTOMER_ID
    LEFT JOIN DIM_PRODUCT dp ON s.PRODUCT_ID = dp.PRODUCT_ID
    LEFT JOIN DIM_DATE dd ON TO_DATE(s.SALE_DATE) = dd.DATE_VALUE
    WHERE s.ORDER_ID NOT IN (SELECT ORDER_ID FROM FACT_SALES WHERE ORDER_ID IS NOT NULL);
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

# Refresh materialized views
refresh_mv_sales_customer = SnowflakeOperator(
    task_id='refresh_mv_sales_customer',
    sql="ALTER MATERIALIZED VIEW MV_SALES_BY_CUSTOMER REFRESH;",
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

refresh_mv_daily_sales = SnowflakeOperator(
    task_id='refresh_mv_daily_sales',
    sql="ALTER MATERIALIZED VIEW MV_DAILY_SALES REFRESH;",
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

# Data quality checks
data_quality_check = SnowflakeOperator(
    task_id='data_quality_check',
    sql="""
    SELECT 
        CASE 
            WHEN COUNT(*) = 0 THEN 'FAIL'
            ELSE 'PASS'
        END as quality_check
    FROM FACT_SALES
    WHERE CREATED_AT >= CURRENT_DATE
        AND CUSTOMER_KEY IS NOT NULL
        AND PRODUCT_KEY IS NOT NULL
        AND NET_AMOUNT > 0;
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    dag=dag,
)

# Task dependencies
[load_raw_customers, load_raw_sales] >> transform_customers >> transform_sales >> [refresh_mv_sales_customer, refresh_mv_daily_sales] >> data_quality_check
