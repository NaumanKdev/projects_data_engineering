"""
Snowflake Data Warehouse ETL Pipeline
Orchestrates data ingestion, transformation, and loading into star schema
"""

import logging
from typing import Dict, List, Optional
from snowflake.connector import connect
from snowflake.connector.errors import ProgrammingError
import pandas as pd
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowflakeWarehouse:
    """Manage Snowflake warehouse connections and operations"""
    
    def __init__(self, config: Dict[str, str]):
        """Initialize Snowflake connection"""
        self.config = config
        self.connection = None
        self.connect()
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.connection = connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema']
            )
            logger.info(f"Connected to Snowflake - {self.config['account']}")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List:
        """Execute SQL query"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            return results
        except ProgrammingError as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_update(self, query: str) -> int:
        """Execute INSERT/UPDATE/DELETE"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            rows_affected = cursor.rowcount
            self.connection.commit()
            cursor.close()
            return rows_affected
        except ProgrammingError as e:
            logger.error(f"Update failed: {e}")
            self.connection.rollback()
            raise
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, mode: str = 'append'):
        """Load pandas DataFrame to Snowflake"""
        try:
            cursor = self.connection.cursor()
            cursor.write_pandas(df, table_name, auto_create_table=(mode=='create'))
            logger.info(f"Loaded {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"DataFrame load failed: {e}")
            raise
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()


class StarSchemaBuilder:
    """Build and maintain star schema dimensions and facts"""
    
    def __init__(self, warehouse: SnowflakeWarehouse):
        self.warehouse = warehouse
    
    def create_dimension_date(self):
        """Create date dimension table"""
        query = """
        CREATE TABLE IF NOT EXISTS DIM_DATE (
            DATE_KEY INT PRIMARY KEY,
            DATE_VALUE DATE,
            YEAR INT,
            QUARTER INT,
            MONTH INT,
            DAY INT,
            WEEK INT,
            DAY_OF_WEEK VARCHAR(10),
            IS_WEEKEND BOOLEAN,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """
        self.warehouse.execute_update(query)
        logger.info("Created DIM_DATE dimension")
    
    def create_dimension_customer(self):
        """Create customer dimension table"""
        query = """
        CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
            CUSTOMER_KEY INT PRIMARY KEY AUTOINCREMENT,
            CUSTOMER_ID VARCHAR(50) UNIQUE NOT NULL,
            FIRST_NAME VARCHAR(100),
            LAST_NAME VARCHAR(100),
            EMAIL VARCHAR(100),
            COUNTRY VARCHAR(50),
            STATE VARCHAR(50),
            SEGMENT VARCHAR(50),
            LIFETIME_VALUE DECIMAL(15,2),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """
        self.warehouse.execute_update(query)
        logger.info("Created DIM_CUSTOMER dimension")
    
    def create_dimension_product(self):
        """Create product dimension table"""
        query = """
        CREATE TABLE IF NOT EXISTS DIM_PRODUCT (
            PRODUCT_KEY INT PRIMARY KEY AUTOINCREMENT,
            PRODUCT_ID VARCHAR(50) UNIQUE NOT NULL,
            PRODUCT_NAME VARCHAR(255),
            CATEGORY VARCHAR(100),
            SUBCATEGORY VARCHAR(100),
            PRICE DECIMAL(10,2),
            COST DECIMAL(10,2),
            ACTIVE BOOLEAN,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
        """
        self.warehouse.execute_update(query)
        logger.info("Created DIM_PRODUCT dimension")
    
    def create_fact_sales(self):
        """Create fact sales table"""
        query = """
        CREATE TABLE IF NOT EXISTS FACT_SALES (
            SALES_KEY INT PRIMARY KEY AUTOINCREMENT,
            CUSTOMER_KEY INT NOT NULL,
            PRODUCT_KEY INT NOT NULL,
            DATE_KEY INT NOT NULL,
            QUANTITY INT,
            UNIT_PRICE DECIMAL(10,2),
            TOTAL_AMOUNT DECIMAL(15,2),
            DISCOUNT_AMOUNT DECIMAL(10,2),
            NET_AMOUNT DECIMAL(15,2),
            TAX_AMOUNT DECIMAL(10,2),
            ORDER_ID VARCHAR(50),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (CUSTOMER_KEY) REFERENCES DIM_CUSTOMER(CUSTOMER_KEY),
            FOREIGN KEY (PRODUCT_KEY) REFERENCES DIM_PRODUCT(PRODUCT_KEY),
            FOREIGN KEY (DATE_KEY) REFERENCES DIM_DATE(DATE_KEY)
        );
        """
        self.warehouse.execute_update(query)
        logger.info("Created FACT_SALES table")
    
    def create_clustering_key(self):
        """Create clustering key for query optimization"""
        query = """
        ALTER TABLE FACT_SALES CLUSTER BY (CUSTOMER_KEY, DATE_KEY);
        """
        try:
            self.warehouse.execute_update(query)
            logger.info("Applied clustering to FACT_SALES")
        except:
            logger.warning("Clustering already applied or not supported")


class SnowflakeDimensionManagement:
    """Manage dimensions with advanced techniques"""
    
    def __init__(self, warehouse: SnowflakeWarehouse):
        self.warehouse = warehouse
        self.logger = logger

class SnowflakeDimensionManagement:
    """Manage dimensions with advanced techniques including SCD Type 2"""
    
    def __init__(self, warehouse: SnowflakeWarehouse):
        self.warehouse = warehouse
        self.logger = logger
    
    def implement_scd_type_2(self, staging_table: str, dimension_table: str, 
                            key_column: str, changing_columns: list):
        """Implement Slowly Changing Dimension Type 2 for tracking history"""
        query = f"""
        -- SCD Type 2 Merge Logic
        MERGE INTO {dimension_table} t
        USING (
            SELECT 
                {key_column},
                {', '.join(changing_columns)},
                CURRENT_TIMESTAMP() as effective_date,
                NULL as end_date,
                TRUE as is_current
            FROM {staging_table}
        ) s
        ON t.{key_column} = s.{key_column} AND t.is_current = TRUE
        WHEN MATCHED AND {' OR '.join([f't.{col} != s.{col}' for col in changing_columns])} THEN
            UPDATE SET 
                t.end_date = CURRENT_TIMESTAMP(),
                t.is_current = FALSE
        WHEN NOT MATCHED THEN
            INSERT (
                {key_column}, {', '.join(changing_columns)}, 
                effective_date, end_date, is_current
            )
            VALUES (
                s.{key_column}, {', '.join([f's.{col}' for col in changing_columns])},
                s.effective_date, s.end_date, s.is_current
            );
        """
        self.warehouse.execute_update(query)
        self.logger.info(f"Applied SCD Type 2 to {dimension_table}")
    
    def create_conformed_dimension(self, dimension_name: str, source_table: str):
        """Create conformed dimension for enterprise consistency"""
        query = f"""
        CREATE OR REPLACE TABLE {dimension_name}_CONFORMED AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY {dimension_name}_ID) as SURROGATE_KEY,
            {dimension_name}_ID,
            *,
            CURRENT_TIMESTAMP() as CREATED_AT,
            NULL as DEPRECATED_AT
        FROM {source_table}
        WHERE IS_ACTIVE = TRUE;
        """
        self.warehouse.execute_update(query)
        self.logger.info(f"Created conformed dimension: {dimension_name}_CONFORMED")


class SnowflakeLETPipeline:
    """ELT pipeline for Snowflake"""
    
    def __init__(self, warehouse: SnowflakeWarehouse):
        self.warehouse = warehouse
    
    def load_raw_customers(self, source_data: pd.DataFrame) -> int:
        """Load raw customer data with incremental CDC patterns"""
        staging_table = "STG_CUSTOMERS"
        try:
            self.warehouse.load_dataframe(source_data, staging_table, mode='create')
            logger.info(f"Loaded {len(source_data)} raw customer records")
            
            # Add change data capture columns
            cdc_query = f"""
            ALTER TABLE {staging_table} ADD COLUMN IF NOT EXISTS CDC_OPERATION VARCHAR(1);
            ALTER TABLE {staging_table} ADD COLUMN IF NOT EXISTS CDC_TIMESTAMP TIMESTAMP;
            
            UPDATE {staging_table} 
            SET CDC_OPERATION = 'I', CDC_TIMESTAMP = CURRENT_TIMESTAMP()
            WHERE CDC_OPERATION IS NULL;
            """
            self.warehouse.execute_update(cdc_query)
            
            return len(source_data)
        except Exception as e:
            logger.error(f"Failed to load raw customers: {e}")
            raise
    
    def transform_customers(self) -> int:
        """Transform and load customers to dimension"""
        query = """
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
        FROM STG_CUSTOMERS
        WHERE CUSTOMER_ID NOT IN (SELECT CUSTOMER_ID FROM DIM_CUSTOMER);
        """
        rows = self.warehouse.execute_update(query)
        logger.info(f"Transformed {rows} customer records")
        return rows
    
    def load_raw_sales(self, source_data: pd.DataFrame) -> int:
        """Load raw sales data"""
        staging_table = "STG_SALES"
        try:
            self.warehouse.load_dataframe(source_data, staging_table, mode='create')
            logger.info(f"Loaded {len(source_data)} raw sales records")
            return len(source_data)
        except Exception as e:
            logger.error(f"Failed to load raw sales: {e}")
            raise
    
    def transform_sales(self) -> int:
        """Transform and load sales to fact table"""
        query = """
        INSERT INTO FACT_SALES 
            (CUSTOMER_KEY, PRODUCT_KEY, DATE_KEY, QUANTITY, UNIT_PRICE, 
             TOTAL_AMOUNT, DISCOUNT_AMOUNT, NET_AMOUNT, TAX_AMOUNT, ORDER_ID)
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
        FROM STG_SALES s
        LEFT JOIN DIM_CUSTOMER dc ON s.CUSTOMER_ID = dc.CUSTOMER_ID
        LEFT JOIN DIM_PRODUCT dp ON s.PRODUCT_ID = dp.PRODUCT_ID
        LEFT JOIN DIM_DATE dd ON TO_DATE(s.SALE_DATE) = dd.DATE_VALUE
        WHERE s.ORDER_ID NOT IN (SELECT ORDER_ID FROM FACT_SALES WHERE ORDER_ID IS NOT NULL);
        """
        rows = self.warehouse.execute_update(query)
        logger.info(f"Transformed {rows} sales records")
        return rows


class PerformanceOptimization:
    """Query performance optimization"""
    
    def __init__(self, warehouse: SnowflakeWarehouse):
        self.warehouse = warehouse
    
    def create_materialized_view_sales_by_customer(self):
        """Create materialized view for sales by customer"""
        query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS MV_SALES_BY_CUSTOMER AS
        SELECT 
            c.CUSTOMER_ID,
            c.FIRST_NAME,
            c.LAST_NAME,
            COUNT(DISTINCT f.SALES_KEY) as TOTAL_TRANSACTIONS,
            SUM(f.NET_AMOUNT) as TOTAL_REVENUE,
            AVG(f.NET_AMOUNT) as AVG_TRANSACTION,
            MAX(f.CREATED_AT) as LAST_PURCHASE_DATE
        FROM FACT_SALES f
        JOIN DIM_CUSTOMER c ON f.CUSTOMER_KEY = c.CUSTOMER_KEY
        GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME;
        """
        self.warehouse.execute_update(query)
        logger.info("Created materialized view: MV_SALES_BY_CUSTOMER")
    
    def create_materialized_view_daily_sales(self):
        """Create materialized view for daily sales metrics"""
        query = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS MV_DAILY_SALES AS
        SELECT 
            d.DATE_VALUE,
            d.YEAR,
            d.QUARTER,
            d.MONTH,
            COUNT(DISTINCT f.SALES_KEY) as TRANSACTION_COUNT,
            COUNT(DISTINCT f.CUSTOMER_KEY) as UNIQUE_CUSTOMERS,
            SUM(f.NET_AMOUNT) as TOTAL_REVENUE,
            AVG(f.NET_AMOUNT) as AVG_TRANSACTION_VALUE
        FROM FACT_SALES f
        JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY
        GROUP BY d.DATE_VALUE, d.YEAR, d.QUARTER, d.MONTH;
        """
        self.warehouse.execute_update(query)
        logger.info("Created materialized view: MV_DAILY_SALES")
    
    def analyze_query_performance(self) -> pd.DataFrame:
        """Analyze query performance statistics"""
        query = """
        SELECT 
            QUERY_ID,
            USER_NAME,
            QUERY_TEXT,
            EXECUTION_TIME,
            QUEUE_TIME,
            COMPILATION_TIME,
            ROWS_PRODUCED,
            ROWS_SCANNED,
            START_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
        ORDER BY EXECUTION_TIME DESC
        LIMIT 100;
        """
        results = self.warehouse.execute_query(query)
        df = pd.DataFrame(results)
        logger.info("Query performance analysis completed")
        return df


def main():
    """Main pipeline execution"""
    config = {
        'user': 'your_user',
        'password': 'your_password',
        'account': 'xy12345.us-east-1',
        'warehouse': 'COMPUTE_WH',
        'database': 'ANALYTICS',
        'schema': 'PUBLIC'
    }
    
    warehouse = SnowflakeWarehouse(config)
    
    try:
        # Build star schema
        builder = StarSchemaBuilder(warehouse)
        builder.create_dimension_date()
        builder.create_dimension_customer()
        builder.create_dimension_product()
        builder.create_fact_sales()
        builder.create_clustering_key()
        
        # Run ELT pipeline
        pipeline = SnowflakeLETPipeline(warehouse)
        
        # Load sample data
        sample_customers = pd.DataFrame({
            'CUSTOMER_ID': ['C001', 'C002'],
            'FIRST_NAME': ['John', 'Jane'],
            'LAST_NAME': ['Doe', 'Smith'],
            'EMAIL': ['john@example.com', 'jane@example.com'],
            'COUNTRY': ['USA', 'USA'],
            'LIFETIME_VALUE': [150000, 75000]
        })
        
        pipeline.load_raw_customers(sample_customers)
        pipeline.transform_customers()
        
        # Optimize performance
        optimizer = PerformanceOptimization(warehouse)
        optimizer.create_materialized_view_sales_by_customer()
        optimizer.create_materialized_view_daily_sales()
        
        logger.info("Pipeline execution completed successfully")
        
    finally:
        warehouse.close()


if __name__ == "__main__":
    main()
