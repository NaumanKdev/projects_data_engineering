# Enterprise Data Warehouse - Advanced Design Patterns

## Star Schema Deep Dive

### Dimensional Modeling Strategy

```
Facts:
├── FACT_SALES (Grain: One row per transaction)
├── FACT_INVENTORY (Grain: One row per SKU per day)
└── FACT_CUSTOMER_METRICS (Grain: One row per customer per month)

Dimensions:
├── DIM_CUSTOMER (SCD Type 2 - tracks history)
├── DIM_PRODUCT (SCD Type 1 - overwrites)
├── DIM_DATE (Conformed - shared across all facts)
├── DIM_LOCATION (SCD Type 2)
└── DIM_TIME_OF_DAY (Conformed - hours/minutes/seconds)
```

### Slowly Changing Dimensions (SCD)

**SCD Type 2 - Track History**:
```sql
-- Before: 2024-01-01
John Doe, Premium Segment

-- After: 2024-06-01 (upgrade event)
-- Version 1 (ended)
John Doe, Premium, effective_date: 2024-01-01, end_date: 2024-06-01, is_current: FALSE

-- Version 2 (current)
John Doe, VIP, effective_date: 2024-06-01, end_date: NULL, is_current: TRUE
```

**Implementation**:
- Effective date tracking
- Surrogate keys for fact table linkage
- Current flag for filtering latest records
- Supports full audit trail

### Query Optimization Techniques

**1. Aggregation Tables (Pre-computed)**
```sql
-- SALES_AGGS_DAILY: Pre-computed daily metrics
SELECT 
    DATE_ID,
    CUSTOMER_SEGMENT,
    PRODUCT_CATEGORY,
    SUM(SALES_AMOUNT) as daily_total_sales,
    COUNT(DISTINCT ORDER_ID) as daily_order_count,
    AVG(ORDER_VALUE) as daily_avg_order
FROM FACT_SALES
GROUP BY DATE_ID, CUSTOMER_SEGMENT, PRODUCT_CATEGORY
```

**2. Materialized Views with Auto-Refresh**
```sql
CREATE MATERIALIZED VIEW MV_TOP_100_CUSTOMERS AS
SELECT 
    CUSTOMER_ID,
    SUM(NET_AMOUNT) as lifetime_value,
    COUNT(DISTINCT ORDER_DATE) as purchase_days,
    MAX(ORDER_DATE) as last_purchase_date
FROM FACT_SALES
GROUP BY CUSTOMER_ID
ORDER BY lifetime_value DESC
LIMIT 100;
```

**3. Dynamic Clustering**
```sql
ALTER TABLE FACT_SALES 
CLUSTER BY (CUSTOMER_KEY, DATE_KEY, PRODUCT_KEY);
```

### Query Cost Analysis

```
Query Pattern Analysis:
1. High-volume queries
   - Filtered by date range first
   - Cost: O(n) where n = days queried
   
2. Dimension joins
   - Use surrogate keys (integers)
   - Cost: O(1) lookups
   
3. Aggregations
   - Pre-computed in gold layer
   - Cost: O(1) table scans
```

## ELT Pipeline Architecture

### Incremental Load Strategy

```
Initial Load (Full):
├─ Extract all records from source
├─ Load to staging table
├─ Transform and validate
└─ Insert to dimension/fact tables

Subsequent Loads (Incremental):
├─ Extract changed records only
│  └─ CDC (Change Data Capture) from source
├─ Load to staging table
├─ Identify new/updated/deleted
├─ For dimensions: Apply SCD logic
└─ For facts: UPSERT or INSERT only
```

### Data Lineage Tracking

```python
# Every transformation tracks:
- Source table
- Target table
- Transformation rules applied
- Timestamp
- Data quality checks
- Row counts (before/after)
```

## Advanced Performance Features

### Partition Pruning
```sql
-- Only scans 2024 data
SELECT * FROM FACT_SALES 
WHERE YEAR = 2024 AND MONTH = 6
```

### Compression Strategy
```
Snowflake Auto-Compression:
- Text: 7-9x reduction
- Numeric: 4-5x reduction
- Overall: 15-20x average
```

### Query Result Caching
```
Cache Hierarchy:
1. Metadata cache (instant)
2. Result cache (24 hour TTL)
3. Remote disk cache (weeks)
4. Full table scan (when needed)
```

## Conformed Dimensions

**Global Dimensions** (shared across facts):
```sql
DIM_DATE: Enterprise-wide date dimension
- Grain: One row per day
- Attributes: Day, Week, Month, Quarter, Year, Holidays, Fiscal periods
- Usage: Every fact table

DIM_TIME_OF_DAY: Hour/minute/second breakdown
- Used for precise time analysis
- Pre-computed for performance
```

## Incremental Loading Patterns

### Pattern 1: Changed Data Capture (CDC)
```sql
-- Source system provides CDC
INSERT INTO STG_INCREMENTAL_DATA
SELECT * FROM SOURCE_SYSTEM_CDC
WHERE CAPTURE_TIME >= LAST_RUN_TIME

-- Then merge into dimensions/facts
```

### Pattern 2: Surrogate Key Updates
```sql
-- Track surrogate key generation
CREATE OR REPLACE SURROGATE_KEY_SEQUENCE
START 1000000 INCREMENT BY 1
```

### Pattern 3: Upsert Operations
```sql
MERGE INTO DIM_CUSTOMER t
USING STG_CUSTOMER s
ON t.CUSTOMER_ID = s.CUSTOMER_ID
WHEN MATCHED AND t.HASH != s.HASH THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```

## Cost Optimization Strategies

**1. Query Pruning**
- Filter early in WHERE clause
- Reduces scanned data by 90%+

**2. Aggregation Pushdown**
- GROUP BY before UNION
- Reduces data transfer

**3. Dynamic Partitioning**
```
Daily partitions for last 90 days
Monthly partitions for older data
Archived to cheaper storage after 2 years
```

**4. Auto-Clustering**
- Snowflake automatically clusters new data
- Improves query speed 40-60%

## Monitoring & Maintenance

**Query Performance Metrics**:
```
- Execution time trend
- Partition scans
- Cache hit rate (target: > 70%)
- Bytes scanned (cost driver)
- Query queue time
```

**Table Maintenance**:
```
Daily:
- Clustering score analysis
- Missing statistics detection

Weekly:
- Vacuum old deleted records
- Analyze table growth

Monthly:
- Archive old partitions
- Cost analysis
```
