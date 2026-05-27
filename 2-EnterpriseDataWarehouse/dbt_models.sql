"""
dbt models for Snowflake star schema
"""

-- dbt/models/staging/stg_customers.sql
SELECT 
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    COUNTRY,
    STATE,
    PHONE,
    CREATED_AT,
    UPDATED_AT,
    CURRENT_TIMESTAMP() as DBT_LOADED_AT
FROM RAW.CUSTOMERS
WHERE DELETED_AT IS NULL

-- dbt/models/staging/stg_products.sql
SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    SUBCATEGORY,
    PRICE,
    COST,
    ACTIVE,
    CREATED_AT,
    UPDATED_AT,
    CURRENT_TIMESTAMP() as DBT_LOADED_AT
FROM RAW.PRODUCTS

-- dbt/models/marts/fct_sales.sql
SELECT 
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as SALES_KEY,
    s.ORDER_ID,
    c.CUSTOMER_KEY,
    p.PRODUCT_KEY,
    d.DATE_KEY,
    s.QUANTITY,
    s.UNIT_PRICE,
    s.QUANTITY * s.UNIT_PRICE as TOTAL_AMOUNT,
    COALESCE(s.DISCOUNT_AMOUNT, 0) as DISCOUNT_AMOUNT,
    (s.QUANTITY * s.UNIT_PRICE) - COALESCE(s.DISCOUNT_AMOUNT, 0) as NET_AMOUNT,
    COALESCE(s.TAX_AMOUNT, 0) as TAX_AMOUNT,
    s.CREATED_AT
FROM {{ ref('stg_sales') }} s
LEFT JOIN {{ ref('dim_customer') }} c USING (CUSTOMER_ID)
LEFT JOIN {{ ref('dim_product') }} p USING (PRODUCT_ID)
LEFT JOIN {{ ref('dim_date') }} d ON TO_DATE(s.SALE_DATE) = d.DATE_VALUE

-- dbt/models/marts/dim_customer.sql
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} as CUSTOMER_KEY,
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    COUNTRY,
    STATE,
    CASE 
        WHEN SUM(NET_AMOUNT) OVER (PARTITION BY CUSTOMER_ID) > 100000 THEN 'VIP'
        WHEN SUM(NET_AMOUNT) OVER (PARTITION BY CUSTOMER_ID) > 50000 THEN 'PREMIUM'
        ELSE 'STANDARD'
    END as SEGMENT,
    CREATED_AT,
    UPDATED_AT
FROM {{ ref('stg_customers') }}

-- dbt/models/marts/dim_product.sql
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['PRODUCT_ID']) }} as PRODUCT_KEY,
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    SUBCATEGORY,
    PRICE,
    COST,
    ACTIVE,
    CREATED_AT
FROM {{ ref('stg_products') }}
