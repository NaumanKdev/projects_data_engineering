# Enterprise Data Warehouse Modernization

## Overview
Scalable cloud-native data warehouse built on Snowflake using star schema design, ELT pipelines, and modern DevOps practices. Improved BI dashboard performance by 40% through partitioning and query optimization strategies.

## Architecture
- **Data Layer**: Snowflake (Multi-cluster warehouse)
- **Orchestration**: dbt for transformations + Apache Airflow
- **Ingestion**: Fivetran/Stitch connectors + custom ELT
- **Reporting**: Tableau/Looker dashboards
- **Infrastructure**: AWS + Snowflake

## Key Features
✅ Star schema for dimensional modeling  
✅ Dynamic SQL optimization & materialized views  
✅ Auto-scaling compute resources  
✅ Role-based access control (RBAC)  
✅ Data lineage tracking  
✅ Incremental loading strategies  
✅ Query performance monitoring  

## Performance Improvements
- **40% faster BI queries** through partitioning
- **60% cost reduction** via clustering & materialized views
- **99.9% query availability** with failover strategies
- **Reduced ETL latency** by 45% with optimized SQL

## Tech Stack
- Snowflake
- dbt (Data Build Tool)
- Python
- SQL
- Airflow
- Terraform
- Git + CI/CD

## Project Structure
```
├── dbt/                        # dbt project
│   ├── models/                 # Dimensional models
│   ├── macros/                 # Reusable SQL macros
│   └── tests/                  # Data validation tests
├── sql/                        # Manual SQL transformations
├── airflow/                    # Orchestration DAGs
├── terraform/                  # Infrastructure
└── dashboards/                 # Analytics configs
```
