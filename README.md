# END TO END DATA PIPELINE EXAMPLE: Medallion Architecture with Docker Compose

## Overview

A production-grade, portfolio-ready data engineering platform demonstrating modern data stack practices. This project implements a complete medallion architecture (Bronze, Silver, Gold layers) with Apache Airflow orchestration, PySpark processing, and containerized infrastructure optimized for 16GB RAM machines.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     EXTERNAL DATA SOURCES                        │
│                (APIs, Databases, Files, Kafka)                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (MinIO)                         │
│              Raw data ingestion & archive layer                 │
│       Storage: S3-compatible (Parquet format, partitioned)      │
└────────────────────────┬────────────────────────────────────────┘
                         │
            ┌────────────┴────────────┐
            │  PySpark Transformation │
            │    (Data Cleaning)      │
            └────────────┬────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (MinIO/Delta)                     │
│         Cleaned, deduplicated, validated data ready for BI      │
│       Storage: Delta Lake (supports ACID transactions)          │
└────────────────────────┬────────────────────────────────────────┘
                         │
            ┌────────────┴────────────┐
            │  PySpark Aggregation    │
            │   (Star Schema Joins)   │
            └────────────┬────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (PostgreSQL)                        │
│              Star Schema for BI/Analytics (Fact + Dim)          │
│    - Fact Tables: Sales transactions, Events, Metrics           │
│    - Dimension Tables: Customers, Products, Dates               │
│    - Aggregation Tables: Pre-computed summaries                 │
└─────────────────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   ┌─────────┐   ┌──────────────┐   ┌──────────┐
   │  BI/SQL │   │    Python    │   │ Streaming│
   │ Queries │   │  Analytics   │   │  Apps    │
   └─────────┘   └──────────────┘   └──────────┘
```

## Tech Stack

| Component | Purpose | Technology |
|-----------|---------|-----------|
| **Orchestration** | Workflow scheduling & DAG management | Apache Airflow 2.7 |
| **Processing** | Distributed data processing | Apache Spark 3.4 (PySpark) |
| **Data Lake** | Raw/processed data storage | MinIO (S3-compatible) |
| **Warehouse** | Analytics database & star schema | PostgreSQL 15 |
| **Streaming** | Event streaming & CDC | Apache Kafka 7.5 |
| **Task Queue** | Distributed task execution | Celery with Redis |
| **Containerization** | Infrastructure & deployments | Docker + Docker Compose |
| **Data Formats** | Efficient storage | Parquet + Delta Lake |

## Prerequisites

### System Requirements
- **OS**: Linux (Lubuntu preferred)
- **RAM**: 16GB minimum
- **CPU**: 4 cores recommended
- **Disk Space**: 50GB free (for MinIO, PostgreSQL, logs)
- **Docker**: 20.10+
- **Docker Compose**: 2.0+

### Installation

```bash
# Install Docker & Docker Compose (if not already installed)
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Add current user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Verify installation
docker --version
docker-compose --version
```

## Project Structure

```
data_platform/
├── dags/                          # Airflow DAGs
│   ├── data_platform_medallion_pipeline.py
│   └── sql/
│       ├── compute_daily_sales_summary.sql
│       ├── compute_product_performance.sql
│       └── compute_customer_lifetime_value.sql
│
├── spark_jobs/                    # PySpark jobs
│   ├── 01_ingest_customers.py
│   ├── 01_ingest_products.py
│   ├── 01_ingest_orders.py
│   ├── 02_transform_customers.py
│   ├── 02_transform_products.py
│   ├── 02_transform_orders.py
│   └── 03_aggregate_to_warehouse.py
│
├── docker/                        # Docker configurations
│   └── Dockerfile.airflow
│
├── configs/                       # Application configs
│   ├── config.py
│   └── platform_config.yaml
│
├── scripts/                       # Utility scripts
│   ├── startup.sh                # Initialize platform
│   ├── cleanup.sh                # Cleanup/shutdown
│   ├── health_check.sh           # Monitor health
│   ├── generate_data.py          # Generate test data
│   └── init_db.sql               # Database schema
│
├── tests/                         # Testing & validation
│   ├── validate_data_quality.py
│   └── test_integration.py
│
├── data/                          # Test datasets (generated)
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
│
├── docker-compose.yml             # Container orchestration
├── requirements.txt               # Python dependencies
├── .env                          # Environment variables
├── .gitignore                    # Git configuration
└── README.md                     # This file
```

## Quick Start

### 1. Clone & Setup

```bash
cd /path/to/data_platform
chmod +x scripts/*.sh
```

### 2. Start Services

```bash
# Pull images (first time)
docker-compose pull

# Start all services
docker-compose up -d

# Monitor startup
docker-compose logs -f
```

### 3. Wait for Services

```bash
# Run health check
./scripts/health_check.sh

# Or manually check:
docker-compose ps
```

### 4. Initialize Data

```bash
# Generate test data
python scripts/generate_data.py

# Verify data was created
ls -lh data/
```

### 5. Access UIs

| Service | URL | Credentials |
|---------|-----|-----------|
| **Airflow** | http://localhost:8080 | admin / admin123 |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin_secure_pass |
| **Spark Master** | http://localhost:8081 | - |
| **PostgreSQL** | localhost:5432 | warehouse_user / warehouse_pass |

### 6. Run Pipeline

**In Airflow UI:**
1. Go to DAGs → `data_platform_medallion_pipeline`
2. Toggle the DAG to enabled (blue switch)
3. Click "Trigger DAG" (blue play button)
4. Monitor execution in the Graph view

**Or via CLI:**
```bash
docker-compose exec airflow_webserver airflow dags trigger data_platform_medallion_pipeline
```

## Pipeline Overview

### DAG: `data_platform_medallion_pipeline`

Executes daily with the following task flow:

```
pipeline_start
    │
    ├─► ingest_customers ──┐
    ├─► ingest_products   ├─► validate_bronze_data
    └─► ingest_orders     ┘
            │
            ├─► transform_customers_silver ─────┐
            ├─► transform_products_silver       ├─► aggregate_to_warehouse
            └─► transform_orders_silver         ┘
                    │
                    ├─► compute_daily_summary
                    ├─► compute_product_performance
                    └─► compute_customer_ltv
                            │
                            └─► pipeline_end
```

### Stage Details

#### Bronze Layer (Ingestion)
- **Tasks**: `ingest_customers`, `ingest_products`, `ingest_orders`
- **Input**: CSV files (generated or real sources)
- **Output**: Parquet files in MinIO `s3://bronze/`
- **Processing**: Raw data is loaded as-is with metadata
- **Partitioning**: By `ingestion_date`

#### Silver Layer (Transformation)
- **Tasks**: `transform_customers_silver`, `transform_products_silver`, `transform_orders_silver`
- **Input**: Bronze layer Parquet files
- **Output**: Delta Lake format in MinIO `s3://silver/`
- **Processing**:
  - Data validation & cleaning
  - Deduplication
  - Schema enforcement
  - Type conversions
- **Partitioning**: By business entity

#### Gold Layer (Aggregation)
- **Tasks**: `aggregate_to_warehouse`, `compute_*` aggregations
- **Input**: Silver layer Delta files
- **Output**: PostgreSQL star schema tables
- **Processing**:
  - Dimension & fact table joins
  - Business logic computations
  - Summary table generation

## Data Model

### Star Schema (PostgreSQL Gold Layer)

```
┌──────────────────────────────────────────────────────────────┐
│                      FACT_SALES (center)                     │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ sale_id (PK)                                            │ │
│  │ customer_id (FK) ──► dim_customers                      │ │
│  │ product_id (FK) ──► dim_products                        │ │
│  │ date_id (FK) ──► dim_dates                              │ │
│  │ quantity, unit_price, total_amount, discount_percent   │ │
│  │ net_amount, order_id, created_at                        │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘

┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  DIM_CUSTOMERS   │  │  DIM_PRODUCTS    │  │   DIM_DATES      │
├──────────────────┤  ├──────────────────┤  ├──────────────────┤
│ customer_id (PK) │  │ product_id (PK)  │  │ date_id (PK)     │
│ first_name       │  │ product_name     │  │ date             │
│ last_name        │  │ category         │  │ year, quarter    │
│ email            │  │ brand            │  │ month, day, week │
│ country, city    │  │ unit_price       │  │ day_name, is_wknd│
│ phone            │  │ updated_at       │  │ month_name       │
│ registration_date│  │                  │  │                  │
│ created_at       │  │                  │  │                  │
└──────────────────┘  └──────────────────┘  └──────────────────┘

AGGREGATION TABLES:
├─ agg_daily_sales (date_id: daily summary of sales)
├─ agg_product_performance (product_id: product metrics)
└─ agg_customer_lifetime_value (customer_id: CLV metrics)
```

### Table Definitions

**FACT_SALES**
- Grain: One row per order line item
- ~10M rows initially, grows daily
- Partitioned by date for query efficiency

**DIM_CUSTOMERS**
- Slowly changing dimension (SCD Type 1)
- ~1K unique customers
- Full outer join with fact table

**DIM_PRODUCTS**
- ~500 unique products
- Includes category & brand hierarchies
- Full outer join with fact table

**DIM_DATES**
- Conformed dimension (pre-populated for 5 years)
- Enables year-on-year comparisons
- Supports holiday/weekend analysis

## Example Queries

### Connect to PostgreSQL

```bash
psql -h localhost -U warehouse_user -d data_warehouse -W
# Password: warehouse_pass
```

### Analytics Queries

**1. Daily Revenue Trend**
```sql
SELECT 
    d.date,
    SUM(fs.total_amount) as revenue,
    COUNT(DISTINCT fs.customer_id) as unique_customers,
    COUNT(*) as transactions
FROM fact_sales fs
JOIN dim_dates d ON fs.date_id = d.date_id
WHERE d.year = 2024
GROUP BY d.date
ORDER BY d.date DESC;
```

**2. Top 10 Products by Revenue**
```sql
SELECT 
    dp.product_name,
    dp.category,
    SUM(fs.total_amount) as total_revenue,
    SUM(fs.quantity) as total_qty,
    COUNT(*) as transaction_count,
    ROUND(AVG(fs.total_amount), 2) as avg_order_value
FROM fact_sales fs
JOIN dim_products dp ON fs.product_id = dp.product_id
GROUP BY dp.product_id, dp.product_name, dp.category
ORDER BY total_revenue DESC
LIMIT 10;
```

**3. Customer Segmentation by Spending**
```sql
SELECT 
    CASE 
        WHEN clv.total_spent > 10000 THEN 'VIP'
        WHEN clv.total_spent > 5000 THEN 'Gold'
        WHEN clv.total_spent > 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as customer_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(clv.total_spent), 2) as avg_spend,
    ROUND(SUM(clv.total_spent), 2) as total_spend
FROM agg_customer_lifetime_value clv
GROUP BY customer_segment
ORDER BY customer_segment;
```

**4. Month-over-Month Growth**
```sql
WITH monthly_sales AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        SUM(fs.total_amount) as monthly_revenue
    FROM fact_sales fs
    JOIN dim_dates d ON fs.date_id = d.date_id
    GROUP BY d.year, d.month, d.month_name
)
SELECT 
    *,
    LAG(monthly_revenue) OVER (ORDER BY year, month) as prev_month_revenue,
    ROUND(100.0 * (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY year, month)) 
        / LAG(monthly_revenue) OVER (ORDER BY year, month), 2) as growth_percent
FROM monthly_sales
ORDER BY year DESC, month DESC;
```

**5. Cohort Analysis - Customer Retention**
```sql
SELECT 
    DATE_TRUNC('month', dc.registration_date)::date as cohort_date,
    DATE_TRUNC('month', d.date)::date as transaction_date,
    COUNT(DISTINCT fs.customer_id) as customers
FROM fact_sales fs
JOIN dim_customers dc ON fs.customer_id = dc.customer_id
JOIN dim_dates d ON fs.date_id = d.date_id
WHERE d.date >= dc.registration_date
GROUP BY 
    DATE_TRUNC('month', dc.registration_date),
    DATE_TRUNC('month', d.date)
ORDER BY cohort_date DESC, transaction_date DESC;
```

## Monitoring & Observability

### Logging Strategy

All components log to:
- **Airflow**: `/opt/airflow/logs/` (mounted volume)
- **Spark**: `/opt/spark/logs/` (Spark Master/Worker logs)
- **Docker**: Use `docker-compose logs <service>`

### Key Monitoring Points

```bash
# Airflow task logs
docker-compose logs airflow_scheduler | tail -50

# Spark job progress
curl http://localhost:8081/api/v1/applications

# MinIO bucket stats
docker-compose exec minio mc du minio/bronze
docker-compose exec minio mc du minio/silver

# PostgreSQL table sizes
psql -h localhost -U warehouse_user -d data_warehouse -c "
  SELECT schemaname, tablename, 
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) 
  FROM pg_tables 
  WHERE schemaname = 'public' 
  ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"

# Data quality metrics
SELECT * FROM data_quality_metrics 
ORDER BY check_timestamp DESC LIMIT 10;

# Pipeline execution history
SELECT * FROM pipeline_execution_log 
WHERE status = 'FAILED' LIMIT 5;
```

## Resource Optimization for 16GB RAM

### Memory Allocation

| Service | Memory | CPU | Notes |
|---------|--------|-----|-------|
| Airflow Webserver | 2GB | 2 | Scheduler + Web UI |
| Airflow Scheduler | 1GB | 1 | DAG parsing |
| Airflow Worker | 2GB | 2 | Task execution |
| Spark Master | 2GB | 1.5 | Cluster coordination |
| Spark Worker | 1.5GB | 1 | Executor node |
| PostgreSQL | 1GB | 1 | Warehouse DB |
| Redis | 512MB | - | Celery broker |
| MinIO | 1GB | 1 | Data lake |
| Kafka | 1GB | 1 | Message queue |
| Zookeeper | 512MB | 0.5 | Kafka coordination |
| **Total** | **~13.5GB** | **~10** | Stable configuration |

### Tuning for 16GB

```bash
# Set in docker-compose.yml:
- max_connections for PostgreSQL
- executor instances for Spark
- parallelism for Airflow
- log retention for MinIO/Kafka

# Monitor actual usage:
docker stats
```

## Best Practices Implemented

### 1. **Data Engineering**
- ✅ Medallion architecture (bronze/silver/gold)
- ✅ Schema enforcement with Spark
- ✅ Data partitioning for query performance
- ✅ Star schema for analytics
- ✅ Slowly Changing Dimensions (SCD)

### 2. **Code Quality**
- ✅ Modular PySpark jobs
- ✅ Error handling & retry logic
- ✅ Comprehensive logging
- ✅ Configuration management
- ✅ Type hints (Python)

### 3. **Infrastructure**
- ✅ Infrastructure as Code (Docker Compose)
- ✅ Environment variables for configuration
- ✅ Health checks for all services
- ✅ Volume management for persistence
- ✅ Network isolation

### 4. **Orchestration**
- ✅ DAG-based workflow management
- ✅ Task dependencies & SLAs
- ✅ Retry mechanisms
- ✅ Monitoring & alerts
- ✅ Idempotent operations

### 5. **Security**
- ✅ Separate database users per layer
- ✅ Credentials in environment variables
- ✅ Network isolation (Docker networks)
- ✅ Firewall rules (ports < 8000 internal only)

## Troubleshooting

### Services Won't Start

```bash
# Check Docker daemon
docker ps

# Check disk space
df -h

# View error logs
docker-compose logs

# Remove stopped containers
docker-compose down
docker system prune -a
```

### Airflow DAG Not Showing

```bash
# Restart scheduler
docker-compose restart airflow_scheduler

# Check DAGs folder
docker-compose exec airflow_webserver ls -la /opt/airflow/dags/

# Validate DAG syntax
docker-compose exec airflow_webserver airflow dags test data_platform_medallion_pipeline
```

### Spark Job Failures

```bash
# Check Spark Master UI: http://localhost:8081

# View job logs
docker-compose logs spark_master | grep ERROR

# Check available memory
docker stats spark_master spark_worker

# Check JDBC connector
docker-compose exec spark_master pip list | grep postgresql
```

### MinIO Access Issues

```bash
# Check MinIO health
curl http://localhost:9000/minio/health/live

# List buckets
docker-compose exec minio mc ls minio/

# Check MinIO logs
docker-compose logs minio | tail -50
```

### PostgreSQL Connection Issues

```bash
# Test connection
docker-compose exec postgres psql -U warehouse_user -d data_warehouse -c "SELECT 1;"

# Check databases
docker-compose exec postgres psql -U warehouse_user -l

# View logs
docker-compose logs postgres | tail -50
```

## Performance Tuning

### Query Optimization

```sql
-- Add indexes for frequently filtered columns
CREATE INDEX idx_fact_sales_customer_date 
ON fact_sales(customer_id, date_id);

-- Analyze table statistics
ANALYZE fact_sales;

-- Check query execution plan
EXPLAIN ANALYZE SELECT ... FROM fact_sales WHERE ...;
```

### Spark Optimization

```python
# Enable broadcast join for small dimensions
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10MB

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Partition optimization
df.repartition(numPartitions=8).write.parquet("path")
```

## Scaling Recommendations

### Horizontal Scaling (Adding Nodes)

```yaml
# Add more Spark workers
spark_worker_2:
    extends: spark_worker
    ports:
      - "8083:8080"

# Add more Airflow workers
airflow_worker_2:
    extends: airflow_worker
    container_name: caliweura_airflow_worker_2
```

### Vertical Scaling (Bigger Machine)

Update resource limits in `docker-compose.yml`:
```yaml
mem_limit: 4g  # from 2g
cpus: 2        # from 1
```

## Advanced Features

### Kafka Streaming Integration

```python
# Example: Consume events from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "events") \
    .load()
```

### Delta Lake Features

```python
# Time travel to previous version
delta_df = spark.read.format("delta").option("versionAsOf", 1).load("s3a://silver/orders")

# Show change data feed
df = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .load("path")
```

### Data Validation with Great Expectations

```python
from great_expectations.dataset import PandasDataset

# Create validation checkpoint
df_ge = PandasDataset(df)
df_ge.expect_column_values_to_be_in_set("status", ["active", "inactive"])
```

## Production Deployment

### Pre-Deployment Checklist

- [ ] Load test with production-like data volume
- [ ] Configure monitoring & alerting
- [ ] Set up backup/disaster recovery
- [ ] Document runbooks & escalation procedures
- [ ] Security audit & penetration testing
- [ ] Performance profiling & optimization
- [ ] Load testing during peak hours

### Deployment to Cloud

Example for **AWS ECS/Fargate**:

```bash
# Build images
docker-compose build

# Push to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com
docker tag data-platform:latest <account>.dkr.ecr.us-east-1.amazonaws.com/data-platform:latest
docker push <account>.dkr.ecr.us-east-1.amazonaws.com/data-platform:latest

# Deploy via CloudFormation or Terraform
terraform apply -var="image_uri=<ecr_image_uri>"
```

## Contributing

To extend this project:

1. **New Data Source**: Add ingestion job in `spark_jobs/01_ingest_*.py`
2. **New Transformation**: Create job in `spark_jobs/02_transform_*.py`
3. **New Analysis**: Add DAG task or SQL query in `dags/`
4. **Testing**: Add tests in `tests/` folder
5. **Documentation**: Update this README

## License

This project is provided as-is for educational and portfolio purposes.

## Support & Contact

For questions or issues:
- Review logs: `docker-compose logs <service>`
- Check health: `./scripts/health_check.sh`
- Read code comments for implementation details

---

**Last Updated**: 2024
**Version**: 1.0.0
**Status**: Production-Ready
