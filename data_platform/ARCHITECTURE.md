# Architecture Deep Dive

## System Components

### 1. Apache Airflow
**Role**: Workflow orchestration and scheduling
- **Webserver**: REST API and UI (port 8080)
- **Scheduler**: Parses DAGs and schedules tasks
- **Celery Worker**: Executes tasks with parallelism
- **Redis**: Message broker for Celery
- **PostgreSQL**: Metadata store

**Key Concepts**:
- DAG (Directed Acyclic Graph): Workflow definition
- Task: Individual unit of work (PySpark job, SQL query, Python function)
- Operator: Task implementation (SparkSubmitOperator, PostgresOperator, PythonOperator)
- XCom: Communication between tasks
- SLA: Service Level Agreement / timeout

**Configuration**:
```python
default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}
```

### 2. Apache Spark
**Role**: Distributed data processing engine
- **Master Node**: Cluster coordinator (port 7077)
- **Worker Nodes**: Execute tasks in parallel
- **Driver**: Client process that creates RDD/DataFrames
- **Executors**: JVM processes that run tasks

**Architecture**:
```
Driver (1 per job)
    ├── Stages (based on shuffles)
    │   ├── Task 1 → Executor 1
    │   ├── Task 2 → Executor 2
    │   └── Task 3 → Executor 3
    └── RDD/DataFrame transformations
```

**Key Optimizations**:
- Partitioning: Distribute data across executors
- Caching: Keep intermediate results in memory
- Broadcast: Send small data to all executors
- Columnar Storage: Parquet/Delta format

### 3. MinIO (Data Lake)
**Role**: S3-compatible object storage for data lake
- **Endpoint**: `minio:9000`
- **Console**: `minio:9001`
- **Buckets**: `bronze`, `silver`, `gold`

**Structure**:
```
s3://bronze/
  ├── customers/
  │   ├── ingestion_date=2024-01-01/
  │   │   ├── part-0.parquet
  │   │   └── part-1.parquet
  │   └── ingestion_date=2024-01-02/
  ├── products/
  └── orders/

s3://silver/
  ├── customers/ (Delta format)
  ├── products/
  └── orders/

s3://gold/
  └── aggregates/
```

### 4. PostgreSQL (Data Warehouse)
**Role**: Relational database for structured analytics
- **Port**: 5432
- **Databases**: `airflow` (metadata), `data_warehouse` (analytics)
- **Schemas**: `public` (facts/dimensions)

**Connection String**:
```
postgresql://warehouse_user:warehouse_pass@postgres:5432/data_warehouse
```

### 5. Kafka (Streaming Platform)
**Role**: Event streaming and message queue
- **Broker**: `kafka:9092`
- **Zookeeper**: `zookeeper:2181`
- **Topics**: `events`, `orders`, `customers`

**Streaming Integration**:
```python
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .load()
```

---

## Data Flow & Transformations

### Bronze → Silver Transformations

**Customers**:
```
Input: Raw CSV with duplicates, nulls, inconsistent formats
├─ Filter: Remove null customer_id
├─ Trim: Remove whitespace
├─ Standardize: Lowercase email, proper case names
├─ Dedup: Keep latest record per customer_id
└─ Output: Delta Lake with data quality score
```

**Products**:
```
Input: Raw product catalog
├─ Filter: unit_price > 0
├─ Clean: Trim strings, lowercase categories
├─ Dedup: Latest version per product_id
├─ Enrich: Add product_status field
└─ Output: Delta Lake with active flag
```

**Orders**:
```
Input: Transaction log with potential issues
├─ Validate: Required fields not null
├─ Calculate: Derive net_amount from discount
├─ Partition: By year/month for efficiency
├─ Dedup: Handle accidental duplicates
└─ Output: Delta Lake partitioned dataset
```

### Silver → Gold Transformations

**Fact Table Creation**:
```
fact_sales = orders.join(dim_customers).join(dim_products)
├─ Map external IDs to surrogate keys
├─ Join dimension tables
├─ Calculate derived columns
├─ Aggregate to grain: one row per line item
└─ Load to PostgreSQL with indices
```

**Dimension Table Creation**:
```
dim_dates (pre-populated):
├─ Generate series from 2020-2030
├─ Calculate: year, quarter, month, day_name
├─ Mark: weekends, holidays
├─ Purpose: Enable time-based analytics

dim_customers (from bronze):
├─ Surrogate key: auto-increment
├─ External ID: customer_id
├─ SCD Type 1: overwrite old values
├─ Grain: one row per customer

dim_products (from bronze):
├─ Surrogate key: auto-increment
├─ Slowly changing: category changes
├─ Hierarchy: category → subcategory
├─ Grain: one row per product
```

### Aggregation Tables

**agg_daily_sales**:
```sql
SELECT 
    date_id,
    SUM(total_amount) as daily_revenue,
    COUNT(*) as transaction_count,
    ...
GROUP BY date_id
-- Purpose: Quick daily dashboards without full fact table scan
```

**agg_product_performance**:
```sql
SELECT 
    product_id,
    SUM(quantity) as total_units_sold,
    SUM(total_amount) as total_revenue,
    COUNT(DISTINCT customer_id) as unique_buyers,
    ...
GROUP BY product_id
-- Purpose: Product-level BI without joining all facts
```

**agg_customer_lifetime_value**:
```sql
SELECT 
    customer_id,
    SUM(total_amount) as lifetime_value,
    COUNT(*) as transaction_count,
    MAX(date) as last_purchase_date,
    ...
GROUP BY customer_id
-- Purpose: Customer segmentation and retention analysis
```

---

## Configuration Management

### Environment Variables (.env)

```bash
# Database
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://...

# S3/MinIO
MINIO_ENDPOINT=minio:9000
AWS_ACCESS_KEY_ID=minioadmin

# Spark
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=1g

# Kafka
KAFKA_BROKERS=kafka:9092
```

### Application Config (config.py)

```python
# Centralized configuration
DATABASE_ENGINE = "postgresql"
SPARK_MASTER = "spark://spark_master:7077"
MINIO_BUCKETS = ['bronze', 'silver', 'gold']
```

### Platform Config (platform_config.yaml)

```yaml
# YAML-based declarative configuration
data_layers:
  bronze:
    path: s3://bronze/
    format: parquet
  silver:
    path: s3://silver/
    format: delta
  gold:
    path: postgresql://
    format: table
```

---

## Error Handling & Resilience

### Retry Mechanism (Airflow)

```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

### Data Validation

```python
# Spark: Schema validation
try:
    df = spark.read.schema(EXPECTED_SCHEMA).parquet(path)
except AnalysisException as e:
    logger.error(f"Schema validation failed: {e}")
    # Handle error
```

### Error Recovery

```python
# Idempotent writes (overwrite mode)
df.write.mode("overwrite").parquet(path)

# Transaction support (Delta Lake)
delta_df.write.format("delta").mode("overwrite").save(path)
```

---

## Performance Characteristics

### Expected Data Volume

- **Bronze Layer**: ~1GB (raw, uncompressed)
- **Silver Layer**: ~200MB (compressed with Parquet)
- **Gold Layer**: ~50MB (aggregated facts)
- **Growth**: +10% monthly

### Query Performance (Gold Layer)

| Query | Data Range | Typical Time | Index Used |
|-------|-----------|--------------|-----------|
| Daily sales | 1 day | <100ms | date_id |
| Monthly report | 30 days | <500ms | date_id, customer_id |
| Customer analytics | 1 year | <2s | customer_id |
| Full scan | 2 years | <10s | table partitioning |

### Memory Usage

- **Spark Job (1 executor)**: 1-2GB
- **Airflow Webserver**: 300-500MB
- **PostgreSQL**: 200-300MB (in-memory cache)

---

## Security & Authentication

### Data Warehouse Access

```bash
# Connect as analytics user (read-only)
psql -h postgres -U analytics_user -d data_warehouse

# Connect as admin
psql -h postgres -U warehouse_user -d data_warehouse
```

### S3/MinIO Access

```python
# Configure boto3
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin_secure_pass'
)
```

### Network Isolation

```yaml
# All services on isolated network
networks:
  data_platform_network:
    driver: bridge
```

---

## Monitoring & Alerting

### Key Metrics

1. **Pipeline Health**
   - DAG success rate
   - Average task duration
   - Task failure rate

2. **Data Quality**
   - Record counts (bronze vs silver vs gold)
   - Null/duplicate rates
   - Schema validation failures

3. **Infrastructure**
   - CPU & Memory usage
   - Disk space (MinIO, PostgreSQL)
   - Network I/O

4. **Query Performance**
   - Query execution time (P50, P95, P99)
   - Slow query count
   - Index efficiency

### Example Alert Rules

```python
# Data freshness alert
if (current_time - last_gold_load_time) > timedelta(hours=25):
    send_alert("Data not updated in 24+ hours")

# Memory alert
if spark_executor_memory > spark_memory_limit * 0.9:
    send_alert("Spark memory critical")

# Failed tasks alert
if airflow_task_failure_count > 5:
    send_alert("Multiple task failures detected")
```

---

## Disaster Recovery

### Backup Strategy

```bash
# Backup PostgreSQL
docker-compose exec postgres pg_dump -U warehouse_user -d data_warehouse > backup.sql

# Backup MinIO
docker-compose exec minio mc mirror minio/bronze local/backup/

# Backup Airflow DAGs
tar -czf airflow_dags_backup.tar.gz dags/
```

### Recovery Procedures

```bash
# Restore PostgreSQL
psql -U warehouse_user -d data_warehouse < backup.sql

# Restore MinIO
docker-compose exec minio mc mirror local/backup/ minio/bronze

# Restore DAGs
tar -xzf airflow_dags_backup.tar.gz -C .
```

---

## Integration Patterns

### Real-Time Streaming

```python
# Kafka → Bronze → Silver → Gold (real-time)
kafka_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()
```

### Batch + Streaming Hybrid

```python
# Combine batch fact table with streaming events
batch_facts = spark.read.format("delta").load("bronze_path")
streaming_events = spark.readStream.format("kafka").load()

combined = batch_facts.union(streaming_events)
```

### External API Integration

```python
# Pull data from external API
response = requests.get("https://api.example.com/data")
api_df = spark.createDataFrame(response.json())
api_df.write.parquet("s3a://bronze/external_api/")
```

---

**Document Version**: 1.0
**Last Updated**: 2024
**Audience**: Data Engineers, Data Analysts, DevOps Engineers
