# 📊 Data Platform Project Map

## Complete Folder Structure

```
/media/hanafiahrh/DataAll/Project/Project-CaliWeura/data_platform/
├── 📄 docker-compose.yml          ⭐ Main orchestration file (10 services)
├── 📄 requirements.txt             Python dependencies
├── 📄 .env                         Environment variables
├── 📄 .gitignore                   Git ignore rules
│
├── 📖 README.md                    ⭐ Complete documentation (500+ lines)
├── 📖 QUICKSTART.md                60-second setup guide
├── 📖 ARCHITECTURE.md              Deep technical dive
│
├── 🐳 docker/
│   └── 📄 Dockerfile.airflow       Airflow + PySpark image
│
├── 🔄 dags/                        Airflow DAG definitions
│   ├── 📄 data_platform_medallion_pipeline.py    ⭐ Main pipeline DAG
│   └── sql/
│       ├── 📄 compute_daily_sales_summary.sql
│       ├── 📄 compute_product_performance.sql
│       └── 📄 compute_customer_lifetime_value.sql
│
├── ⚡ spark_jobs/                  PySpark transformation jobs
│   ├── 📄 01_ingest_customers.py              Bronze: Customers ingestion
│   ├── 📄 01_ingest_products.py               Bronze: Products ingestion
│   ├── 📄 01_ingest_orders.py                 Bronze: Orders ingestion
│   ├── 📄 02_transform_customers.py           Silver: Customer transformation
│   ├── 📄 02_transform_products.py            Silver: Product transformation
│   ├── 📄 02_transform_orders.py              Silver: Order transformation
│   └── 📄 03_aggregate_to_warehouse.py        Gold: Warehouse loading
│
├── ⚙️ configs/
│   ├── 📄 config.py                Application configuration
│   └── 📄 platform_config.yaml     Platform configuration
│
├── 🛠️ scripts/
│   ├── 📄 startup.sh               ⭐ Initialize platform
│   ├── 📄 cleanup.sh               Stop and remove containers
│   ├── 📄 health_check.sh          Monitor service health
│   ├── 📄 generate_data.py         ⭐ Generate test data (1K customers, 500 products, 10K orders)
│   └── 📄 init_db.sql              ⭐ PostgreSQL schema initialization
│
├── 🧪 tests/
│   ├── 📄 test_integration.py      Integration tests
│   └── 📄 validate_data_quality.py Data quality validation
│
├── 💾 data/                        Test datasets (generated at runtime)
│   ├── 📄 customers.csv
│   ├── 📄 products.csv
│   └── 📄 orders.csv
│
└── 📦 volumes/ (created at runtime)
    ├── postgres_data/              PostgreSQL persistent storage
    ├── minio_data/                 MinIO object storage
    ├── airflow_logs/               Airflow task logs
    ├── spark_logs/                 Spark execution logs
    └── kafka_data/                 Kafka message storage
```

## Services Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DOCKER COMPOSE STACK                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  🟦 ORCHESTRATION                                                    │
│  ├─ airflow_webserver (2GB, 2 CPU)  → UI: http://localhost:8080     │
│  ├─ airflow_scheduler (1GB, 1 CPU)  → DAG Parser & Scheduler         │
│  ├─ airflow_worker (2GB, 2 CPU)     → Task Executor (Celery)         │
│  ├─ redis (512MB)                   → Celery Message Broker          │
│  │                                                                    │
│  🟩 PROCESSING                                                      │
│  ├─ spark_master (2GB, 1.5 CPU)     → UI: http://localhost:8081     │
│  └─ spark_worker (1.5GB, 1 CPU)     → Data Processing Node           │
│                                                                       │
│  🟨 STORAGE                                                          │
│  ├─ postgres (1GB, 1 CPU)           → Warehouse DB                   │
│  ├─ minio (1GB, 1 CPU)              → Data Lake (Console: :9001)     │
│  │                                                                    │
│  🟧 STREAMING                                                        │
│  ├─ zookeeper (512MB, 0.5 CPU)      → Kafka Coordination             │
│  └─ kafka (1GB, 1 CPU)              → Message Queue                  │
│                                                                       │
│  TOTAL: ~13.5GB RAM, ~10 CPU cores ✅ Optimized for 16GB machine    │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Pipeline Flow

```
                    📊 DATA PIPELINE EXECUTION
                                                    
  Daily Trigger (00:00 UTC)
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Raw Data Ingestion)                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1️⃣ ingest_customers      2️⃣ ingest_products      3️⃣ ingest_orders
│  ↓                         ↓                       ↓               │
│  Read CSV/API  →  Spark  →  MinIO (s3://bronze/*)               │
│  1000 records      Process    Parquet, partitioned               │
│                               by ingestion_date                  │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
            🔍 DATA VALIDATION
            ├─ Null checks
            ├─ Type validation
            └─ Record count verification
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│ SILVER LAYER (Cleaned & Transformed)                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  4️⃣ transform_customers    5️⃣ transform_products               │
│  ├─ Dedup by customer_id    ├─ Dedup by product_id             │
│  ├─ Validate email          ├─ Filter price > 0                 │
│  ├─ Standardize names       └─ Add product_status              │
│  └─ → MinIO (Delta Lake)    → MinIO (Delta Lake)               │
│                                                                  │
│  6️⃣ transform_orders                                           │
│  ├─ Calculate net_amount (with discount)                       │
│  ├─ Validate transaction data                                   │
│  └─ → MinIO (Delta Lake, partitioned by year/month)            │
│                                                                  │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│ GOLD LAYER (Aggregated Analytics)                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  7️⃣ aggregate_to_warehouse                                     │
│  ├─ Load dim_customers  → PostgreSQL                            │
│  ├─ Load dim_products   → PostgreSQL                            │
│  ├─ Load dim_dates      (pre-populated)                         │
│  └─ Load fact_sales     → PostgreSQL                            │
│      (Join silver + dimensions + business logic)                │
│                                                                  │
│  8️⃣ compute_daily_summary        (PostgreSQL)                  │
│  9️⃣ compute_product_performance   (PostgreSQL)                 │
│  🔟 compute_customer_ltv          (PostgreSQL)                  │
│                                                                  │
│  ✅ COMPLETE                                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                  │
                  ▼
            📈 READY FOR BI
            ├─ Dashboards (Tableau, Power BI)
            ├─ SQL Queries
            ├─ Python Analytics
            └─ ML Models
```

## Star Schema (PostgreSQL Gold Layer)

```
                        FACT_SALES (Center)
                    ┌────────────────────┐
                    │   ~10K rows/day    │
        ┌───────────┤  Partitioned by    ├───────────┐
        │           │    customer_id,    │           │
        │           │   product_id,      │           │
        │           │   date_id          │           │
        │           └────────────────────┘           │
        │                                             │
        ▼                                             ▼
┌──────────────────┐                    ┌──────────────────────┐
│  DIM_CUSTOMERS   │                    │   DIM_PRODUCTS       │
│  (1K rows)       │                    │   (500 rows)         │
├──────────────────┤                    ├──────────────────────┤
│ customer_id (PK) │                    │ product_id (PK)      │
│ first_name       │                    │ product_name         │
│ last_name        │                    │ category             │
│ email            │                    │ subcategory          │
│ country, city    │                    │ brand                │
│ registration     │                    │ unit_price           │
│ _date            │                    │                      │
└──────────────────┘                    └──────────────────────┘

        ▲
        │
        └────────────────┐
                         │
                    ┌─────────────────┐
                    │  DIM_DATES      │
                    │  (1826 rows)    │
                    ├─────────────────┤
                    │ date_id (PK)    │
                    │ date            │
                    │ year, quarter   │
                    │ month, day      │
                    │ day_name        │
                    │ is_weekend      │
                    └─────────────────┘

AGGREGATION TABLES (Denormalized for Speed):
├─ agg_daily_sales (365 rows/year)
├─ agg_product_performance (500 rows)
└─ agg_customer_lifetime_value (1K rows)
```

## File Statistics

| Component | Files | Lines | Purpose |
|-----------|-------|-------|---------|
| **DAGs** | 1 | 300 | Orchestration |
| **Spark Jobs** | 7 | 700 | Data Processing |
| **Docker** | 1 | 200 | Infrastructure |
| **Scripts** | 5 | 400 | Utilities |
| **SQL** | 4 | 200 | Database Schema |
| **Configuration** | 3 | 150 | App Config |
| **Documentation** | 4 | 1500 | Guides & Docs |
| **Tests** | 2 | 100 | Validation |
| **Total** | 27 | 3,550 | Production-Ready |

## Technology Versions

| Technology | Version | Image |
|-----------|---------|-------|
| Apache Airflow | 2.7 | apache/airflow:2.7 |
| Apache Spark | 3.4 | apache/spark:3.4 |
| Apache Kafka | 7.5 | confluentinc/cp-kafka:7.5 |
| PostgreSQL | 15 | postgres:15-alpine |
| MinIO | latest | minio/minio:latest |
| Python | 3.11 | Built-in |
| Redis | 7 | redis:7-alpine |
| Zookeeper | 7.5 | confluentinc/cp-zookeeper:7.5 |

## Quick Reference Commands

```bash
# 🚀 Startup
docker-compose up -d
./scripts/generate_data.py
./scripts/startup.sh

# 🔍 Monitoring
docker-compose logs -f airflow_scheduler
./scripts/health_check.sh

# 🧪 Testing
docker-compose exec airflow_webserver python tests/test_integration.py
docker-compose exec spark_master spark-submit spark_jobs/01_ingest_customers.py

# 📊 Data Access
psql -h localhost -U warehouse_user -d data_warehouse -W
# Query results in PostgreSQL tables

# 📦 MinIO Access
# Console: http://localhost:9001
# Buckets: bronze, silver, gold

# 🛑 Cleanup
./scripts/cleanup.sh
docker system prune -a

# 🔧 Configuration
vim .env
vim configs/config.py
vim docker-compose.yml
```

## Success Criteria ✅

- [x] Docker Compose configuration (10 services)
- [x] Medallion architecture (Bronze/Silver/Gold)
- [x] Airflow orchestration with DAG
- [x] PySpark jobs (ingestion, transformation, aggregation)
- [x] PostgreSQL data warehouse with star schema
- [x] MinIO data lake (S3-compatible)
- [x] Kafka streaming platform
- [x] Test data generator
- [x] Health check & monitoring
- [x] Comprehensive documentation
- [x] Optimized for 16GB RAM
- [x] Production-ready code quality

---

**Project Status**: ✅ COMPLETE & PRODUCTION-READY

**Ready to Deploy**: Start with [QUICKSTART.md](QUICKSTART.md)
