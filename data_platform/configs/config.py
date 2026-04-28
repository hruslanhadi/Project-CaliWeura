# Airflow Configuration

# Database Configuration
DATABASE_ENGINE = "postgresql"
DATABASE_HOST = "postgres"
DATABASE_USER = "airflow"
DATABASE_PASSWORD = "airflow_secure_pass"
DATABASE_PORT = 5432
DATABASE_NAME = "airflow"

# MinIO Configuration
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin_secure_pass"
MINIO_USE_SSL = False

# PostgreSQL Warehouse
PG_WAREHOUSE_HOST = "postgres"
PG_WAREHOUSE_USER = "warehouse_user"
PG_WAREHOUSE_PASSWORD = "warehouse_pass"
PG_WAREHOUSE_PORT = 5432
PG_WAREHOUSE_DB = "data_warehouse"

# Spark Configuration
SPARK_MASTER = "spark://spark_master:7077"
# SPARK_MASTER = "spark://caliweura_spark_master:7077"
SPARK_DRIVER_MEMORY = "1g"
SPARK_EXECUTOR_MEMORY = "1g"
SPARK_EXECUTOR_CORES = 2
SPARK_EXECUTOR_INSTANCES = 2

# Kafka Configuration
KAFKA_BROKERS = "kafka:9092"
KAFKA_CONSUMER_GROUP = "data_platform"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
