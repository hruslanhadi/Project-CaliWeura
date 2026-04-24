import sys

from pyspark.sql.functions import col, current_timestamp, date_format, row_number
from pyspark.sql.window import Window

from job_config import build_spark_session, configure_logger, get_warehouse_config, layer_path, load_runtime_config, should_run


logger = configure_logger(__name__)


def aggregate_to_warehouse():
    runtime = load_runtime_config()
    if not should_run("orders", "gold", runtime):
        logger.info("Skipping gold warehouse load for runtime=%s", runtime)
        return 0

    spark = build_spark_session("GoldLayerAggregation")
    warehouse = get_warehouse_config()
    jdbc_url = warehouse["jdbc_url"] or f"jdbc:postgresql://{warehouse['host']}:{warehouse['port']}/{warehouse['database']}"
    jdbc_properties = {
        "user": warehouse["user"],
        "password": warehouse["password"],
        "driver": "org.postgresql.Driver",
    }

    customers_df = spark.read.format("delta").load(layer_path("silver", "customers"))
    products_df = spark.read.format("delta").load(layer_path("silver", "products"))
    orders_df = spark.read.format("delta").load(layer_path("silver", "orders"))

    customer_window = Window.orderBy("external_customer_id")
    dim_customers = (
        customers_df.select(
            col("customer_id").alias("external_customer_id"),
            "first_name",
            "last_name",
            "email",
            "country",
            "city",
            "phone",
            "registration_date",
        )
        .distinct()
        .withColumn("customer_id", row_number().over(customer_window))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .select(
            "customer_id",
            "external_customer_id",
            "first_name",
            "last_name",
            "email",
            "country",
            "city",
            "phone",
            "registration_date",
            "created_at",
            "updated_at",
        )
    )

    product_window = Window.orderBy("external_product_id")
    dim_products = (
        products_df.select(
            col("product_id").alias("external_product_id"),
            "product_name",
            "category",
            "subcategory",
            "brand",
            "unit_price",
        )
        .distinct()
        .withColumn("product_id", row_number().over(product_window))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .select(
            "product_id",
            "external_product_id",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "unit_price",
            "created_at",
            "updated_at",
        )
    )

    orders_alias = orders_df.alias("orders")
    customer_lookup = dim_customers.select("customer_id", "external_customer_id").alias("customers")
    product_lookup = dim_products.select("product_id", "external_product_id").alias("products")

    fact_sales = (
        orders_alias.join(
            customer_lookup,
            col("orders.customer_id") == col("customers.external_customer_id"),
            "left",
        )
        .join(
            product_lookup,
            col("orders.product_id") == col("products.external_product_id"),
            "left",
        )
        .select(
            col("customers.customer_id").cast("int").alias("customer_id"),
            col("products.product_id").cast("int").alias("product_id"),
            date_format(col("orders.order_date"), "yyyyMMdd").cast("int").alias("date_id"),
            col("orders.quantity").alias("quantity"),
            col("orders.unit_price").alias("unit_price"),
            col("orders.total_amount").alias("total_amount"),
            col("orders.discount_percent").alias("discount_percent"),
            col("orders.net_amount").alias("net_amount"),
            col("orders.order_id").alias("order_id"),
            current_timestamp().alias("created_at"),
        )
    )

    dim_customers.write.option("truncate", "true").jdbc(
        jdbc_url, "public.dim_customers", mode="overwrite", properties=jdbc_properties
    )
    dim_products.write.option("truncate", "true").jdbc(
        jdbc_url, "public.dim_products", mode="overwrite", properties=jdbc_properties
    )
    fact_sales.write.option("truncate", "true").jdbc(
        jdbc_url, "public.fact_sales", mode="overwrite", properties=jdbc_properties
    )

    logger.info(
        "Warehouse load complete: customers=%s products=%s facts=%s",
        dim_customers.count(),
        dim_products.count(),
        fact_sales.count(),
    )
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(aggregate_to_warehouse())
