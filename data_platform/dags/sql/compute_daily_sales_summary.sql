INSERT INTO agg_daily_sales (
    date_id,
    total_sales,
    total_quantity,
    transaction_count,
    unique_customers,
    avg_transaction_value,
    created_at
)
SELECT
    fs.date_id,
    SUM(fs.total_amount) AS total_sales,
    SUM(fs.quantity) AS total_quantity,
    COUNT(DISTINCT fs.sale_id) AS transaction_count,
    COUNT(DISTINCT fs.customer_id) AS unique_customers,
    AVG(fs.total_amount) AS avg_transaction_value,
    CURRENT_TIMESTAMP
FROM fact_sales fs
GROUP BY fs.date_id
ON CONFLICT (date_id)
DO UPDATE SET
    total_sales = EXCLUDED.total_sales,
    total_quantity = EXCLUDED.total_quantity,
    transaction_count = EXCLUDED.transaction_count,
    unique_customers = EXCLUDED.unique_customers,
    avg_transaction_value = EXCLUDED.avg_transaction_value,
    created_at = CURRENT_TIMESTAMP;
