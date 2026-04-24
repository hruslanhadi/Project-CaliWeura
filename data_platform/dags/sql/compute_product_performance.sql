INSERT INTO agg_product_performance (product_id, total_sales, total_quantity, transaction_count, unique_customers, last_updated)
SELECT
    fs.product_id,
    SUM(fs.total_amount) AS total_sales,
    SUM(fs.quantity) AS total_quantity,
    COUNT(DISTINCT fs.sale_id) AS transaction_count,
    COUNT(DISTINCT fs.customer_id) AS unique_customers,
    CURRENT_TIMESTAMP
FROM fact_sales fs
WHERE fs.product_id IS NOT NULL
GROUP BY fs.product_id
ON CONFLICT (product_id)
DO UPDATE SET
    total_sales = EXCLUDED.total_sales,
    total_quantity = EXCLUDED.total_quantity,
    transaction_count = EXCLUDED.transaction_count,
    unique_customers = EXCLUDED.unique_customers,
    last_updated = CURRENT_TIMESTAMP;
