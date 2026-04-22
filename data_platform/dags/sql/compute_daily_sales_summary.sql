-- Compute daily sales summary
REFRESH MATERIALIZED VIEW CONCURRENTLY agg_daily_sales;

INSERT INTO agg_daily_sales
SELECT 
    fs.date_id,
    SUM(fs.total_amount) as total_sales,
    SUM(fs.quantity) as total_quantity,
    COUNT(DISTINCT fs.sale_id) as transaction_count,
    COUNT(DISTINCT fs.customer_id) as unique_customers,
    AVG(fs.total_amount) as avg_transaction_value,
    CURRENT_TIMESTAMP
FROM fact_sales fs
WHERE fs.date_id = (SELECT MAX(date_id) FROM dim_dates WHERE date = CURRENT_DATE)
GROUP BY fs.date_id
ON CONFLICT (date_id) 
DO UPDATE SET 
    total_sales = EXCLUDED.total_sales,
    total_quantity = EXCLUDED.total_quantity,
    transaction_count = EXCLUDED.transaction_count,
    unique_customers = EXCLUDED.unique_customers,
    avg_transaction_value = EXCLUDED.avg_transaction_value,
    created_at = CURRENT_TIMESTAMP;
