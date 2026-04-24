INSERT INTO agg_customer_lifetime_value (customer_id, total_spent, total_transactions, avg_transaction_value, last_purchase_date, last_updated)
SELECT
    fs.customer_id,
    SUM(fs.total_amount) AS total_spent,
    COUNT(DISTINCT fs.sale_id) AS total_transactions,
    AVG(fs.total_amount) AS avg_transaction_value,
    MAX(d.date) as last_purchase_date,
    CURRENT_TIMESTAMP
FROM fact_sales fs
JOIN dim_dates d ON fs.date_id = d.date_id
WHERE fs.customer_id IS NOT NULL
GROUP BY fs.customer_id
ON CONFLICT (customer_id)
DO UPDATE SET
    total_spent = EXCLUDED.total_spent,
    total_transactions = EXCLUDED.total_transactions,
    avg_transaction_value = EXCLUDED.avg_transaction_value,
    last_purchase_date = EXCLUDED.last_purchase_date,
    last_updated = CURRENT_TIMESTAMP;
