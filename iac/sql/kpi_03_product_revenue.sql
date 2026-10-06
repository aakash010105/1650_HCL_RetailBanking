-- KPI 3: Product net revenue per month, share of its category, share of total, and period-over-period %.
-- Revenue = successful INR, refunds subtracted. Category is taken from the product version at txn time.
WITH rev AS (
    SELECT p.category,
           f.product_id,
           date_trunc('month', f.transaction_date) AS month,
           SUM(CASE WHEN f.is_refund THEN -f.amount ELSE f.amount END) AS revenue
    FROM v_fact f
    JOIN dim_product p ON p.product_sk = f.product_sk
    WHERE f.status = 'SUCCESS' AND f.currency = 'INR'
    GROUP BY 1, 2, 3
)
SELECT category,
       product_id,
       month,
       revenue,
       ROUND(100.0 * revenue / NULLIF(SUM(revenue) OVER (PARTITION BY category, month), 0), 2) AS category_share_pct,
       ROUND(100.0 * revenue / NULLIF(SUM(revenue) OVER (PARTITION BY month), 0), 2) AS total_share_pct,
       LAG(revenue) OVER w AS prev_period_revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER w) / NULLIF(LAG(revenue) OVER w, 0), 2) AS pop_pct
FROM rev
WINDOW w AS (PARTITION BY product_id ORDER BY month)
ORDER BY month, category, revenue DESC;
