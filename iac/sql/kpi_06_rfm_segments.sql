-- KPI 6: RFM-lite segmentation per account, from successful INR txns.
-- Recency = days from last txn to the as-of date (lower is better). Frequency = txn count.
-- Monetary = net value. Each measure is split into tertiles on PERCENT_RANK, so tied values always get
-- the same score. (NTILE splits ties arbitrarily, which changed the result between runs.) Score = R + F + M (3-9).
-- Segment: 8-9 Platinum, 6-7 Gold, 5 Silver, 3-4 Bronze. Accounts with no successful txn are not scored.
-- {as_of} is replaced by the pipeline run date.
WITH rfm AS (
    SELECT account_id,
           DATE_DIFF('day', MAX(txn_ts), TIMESTAMP '{as_of}') AS recency_days,
           COUNT(*) AS frequency,
           SUM(CASE WHEN is_refund THEN -amount ELSE amount END) AS monetary
    FROM v_fact
    WHERE status = 'SUCCESS' AND currency = 'INR'
    GROUP BY account_id
),
scored AS (
    SELECT *,
           CASE WHEN PERCENT_RANK() OVER (ORDER BY recency_days DESC) < 1.0 / 3 THEN 1
                WHEN PERCENT_RANK() OVER (ORDER BY recency_days DESC) < 2.0 / 3 THEN 2
                ELSE 3 END AS r_score,
           CASE WHEN PERCENT_RANK() OVER (ORDER BY frequency ASC) < 1.0 / 3 THEN 1
                WHEN PERCENT_RANK() OVER (ORDER BY frequency ASC) < 2.0 / 3 THEN 2
                ELSE 3 END AS f_score,
           CASE WHEN PERCENT_RANK() OVER (ORDER BY monetary ASC) < 1.0 / 3 THEN 1
                WHEN PERCENT_RANK() OVER (ORDER BY monetary ASC) < 2.0 / 3 THEN 2
                ELSE 3 END AS m_score
    FROM rfm
)
SELECT c.customer_id,
       s.account_id,
       s.recency_days,
       s.frequency,
       s.monetary,
       s.r_score + s.f_score + s.m_score AS rfm_score,
       CASE
           WHEN s.r_score + s.f_score + s.m_score >= 8 THEN 'Platinum'
           WHEN s.r_score + s.f_score + s.m_score >= 6 THEN 'Gold'
           WHEN s.r_score + s.f_score + s.m_score = 5 THEN 'Silver'
           ELSE 'Bronze'
       END AS segment
FROM scored s
LEFT JOIN dim_customer c ON c.account_id = s.account_id AND c.is_current
ORDER BY rfm_score DESC, monetary DESC, s.account_id;
