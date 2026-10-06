-- KPI 10: Data quality scorecard per file and batch. Source: dq_scorecard (one row per file per batch).
-- top_reasons = the three most frequent error codes in that file's quarantine.
SELECT batch_id,
       entity,
       source_file,
       received,
       passed,
       rejected,
       ROUND(100.0 * rejected / NULLIF(received, 0), 2) AS reject_pct,
       top_reasons
FROM dq_scorecard
ORDER BY batch_id, entity;
