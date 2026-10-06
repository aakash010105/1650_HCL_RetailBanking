-- KPI 11 (Day 2): Day-over-day incremental reconciliation.
-- Counts come from batch_changes (written from each batch audit). KPI 1 and KPI 7 totals come from
-- kpi_snapshot (one snapshot per batch), and delta_vs_prev compares each batch with the batch before it.
WITH snap AS (
    SELECT batch_id, kpi, SUM(metric) AS total
    FROM kpi_snapshot
    GROUP BY batch_id, kpi
),
snap_delta AS (
    SELECT batch_id, kpi, total,
           total - LAG(total) OVER (PARTITION BY kpi ORDER BY batch_id) AS delta_vs_prev
    FROM snap
)
SELECT bc.batch_id,
       bc.fact_inserted AS new_txns,
       bc.fact_updated AS updated_txns,
       bc.fact_corrected AS corrected_txns,
       bc.customers_new,
       bc.customers_changed,
       bc.products_changed,
       bc.branches_changed,
       (SELECT total FROM snap_delta d WHERE d.batch_id = bc.batch_id AND d.kpi = 'kpi_01_top5_net') AS kpi01_top5_net,
       (SELECT delta_vs_prev FROM snap_delta d WHERE d.batch_id = bc.batch_id AND d.kpi = 'kpi_01_top5_net') AS kpi01_delta,
       (SELECT total FROM snap_delta d WHERE d.batch_id = bc.batch_id AND d.kpi = 'kpi_07_branch_net') AS kpi07_network_net,
       (SELECT delta_vs_prev FROM snap_delta d WHERE d.batch_id = bc.batch_id AND d.kpi = 'kpi_07_branch_net') AS kpi07_delta
FROM batch_changes bc
ORDER BY bc.batch_id;
