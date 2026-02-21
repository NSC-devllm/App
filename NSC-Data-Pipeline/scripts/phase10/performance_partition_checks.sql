-- Phase 10 performance and partition checks
-- Replace ${catalog} with target catalog name.

-- 1) Partition row counts (recent 7 days)
SELECT date_kst, COUNT(*) AS row_cnt
FROM ${catalog}.gold.fact_payment_anonymized
WHERE date_kst >= date_sub(current_date(), 7)
GROUP BY date_kst
ORDER BY date_kst DESC;

-- 2) Reconciliation drift distribution
SELECT date_kst,
       COUNT(*) AS user_cnt,
       MAX(drift_abs) AS max_drift_abs,
       AVG(drift_abs) AS avg_drift_abs
FROM ${catalog}.gold.recon_daily_snapshot_flow
WHERE date_kst >= date_sub(current_date(), 7)
GROUP BY date_kst
ORDER BY date_kst DESC;

-- 3) Exception severity trend
SELECT date_kst, severity, COUNT(*) AS cnt
FROM ${catalog}.gold.exception_ledger
WHERE date_kst >= date_sub(current_date(), 7)
GROUP BY date_kst, severity
ORDER BY date_kst DESC, severity;
