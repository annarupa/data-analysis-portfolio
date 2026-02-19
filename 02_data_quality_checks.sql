-- ============================================================
-- DATA QUALITY CHECKS: Reusable Validation Rules
-- Use Case: Production support checks run daily to catch
--           data issues before they reach downstream reports
-- Author: Anna Rupa Anthony
-- ============================================================


-- -------------------------------------------------------
-- CHECK 1: NULL / MISSING VALUES in critical fields
-- -------------------------------------------------------
SELECT
    'NULL_CHECK'        AS check_type,
    'transactions'      AS table_name,
    COUNT(*)            AS total_records,
    SUM(CASE WHEN partner_id        IS NULL THEN 1 ELSE 0 END) AS null_partner_id,
    SUM(CASE WHEN transaction_date  IS NULL THEN 1 ELSE 0 END) AS null_txn_date,
    SUM(CASE WHEN incentive_amount  IS NULL THEN 1 ELSE 0 END) AS null_amount,
    SUM(CASE WHEN program_code      IS NULL THEN 1 ELSE 0 END) AS null_program_code,
    SUM(CASE WHEN status            IS NULL THEN 1 ELSE 0 END) AS null_status
FROM transactions
WHERE transaction_date >= CAST(GETDATE() AS DATE);  -- today's load only


-- -------------------------------------------------------
-- CHECK 2: DUPLICATE RECORDS
-- -------------------------------------------------------
SELECT
    transaction_id,
    partner_id,
    transaction_date,
    COUNT(*) AS duplicate_count
FROM transactions
GROUP BY
    transaction_id,
    partner_id,
    transaction_date
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- -------------------------------------------------------
-- CHECK 3: REFERENTIAL INTEGRITY — orphaned transactions
-- (transactions with no matching partner record)
-- -------------------------------------------------------
SELECT
    t.transaction_id,
    t.partner_id,
    t.transaction_date,
    t.incentive_amount
FROM transactions t
LEFT JOIN partners p ON t.partner_id = p.partner_id
WHERE p.partner_id IS NULL
  AND t.transaction_date >= DATEADD(DAY, -1, GETDATE());


-- -------------------------------------------------------
-- CHECK 4: AMOUNT OUTLIERS (statistical anomaly detection)
-- Flags records where incentive_amount is beyond 3 std deviations
-- -------------------------------------------------------
WITH stats AS (
    SELECT
        program_code,
        AVG(incentive_amount)                    AS avg_amount,
        STDEV(incentive_amount)                  AS std_amount
    FROM transactions
    WHERE transaction_date >= DATEADD(MONTH, -3, GETDATE())
    GROUP BY program_code
)
SELECT
    t.transaction_id,
    t.partner_id,
    t.program_code,
    t.incentive_amount,
    s.avg_amount,
    s.std_amount,
    ROUND((t.incentive_amount - s.avg_amount) / NULLIF(s.std_amount, 0), 2) AS z_score
FROM transactions t
JOIN stats s ON t.program_code = s.program_code
WHERE
    ABS((t.incentive_amount - s.avg_amount) / NULLIF(s.std_amount, 0)) > 3
    AND t.transaction_date >= DATEADD(DAY, -1, GETDATE())
ORDER BY ABS((t.incentive_amount - s.avg_amount) / NULLIF(s.std_amount, 0)) DESC;


-- -------------------------------------------------------
-- CHECK 5: STATUS DISTRIBUTION — spot unexpected status values
-- -------------------------------------------------------
SELECT
    status,
    COUNT(*)    AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM transactions
WHERE transaction_date >= CAST(GETDATE() AS DATE)
GROUP BY status
ORDER BY record_count DESC;


-- -------------------------------------------------------
-- CHECK 6: DAILY VOLUME TREND — detect sudden drops (load failures)
-- -------------------------------------------------------
SELECT
    CAST(transaction_date AS DATE) AS load_date,
    COUNT(*)                        AS record_count,
    SUM(incentive_amount)           AS total_incentive_amount,
    LAG(COUNT(*), 1) OVER (ORDER BY CAST(transaction_date AS DATE))
                                    AS prev_day_count,
    COUNT(*) - LAG(COUNT(*), 1) OVER (ORDER BY CAST(transaction_date AS DATE))
                                    AS day_over_day_change
FROM transactions
WHERE transaction_date >= DATEADD(DAY, -14, GETDATE())
GROUP BY CAST(transaction_date AS DATE)
ORDER BY load_date DESC;
