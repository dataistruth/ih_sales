-- sql/gold/gold_sales_summary.sql
-- ============================================================
-- Monthly revenue summary per client, year, month, currency
-- Excludes refunds from revenue but counts them separately
-- ============================================================

SELECT
    f.client_id,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.quarter_label,
    f.currency,

    -- Transaction counts
    COUNT(*)                                                        AS total_transactions,
    COUNT(CASE WHEN f.is_refund = false OR f.is_refund IS NULL
               THEN 1 END)                                         AS sales_count,
    COUNT(CASE WHEN f.is_refund = true THEN 1 END)                 AS refund_count,

    -- Revenue metrics (excluding refunds)
    ROUND(SUM(CASE WHEN f.is_refund = false OR f.is_refund IS NULL
                   THEN f.gross_amount ELSE 0 END), 2)             AS gross_revenue,
    ROUND(SUM(CASE WHEN f.is_refund = false OR f.is_refund IS NULL
                   THEN f.net_amount  ELSE 0 END), 2)              AS net_revenue,

    -- Refund metrics
    ROUND(SUM(CASE WHEN f.is_refund = true
                   THEN f.gross_amount ELSE 0 END), 2)             AS refund_amount,

    -- Averages
    ROUND(AVG(CASE WHEN f.is_refund = false OR f.is_refund IS NULL
                   THEN f.gross_amount END), 2)                    AS avg_order_value,

    -- Discount metrics
    COUNT(CASE WHEN f.has_discount = true THEN 1 END)              AS discounted_orders,

    -- Unique customers
    COUNT(DISTINCT f.source_customer_id)                           AS unique_customers

FROM fact_transactions f
LEFT JOIN dim_date d
    ON f.date_sk = d.date_sk
WHERE d.year IS NOT NULL                                           -- exclude sentinel -1
GROUP BY
    f.client_id,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.quarter_label,
    f.currency
ORDER BY
    f.client_id,
    d.year,
    d.month,
    f.currency