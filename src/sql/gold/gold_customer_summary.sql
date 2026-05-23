-- sql/gold/gold_customer_summary.sql
-- ============================================================
-- Customer lifetime value summary
-- One row per (client_id, customer_sk)
-- ============================================================

SELECT
    c.client_id,
    c.customer_sk,
    c.source_customer_id,
    c.full_name,
    c.email,
    c.country,
    c.age,
    c.age_band,
    c.vip_flag,
    c.marketing_opt_in,
    c.status,
    c.loyalty_points,
    c.signup_date,

    -- Transaction counts
    COUNT(f.transaction_sk)                                        AS total_transactions,
    COUNT(CASE WHEN f.is_refund = false OR f.is_refund IS NULL
               THEN 1 END)                                         AS total_purchases,
    COUNT(CASE WHEN f.is_refund = true THEN 1 END)                 AS total_refunds,

    -- Spend metrics
    ROUND(SUM(CASE WHEN f.is_refund = false OR f.is_refund IS NULL
                   THEN f.gross_amount ELSE 0 END), 2)             AS total_gross_spend,
    ROUND(SUM(CASE WHEN f.is_refund = false OR f.is_refund IS NULL
                   THEN f.net_amount   ELSE 0 END), 2)             AS total_net_spend,
    ROUND(AVG(CASE WHEN f.is_refund = false OR f.is_refund IS NULL
                   THEN f.gross_amount END), 2)                    AS avg_order_value,

    -- Engagement
    MIN(f.created_at)                                              AS first_transaction_date,
    MAX(f.created_at)                                              AS last_transaction_date,
    COUNT(CASE WHEN f.has_discount = true THEN 1 END)              AS discounted_orders,

    -- Active flag — has transacted in last 365 days
    CASE WHEN MAX(f.created_at) >= DATE_SUB(CURRENT_DATE(), 365)
         THEN true ELSE false END                                   AS is_active

FROM dim_customer c
LEFT JOIN fact_transactions f
    ON  c.customer_sk = f.customer_sk
    AND c.client_id   = f.client_id
GROUP BY
    c.client_id,
    c.customer_sk,
    c.source_customer_id,
    c.full_name,
    c.email,
    c.country,
    c.age,
    c.age_band,
    c.vip_flag,
    c.marketing_opt_in,
    c.status,
    c.loyalty_points,
    c.signup_date
ORDER BY
    c.client_id,
    total_gross_spend DESC