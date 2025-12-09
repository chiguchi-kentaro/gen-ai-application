-- 売上マート: office_id × product_id × target_month × plan の売上集計
-- 元テーブル: raw.revenue
-- 想定用途: 売上推移、MRR集計のベース

CREATE OR REPLACE TABLE mart.revenue AS
WITH base AS (
    SELECT
        r.office_id,
        r.product_id,
        r.sales_date,
        DATE_TRUNC(r.sales_date, MONTH) AS target_month,
        r.amount_ex_tax          AS revenue_amount,
        r.tax_amount             AS tax_amount,
        r.currency_code          AS currency,
        r.invoice_id,
        r.billing_status
    FROM
        raw.revenue AS r
    WHERE
        -- 課金確定している売上のみを採用
        r.billing_status = 'billed'
),
agg AS (
    SELECT
        office_id,
        product_id,
        target_month
        SUM(revenue_amount)          AS total_revenue_amount,
        SUM(tax_amount)              AS total_tax_amount,
        SUM(revenue_amount + tax_amount) AS total_revenue_gross,
        COUNT(DISTINCT invoice_id)   AS invoice_count
    FROM
        base
    GROUP BY
        office_id,
        product_id,
        target_month
)
SELECT
    office_id,
    product_id,
    target_month,
    total_revenue_amount,
    total_tax_amount,
    total_revenue_gross,
    invoice_count
FROM
    agg;
