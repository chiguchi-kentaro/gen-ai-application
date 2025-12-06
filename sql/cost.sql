-- コストマート: office_id × target_month のコスト集計
-- 元テーブル: raw.cost
-- 想定用途: 売上マートと組み合わせて粗利・利益率を計算

CREATE OR REPLACE TABLE mart.cost AS
WITH base AS (
    SELECT
        c.office_id,
        c.cost_date,
        DATE_TRUNC(c.cost_date, MONTH) AS target_month,
        c.cost_type,        -- personnel, infra, marketing など
        c.amount            AS cost_amount,
        c.currency_code     AS currency,
        c.cost_center_id,
        c.memo,
        c.created_at
    FROM
        raw.cost AS c
    WHERE
        -- 将来的に「未承認コストを除外」などの条件を入れてもよい
        c.is_deleted = FALSE
),
agg_by_type AS (
    SELECT
        office_id,
        target_month,
        cost_type,
        SUM(cost_amount) AS cost_amount
    FROM
        base
    GROUP BY
        office_id,
        target_month,
        cost_type
),
agg_total AS (
    SELECT
        office_id,
        target_month,
        SUM(cost_amount) AS total_cost_amount
    FROM
        agg_by_type
    GROUP BY
        office_id,
        target_month
)
SELECT
    t.office_id,
    t.target_month,
    t.total_cost_amount,
    -- コスト区分別の金額を横持ちしたい場合の例
    SUM(CASE WHEN bt.cost_type = 'personnel' THEN bt.cost_amount ELSE 0 END) AS personnel_cost_amount,
    SUM(CASE WHEN bt.cost_type = 'infra'     THEN bt.cost_amount ELSE 0 END) AS infra_cost_amount,
    SUM(CASE WHEN bt.cost_type = 'marketing' THEN bt.cost_amount ELSE 0 END) AS marketing_cost_amount
FROM
    agg_total AS t
    LEFT JOIN agg_by_type AS bt
        ON t.office_id    = bt.office_id
       AND t.target_month = bt.target_month
GROUP BY
    t.office_id,
    t.target_month,
    t.total_cost_amount;
