-- 事業者(オフィス)マスタ: office_id ごとの属性情報
-- 元テーブル: raw.office
-- 想定用途: 売上・コストとの結合、オフィス属性での分析

CREATE OR REPLACE TABLE master.office AS
WITH base AS (
    SELECT
        o.office_id,
        o.office_code,
        o.office_name,
        o.office_name_kana,
        o.industry_code,
        o.industry_name,
        o.country_code,
        o.prefecture,
        o.city,
        o.address_line1,
        o.address_line2,
        o.postal_code,
        o.contract_status,        -- active, canceled, trial など
        o.contract_start_date,
        o.contract_end_date,
        o.sales_owner_id,
        o.created_at,
        o.updated_at
    FROM
        raw.office AS o
),
normalized AS (
    SELECT
        office_id,
        office_code,
        office_name,
        office_name_kana,
        industry_code,
        industry_name,
        country_code,
        prefecture,
        city,
        address_line1,
        address_line2,
        postal_code,
        contract_status,
        contract_start_date,
        contract_end_date,
        sales_owner_id,
        created_at,
        updated_at,
        -- 契約終了日が NULL の場合は 2099-12-31 などを仮終了日にするなどもあり
        CASE
            WHEN contract_status = 'active' AND contract_end_date IS NULL
                THEN DATE '2099-12-31'
            ELSE contract_end_date
        END AS contract_end_date_effective
    FROM
        base
)
SELECT
    office_id,
    office_code,
    office_name,
    office_name_kana,
    industry_code,
    industry_name,
    country_code,
    prefecture,
    city,
    address_line1,
    address_line2,
    postal_code,
    contract_status,
    contract_start_date,
    contract_end_date,
    contract_end_date_effective,
    sales_owner_id,
    created_at,
    updated_at
FROM
    normalized;
