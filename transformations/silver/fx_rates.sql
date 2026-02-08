-- =========================================
-- Silver FX Rates Transformation
-- Purpose: Create clean, deduplicated FX rates
-- Source: raw_fx_rates
-- Layer: Silver
-- =========================================

BEGIN;

TRUNCATE TABLE silver_fx_rates;

INSERT INTO silver_fx_rates (
    rate_date,
    base_currency,
    quote_currency,
    rate,
    ingestion_ts
)
SELECT
    rate_date,
    base_currency,
    quote_currency,
    rate,
    ingestion_ts
FROM (
    SELECT
        rate_date,
        base_currency,
        quote_currency,
        rate,
        ingestion_ts,
        ROW_NUMBER() OVER (
            PARTITION BY rate_date, base_currency, quote_currency
            ORDER BY ingestion_ts DESC
        ) AS rn
    FROM raw_fx_rates
) t
WHERE rn = 1;

COMMIT;