/* -------- Parameters -------- */
SET end_lag_days = 2;      -- provider delay
SET window_days  = 14;     -- inclusive days to ingest

/* Window:
   end_date   = CURRENT_DATE - end_lag_days
   start_date = end_date - (window_days - 1)  -- 14 days inclusive
*/
WITH bounds AS (
  SELECT
    DATEADD('day', -$end_lag_days, CURRENT_DATE())                       AS end_date,
    DATEADD('day', -($end_lag_days + $window_days - 1), CURRENT_DATE())  AS start_date
)
SELECT
  e.MREL_ID,
  e.MARKET_ID,
  e.MARKET_NAME,
  e.COUNTRY_CODE,
  e.SERVICE_TYPE,
  e.CONTENT_TYPE,
  e.COMMERCIAL_MODEL,
  e.STORE_STRATA,
  e.DISTRIBUTION_CHANNEL,
  e.PURCHASE_METHOD,
  e.PRODUCT_FORMAT,
  e.TRANSACTION_TYPE,
  e.METRIC_CATEGORY,
  e.QUANTITY,
  e.EQUIVALENT_QUANTITY,
  e.REPORT_DATE,
  e.MODIFIED_AT
FROM LUMINATE_DB_LISTING_DETAIL.EXTRACT_S.VW_DAILY_FACT_MREL_DETAIL_DS AS e
CROSS JOIN bounds b
WHERE e.REPORT_DATE BETWEEN b.start_date AND b.end_date;
-- No ORDER BY (keep it fast); sort downstream if needed.
