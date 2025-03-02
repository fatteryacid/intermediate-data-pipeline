-- Purpose is to validate each loaded partition does not contain duplicate ID records

WITH
component_grain_count AS
(
    SELECT
        COUNT(*) OVER(grain)    AS __number_of_grain,
    FROM `sandbox-data-pipelines.sales_bronze.order_line_items_raw_daily`
    WHERE 1=1
        AND execution_ts = TIMESTAMP("{{ ts }}")
    WINDOW
        grain AS (PARTITION BY line_id)
)

SELECT
    CASE WHEN MAX(__number_of_grain) > 1 THEN FALSE ELSE TRUE END AS __validator
FROM component_grain_count
;
