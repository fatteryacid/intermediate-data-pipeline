-- Purpose is to validate each loaded partition is not empty

WITH
component_row_count AS
(
    SELECT
        COUNT(*)    AS __number_of_records,
    FROM `sandbox-data-pipelines.sales_bronze.customers_raw_daily`
    WHERE 1=1
        AND execution_ts = TIMESTAMP("{{ ts }}")
)

SELECT
    CASE WHEN __number_of_records = 0 THEN FALSE ELSE TRUE END  AS __validator,
FROM component_row_count
;
