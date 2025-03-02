SELECT  
    *,
    TIMESTAMP("{{ ts }}")    AS execution_ts,
FROM `sandbox-data-pipelines.sales_bronze.order_line_items_external`
-- The following logic emulates a "daily" incremental load
-- Meaning although the source data is given for the entire year 2025, future sales dates are treated as "pending" sales that haven't happened yet
WHERE 1=1
    AND crt_dt <= DATE("{{ ds }}")
;
