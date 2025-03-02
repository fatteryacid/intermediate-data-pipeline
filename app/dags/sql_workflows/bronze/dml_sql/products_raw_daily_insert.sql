SELECT  
    *,
    TIMESTAMP("{{ ts }}")    AS execution_ts,
FROM `sandbox-data-pipelines.sales_bronze.products_external`
;
