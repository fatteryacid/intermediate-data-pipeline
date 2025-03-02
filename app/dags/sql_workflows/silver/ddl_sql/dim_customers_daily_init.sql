CREATE OR REPLACE TABLE `sandbox-data-pipelines.sales_silver.dim_customers_daily`
(
    customer_id                         INT64,
    customer_company_name               STRING,
    customer_type                       STRING,
    customer_industry                   STRING,
    customer_region_id                  INT64,
    customer_credit_limit               INT64,
    customer_payment_terms              STRING,
    is_customer_active                  BOOLEAN,
    customer_payment_threshold_days     INT64,
    execution_ts                        TIMESTAMP,
)
PARTITION BY TIMESTAMP_TRUNC(execution_ts, DAY)
