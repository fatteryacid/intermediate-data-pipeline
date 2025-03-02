CREATE TABLE `sandbox-data-pipelines.sales_bronze.customers_raw_daily`
(
    cust_id         INT64,
    comp_nm         STRING,
    cust_typ        STRING,
    ind             STRING,
    rgn_id          INT64,
    cred_lim        INT64,
    pmt_terms       STRING,
    act_stat        BOOLEAN,
    execution_ts    TIMESTAMP,
)
PARTITION BY TIMESTAMP_TRUNC(execution_ts, DAY)
;
