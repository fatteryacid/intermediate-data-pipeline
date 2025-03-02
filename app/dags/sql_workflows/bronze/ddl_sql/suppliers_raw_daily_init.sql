CREATE TABLE `sandbox-data-pipelines.sales_bronze.suppliers_raw_daily`
(
    sup_id          INT64,
    comp_nm         STRING,
    cont_nm         STRING,
    pmt_terms       STRING,
    lead_tm         INT64,
    act_stat        BOOLEAN,
    pref_stat       BOOLEAN,
    curr            STRING,
    last_ord_dt     DATE,
    execution_ts    TIMESTAMP,
)
PARTITION BY TIMESTAMP_TRUNC(execution_ts, DAY)
;
