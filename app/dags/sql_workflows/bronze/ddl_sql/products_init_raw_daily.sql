CREATE TABLE `sandbox-data-pipelines.sales_bronze.products_raw_daily`
(
    prod_id         INT64,
    prod_nm         STRING,
    cat_id          INT64,
    sub_cat_id      INT64,
    u_cost          FLOAT64,
    list_pr         FLOAT64,
    min_stk         INT64,
    max_stk         INT64,
    cur_stk         INT64,
    is_mfg          BOOLEAN,
    complexity      FLOAT64,
    sup_id          FLOAT64,
    sup_part_no     STRING,
    execution_ts    TIMESTAMP,
)
PARTITION BY TIMESTAMP_TRUNC(execution_ts, DAY)
;
