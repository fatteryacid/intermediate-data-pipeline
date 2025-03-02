CREATE TABLE `sandbox-data-pipelines.sales_bronze.order_line_items_raw_daily`
(
    line_id         INT64,
    ord_id          INT64,
    ord_type        STRING,
    prod_id         INT64,
    quan            INT64,
    u_price         FLOAT64,
    disc_pct        FLOAT64,
    ret_q           FLOAT64,
    ret_dt          DATE,
    rcv_q           FLOAT64,
    rcv_dt          DATE,
    qual_rt         FLOAT64,
    status          STRING,
    crt_dt          DATE,
    execution_ts    TIMESTAMP,
)
PARTITION BY TIMESTAMP_TRUNC(execution_ts, DAY)
;
