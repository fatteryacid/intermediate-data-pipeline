CREATE TABLE `sandbox-data-pipelines.sales_bronze.sales_orders_raw_daily`
(
    ord_id          INT64,
    cust_id         INT64,
    ord_dt          DATE,
    ship_dt         DATE,
    stat            STRING,
    pmt_mthd        STRING,
    sales_ch        STRING,
    sales_rep_id    INT64,
    tot_amt         FLOAT64,
    disc_amt        FLOAT64,
    execution_ts    TIMESTAMP,
)
PARTITION BY TIMESTAMP_TRUNC(execution_ts, DAY)
;
