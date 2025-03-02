CREATE OR REPLACE TABLE `sandbox-data-pipelines.sales_silver.fact_line_item_sales_daily`
(
    line_id                                 INT64,
    line_quantity_sold                      INT64,
    line_unit_price                         FLOAT64,
    line_discount_percent                   FLOAT64,
    line_quantity_returned                  FLOAT64,
    line_return_date                        DATE,
    line_created_date                       DATE,
    order_status                            STRING,
    order_id                                INT64,
    customer_id                             INT64,
    order_date                              DATE,
    order_payment_method                    STRING,
    order_sales_channel                     STRING,
    total_sale_amount                       FLOAT64,
    total_discount_amount                   FLOAT64,
    product_id                              INT64,
    product_name                            STRING,
    product_category_id                     INT64,
    product_subcategory_id                  INT64,
    product_unit_cost                       FLOAT64,
    is_order_total_sale                     BOOLEAN,
    is_order_direct_sale                    BOOLEAN,
    is_order_distributor_sale               BOOLEAN,
    is_order_online_sale                    BOOLEAN,
    is_line_discounted                      BOOLEAN,
    line_revenue_amount_total               FLOAT64,
    line_cost_of_goods_sold_amount_total    FLOAT64,
    line_discount_amount_total              FLOAT64,
    line_gross_profit_amount_total          FLOAT64,
    line_net_profit_amount_total            FLOAT64,
    execution_ts                            TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(execution_ts, DAY)
