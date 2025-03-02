-- Purpose of this table is to hold granular information on sales orders, including which products are sold, and in what quantity


WITH
component_order_line_items AS
-- Only taking fields relevant to sales order to declutter final table
-- Local assumptions made:
--  1. Product prices reflected in line item sales are accurate to the price given to customers
--      a. line_item.quantity * line_item.product_unit_price = sales_order.total_sale_amount
--  2. Historical transactions are not modified, e.g. an item sold in 2024-01-03 will contain the same product unit price
(
    SELECT
        line_id         AS line_id,
        quan            AS line_quantity_sold,
        u_price         AS line_unit_price,      -- Kind of sus, doesn't reflect changes from `product_changes`.
        disc_pct        AS line_discount_percent,
        ret_q           AS line_quantity_returned,
        ret_dt          AS line_return_date,
        crt_dt          AS line_created_date,
        ord_id          AS order_id,
        status          AS order_status,
        prod_id         AS product_id,
    FROM `sandbox-data-pipelines.sales_bronze.order_line_items_raw_daily`
    WHERE 1=1
        AND execution_ts = TIMESTAMP("{{ ts }}")
        AND ord_type = "SO"
),

component_sales_orders AS
(
    SELECT
        ord_id          AS order_id,
        cust_id         AS customer_id,
        ord_dt          AS order_date,
        pmt_mthd        AS order_payment_method,
        sales_ch        AS order_sales_channel,
        tot_amt         AS total_sale_amount,
        disc_amt        AS total_discount_amount,
    FROM `sandbox-data-pipelines.sales_bronze.sales_orders_raw_daily`
    WHERE 1=1
        AND execution_ts = TIMESTAMP("{{ ts }}")
),

component_products AS
(
    SELECT
        prod_id         AS product_id,
        prod_nm         AS product_name,
        cat_id          AS product_category_id,
        sub_cat_id      AS product_subcategory_id,
        u_cost          AS product_unit_cost,
    FROM `sandbox-data-pipelines.sales_bronze.products_raw_daily`
    WHERE 1=1
        AND execution_ts = TIMESTAMP("{{ ts }}")
),

module_line_item_sales_base AS
(
    SELECT
        -- These `SELECT *` set up so product and order dimensions stay grouped together in table schema
        component_order_line_items.* EXCEPT(order_id, product_id),
        component_sales_orders.*,
        component_products.*,
    FROM component_order_line_items
    LEFT JOIN component_sales_orders
        ON  component_sales_orders.order_id = component_order_line_items.order_id
    LEFT JOIN component_products
        ON  component_products.product_id = component_order_line_items.product_id
),

component_sales_channel_identifiers AS
-- Purpose of these flags are to abstract away hardcoding of string comparisons
-- e.g. WHERE is_order_total_sale instead of WHERE order_sales_channel = "Direct"
(
    SELECT
        *,
        1=1                                                     AS is_order_total_sale,     -- Total sale as Direct + Distributor + Online, done to make it easier to vertical select ;)
        COALESCE(order_sales_channel = "Direct", FALSE)         AS is_order_direct_sale,
        COALESCE(order_sales_channel = "Distributor", FALSE)    AS is_order_distributor_sale,
        COALESCE(order_sales_channel = "Online", FALSE)         AS is_order_online_sale,
        COALESCE(line_discount_percent, 0) > 0                  AS is_line_discounted,
    FROM module_line_item_sales_base
    WHERE 1=1
),

component_baseline_revenue_expense AS
(
    SELECT
        *,
        COALESCE(line_quantity_sold, 0) * COALESCE(line_unit_price, 0)                              AS line_revenue_amount_total,
        COALESCE(line_quantity_sold, 0) * COALESCE(product_unit_cost, 0)                            AS line_cost_of_goods_sold_amount_total,
    FROM component_sales_channel_identifiers
),

component_discount_calculations AS
(
    SELECT
        *,
        COALESCE(line_revenue_amount_total, 0) * COALESCE(line_discount_percent, 0)                 AS line_discount_amount_total,
    FROM component_baseline_revenue_expense
),

component_profit_calculations AS
-- gross as `revenue - COGS`
-- net as `revenue - COGS - discounts`
(
    SELECT
        *,
        COALESCE(line_revenue_amount_total, 0) - COALESCE(line_cost_of_goods_sold_amount_total, 0)  AS line_gross_profit_amount_total,
        COALESCE(line_revenue_amount_total, 0) 
            - COALESCE(line_cost_of_goods_sold_amount_total, 0)
            - COALESCE(line_discount_amount_total, 0)                                               AS line_net_profit_amount_total,
    FROM component_discount_calculations
)

SELECT 
    *,
    TIMESTAMP("{{ ts }}")   AS execution_ts,
FROM component_profit_calculations
;
