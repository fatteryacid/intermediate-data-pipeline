WITH
component_products AS
(
    SELECT
        prod_id     AS product_id,
        prod_nm     AS product_name,
        cat_id      AS product_category_id,
        sub_cat_id  AS product_subcategory_id,
        u_cost      AS product_unit_cost,
        list_pr     AS product_list_price,
        min_stk     AS product_stock_min,
        max_stk     AS product_stock_max,
        cur_stk     AS product_stock_current,
        is_mfg      AS is_product_manufactured_inhouse,
        complexity  AS product_complexity_score,
        sup_id      AS supplier_id,
        sup_part_no AS supplier_part_number,
    FROM `sandbox-data-pipelines.sales_bronze.products_raw_daily`
    WHERE 1=1
        AND execution_ts = TIMESTAMP("{{ ts }}")
),

component_suppliers AS
(
    SELECT
        sup_id      AS supplier_id,
        comp_nm     AS supplier_company_name,
        cont_nm     AS supplier_contact_name,
        pmt_terms   AS supplier_payment_terms,
        lead_tm     AS supplier_lead_time,
        act_stat    AS supplier_is_active,
        pref_stat   AS supplier_is_preferred,
        curr        AS supplier_currency_code,
        last_ord_dt AS supplier_last_order_date,
    FROM `sandbox-data-pipelines.sales_bronze.suppliers_raw_daily`
    WHERE 1=1
        AND execution_ts = TIMESTAMP("{{ ts }}")
)

SELECT
    component_products.*,
    component_suppliers.* EXCEPT(supplier_id),
    TIMESTAMP("{{ ts }}")   AS execution_ts,
FROM component_products
LEFT JOIN component_suppliers
    ON  component_suppliers.supplier_id = component_products.supplier_id
;
