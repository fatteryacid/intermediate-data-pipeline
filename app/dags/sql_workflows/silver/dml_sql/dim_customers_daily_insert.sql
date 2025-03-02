WITH
component_raw_extract AS
(
    SELECT
        cust_id     AS customer_id,
        comp_nm     AS customer_company_name,
        cust_typ    AS customer_type,
        ind         AS customer_industry,
        rgn_id      AS customer_region_id,
        cred_lim    AS customer_credit_limit,
        pmt_terms   AS customer_payment_terms,
        act_stat    AS is_customer_active,
    FROM `sandbox-data-pipelines.sales_bronze.customers_raw_daily`
    WHERE 1=1
        AND execution_ts = TIMESTAMP("{{ ts }}")
        AND cust_id IS NOT NULL
),

component_payment_term_delays AS
-- Meant to be used as variables on any date functions
(
    SELECT
        *,
        CASE
            WHEN customer_payment_terms = "Immediate" THEN 0
            WHEN customer_payment_terms = "Net 30" THEN 30
            WHEN customer_payment_terms = "Net 60" THEN 60
            WHEN customer_payment_terms = "Net 90" THEN 90
        END         AS customer_payment_threshold_days,
    FROM component_raw_extract
)

SELECT
    *,
    TIMESTAMP("{{ ts }}")   AS execution_ts,
FROM component_payment_term_delays
;
