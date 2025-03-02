"""
GCP configurations

These are used to manage GCS connectivity within Airflow environment
"""
GCP_SERVICE_ACCT = "saGCS"
GCS_LANDING_BUCKET = "t3-landing-zone"
GCS_LANDING_SUBDIR = "anduril-take-home-data"

"""
BQ configurations

These are used to manage BQ-specific projects and datasets
"""
BQ_PROJECT_ID = "sandbox-data-pipelines"

BQ_DATASET_BRONZE = "sales_bronze"
BQ_DATASET_SILVER = "sales_silver"
BQ_DATASET_GOLD = "sales_gold"

BQ_LOAD_SUFFIX = "_external"
BQ_RAW_SUFFIX = "_raw_daily"

BQ_DIM_CUSTOMERS = "dim_customers_daily"
BQ_DIM_PRODUCTS = "dim_products_daily"
BQ_FACT_LINE_ITEM_SALES = "fact_line_item_sales_daily"


BQ_GOLD_CUSTOMERS_SALES = "sales_performance_customers_daily"
BQ_GOLD_PRODUCTS_SALES = "sales_performance_products_daily"

"""
Data configurations

These are used to manage raw data landed in GCS
"""
FILE_FORMAT = "csv"
CUSTOMERS = "customers"
ORDER_LINE_ITEMS = "order_line_items"
PRODUCTS = "products"
SALES_ORDERS = "sales_orders"
SUPPLIERS = "suppliers"

"""
SQL Configurations

These are used to direct Airflow to the specific job it needs to run
"""
SQL_BRONZE = "sql_workflows/bronze/dml_sql"
SQL_BRONZE_TESTS = "sql_workflows/bronze/test_sql"
SQL_SILVER = "sql_workflows/silver/dml_sql"
SQL_GOLD = "sql_workflows/gold/dml_sql"

SQL_BRONZE_CUSTOMERS = "customers_raw_daily_insert.sql"
SQL_BRONZE_CUSTOMERS_PRTN_CHECK = "customers_raw_daily_partition_check.sql"
SQL_BRONZE_CUSTOMERS_GRAIN_CHECK = "customers_raw_daily_granularity_check.sql"
SQL_BRONZE_LINE = "order_line_items_raw_daily_insert.sql"
SQL_BRONZE_LINE_PRTN_CHECK = "order_line_items_raw_daily_partition_check.sql"
SQL_BRONZE_LINE_GRAIN_CHECK = "order_line_items_raw_daily_granularity_check.sql"
SQL_BRONZE_PRODUCTS = "products_raw_daily_insert.sql"
SQL_BRONZE_PRODUCTS_PRTN_CHECK = "products_raw_daily_partition_check.sql"
SQL_BRONZE_PRODUCTS_GRAIN_CHECK = "products_raw_daily_granularity_check.sql"
SQL_BRONZE_SALES_ORDERS = "sales_orders_raw_daily_insert.sql"
SQL_BRONZE_SALES_ORDERS_PRTN_CHECK = "sales_orders_raw_daily_partition_check.sql"
SQL_BRONZE_SALES_ORDERS_GRAIN_CHECK = "sales_orders_raw_daily_granularity_check.sql"
SQL_BRONZE_SUPPLIERS = "suppliers_raw_daily_insert.sql"
SQL_BRONZE_SUPPLIERS_PRTN_CHECK = "suppliers_raw_daily_partition_check.sql"
SQL_BRONZE_SUPPLIERS_GRAIN_CHECK = "suppliers_raw_daily_granularity_check.sql"

SQL_SILVER_DIM_CUSTOMERS = "dim_customers_daily_insert.sql"
SQL_SILVER_DIM_PRODUCTS = "dim_products_daily_insert.sql"
SQL_SILVER_FACT_LINE = "fact_line_item_sales_daily_insert.sql"

SQL_GOLD_CUSTOMERS_PERF = "sales_performance_customers_daily_insert.sql"
SQL_GOLD_PRODUCTS_PERF = "sales_performance_products_daily_insert.sql"
