from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from datetime import datetime
from pathlib import Path

import os
import sys
sys.path.append("/opt/airflow/utils")

import common
import services

DAG_ID = Path(__file__).name
CGS_SA = GoogleBaseHook(gcp_conn_id = common.GCP_SERVICE_ACCT).get_credentials()   # SA details stored within Airflow environment
CUR_DIR = os.path.abspath(os.path.dirname(__file__))

with DAG(dag_id=DAG_ID, start_date=datetime(2025, 1, 1), catchup=False, schedule_interval="@daily") as dag:
    # Paradigm for this DAG follows:
    #   - Bronze:
    #       1. External creation (if needed)
    #       2. Raw load to daily partitioned table
    #       3. Check if partition is empty
    #       4. Check if partition has ID dupes
    #   - Silver:
    #       1. Load if bronze dependencies are met
    #   - Gold:
    #       1. Load if silver dependencies are met


    # ============================
    # Customers ETL
    # ============================
    bronze_customer_external = GCSToBigQueryOperator(
        # Loads raw GCS content via external table
        # - done to not incur any transfer costs
        # - external table treated as a staging table and will be removed
        task_id = "bronze_customer_external",
        bucket = common.GCS_LANDING_BUCKET,
        source_objects = [services.build_gcs_object_path(common.GCS_LANDING_SUBDIR, common.CUSTOMERS, common.FILE_FORMAT)],
        destination_project_dataset_table = services.build_bq_table_name(common.BQ_PROJECT_ID, common.BQ_DATASET_BRONZE, common.CUSTOMERS + common.BQ_LOAD_SUFFIX),
        source_format = "CSV",
        create_disposition = "CREATE_IF_NEEDED",
        external_table = True,
        autodetect = True,
        gcp_conn_id = common.GCP_SERVICE_ACCT,
    )

    bronze_customer_load = BigQueryInsertJobOperator(
        task_id = "bronze_customer_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_BRONZE, common.SQL_BRONZE_CUSTOMERS]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_BRONZE,
                    "tableId": common.CUSTOMERS + common.BQ_RAW_SUFFIX + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    bronze_customer_partition_check = BigQueryCheckOperator(
        task_id = "bronze_customer_partition_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_CUSTOMERS_PRTN_CHECK]),
        use_legacy_sql = False
    )

    bronze_customer_granularity_check = BigQueryCheckOperator(
        task_id = "bronze_customer_granularity_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_CUSTOMERS_GRAIN_CHECK]),
        use_legacy_sql = False
    )

    bronze_customer_external >> bronze_customer_load >> bronze_customer_partition_check >> bronze_customer_granularity_check


    # ============================
    # Order Line Items ETL
    # ============================
    bronze_line_external = GCSToBigQueryOperator(
        task_id = "bronze_line_external",
        bucket = common.GCS_LANDING_BUCKET,
        source_objects = [services.build_gcs_object_path(common.GCS_LANDING_SUBDIR, common.ORDER_LINE_ITEMS, common.FILE_FORMAT)],
        destination_project_dataset_table = services.build_bq_table_name(common.BQ_PROJECT_ID, common.BQ_DATASET_BRONZE, common.ORDER_LINE_ITEMS + common.BQ_LOAD_SUFFIX),
        source_format = "CSV",
        create_disposition = "CREATE_IF_NEEDED",
        external_table = True,
        autodetect = True,
        gcp_conn_id = common.GCP_SERVICE_ACCT,
    )

    bronze_line_load = BigQueryInsertJobOperator(
        task_id = "bronze_line_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_BRONZE, common.SQL_BRONZE_LINE]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_BRONZE,
                    "tableId": common.ORDER_LINE_ITEMS + common.BQ_RAW_SUFFIX + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    bronze_line_partition_check = BigQueryCheckOperator(
        task_id = "bronze_line_partition_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_LINE_PRTN_CHECK]),
        use_legacy_sql = False
    )

    bronze_line_granularity_check = BigQueryCheckOperator(
        task_id = "bronze_line_granularity_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_LINE_GRAIN_CHECK]),
        use_legacy_sql = False
    )

    bronze_line_external >> bronze_line_load >> bronze_line_partition_check >> bronze_line_granularity_check


    # ============================
    # Products ETL
    # ============================
    bronze_products_external = GCSToBigQueryOperator(
        task_id = "bronze_products_external",
        bucket = common.GCS_LANDING_BUCKET,
        source_objects = [services.build_gcs_object_path(common.GCS_LANDING_SUBDIR, common.PRODUCTS, common.FILE_FORMAT)],
        destination_project_dataset_table = services.build_bq_table_name(common.BQ_PROJECT_ID, common.BQ_DATASET_BRONZE, common.PRODUCTS + common.BQ_LOAD_SUFFIX),
        source_format = "CSV",
        create_disposition = "CREATE_IF_NEEDED",
        external_table = True,
        autodetect = True,
        gcp_conn_id = common.GCP_SERVICE_ACCT,
    )

    bronze_products_load = BigQueryInsertJobOperator(
        task_id = "bronze_products_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_BRONZE, common.SQL_BRONZE_PRODUCTS]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_BRONZE,
                    "tableId": common.PRODUCTS + common.BQ_RAW_SUFFIX + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    bronze_products_partition_check = BigQueryCheckOperator(
        task_id = "bronze_products_partition_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_PRODUCTS_PRTN_CHECK]),
        use_legacy_sql = False
    )

    bronze_products_granularity_check = BigQueryCheckOperator(
        task_id = "bronze_products_granularity_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_PRODUCTS_GRAIN_CHECK]),
        use_legacy_sql = False
    )

    bronze_products_external >> bronze_products_load >> bronze_products_partition_check >> bronze_products_granularity_check

    # ============================
    # Sales Orders ETL
    # ============================
    bronze_sales_order_external = GCSToBigQueryOperator(
        task_id = "bronze_sales_order_external",
        bucket = common.GCS_LANDING_BUCKET,
        source_objects = [services.build_gcs_object_path(common.GCS_LANDING_SUBDIR, common.SALES_ORDERS, common.FILE_FORMAT)],
        destination_project_dataset_table = services.build_bq_table_name(common.BQ_PROJECT_ID, common.BQ_DATASET_BRONZE, common.SALES_ORDERS + common.BQ_LOAD_SUFFIX),
        source_format = "CSV",
        create_disposition = "CREATE_IF_NEEDED",
        external_table = True,
        autodetect = True,
        gcp_conn_id = common.GCP_SERVICE_ACCT,
    )

    bronze_sales_order_load = BigQueryInsertJobOperator(
        task_id = "bronze_sales_order_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_BRONZE, common.SQL_BRONZE_SALES_ORDERS]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_BRONZE,
                    "tableId": common.SALES_ORDERS + common.BQ_RAW_SUFFIX + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    bronze_sales_order_partition_check = BigQueryCheckOperator(
        task_id = "bronze_sales_order_partition_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_SALES_ORDERS_PRTN_CHECK]),
        use_legacy_sql = False
    )

    bronze_sales_order_granularity_check = BigQueryCheckOperator(
        task_id = "bronze_sales_order_granularity_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_SALES_ORDERS_GRAIN_CHECK]),
        use_legacy_sql = False
    )

    bronze_sales_order_external >> bronze_sales_order_load >> bronze_sales_order_partition_check >> bronze_sales_order_granularity_check


    # ============================
    # Suppliers ETL
    # ============================
    bronze_suppliers_external = GCSToBigQueryOperator(
        task_id = "bronze_suppliers_external",
        bucket = common.GCS_LANDING_BUCKET,
        source_objects = [services.build_gcs_object_path(common.GCS_LANDING_SUBDIR, common.SUPPLIERS, common.FILE_FORMAT)],
        destination_project_dataset_table = services.build_bq_table_name(common.BQ_PROJECT_ID, common.BQ_DATASET_BRONZE, common.SUPPLIERS + common.BQ_LOAD_SUFFIX),
        source_format = "CSV",
        create_disposition = "CREATE_IF_NEEDED",
        external_table = True,
        autodetect = True,
        gcp_conn_id = common.GCP_SERVICE_ACCT,
    )

    bronze_suppliers_load = BigQueryInsertJobOperator(
        task_id = "bronze_suppliers_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_BRONZE, common.SQL_BRONZE_SUPPLIERS]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_BRONZE,
                    "tableId": common.SUPPLIERS + common.BQ_RAW_SUFFIX + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    bronze_suppliers_partition_check = BigQueryCheckOperator(
        task_id = "bronze_suppliers_partition_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_SUPPLIERS_PRTN_CHECK]),
        use_legacy_sql = False
    )

    bronze_suppliers_granularity_check = BigQueryCheckOperator(
        task_id = "bronze_suppliers_granularity_check",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        sql = services.get_query([CUR_DIR, common.SQL_BRONZE_TESTS, common.SQL_BRONZE_SUPPLIERS_GRAIN_CHECK]),
        use_legacy_sql = False
    )

    bronze_suppliers_external >> bronze_suppliers_load >> bronze_suppliers_partition_check >> bronze_suppliers_granularity_check


    # ============================
    # `dim_customers`
    # ============================
    silver_dim_customers_load = BigQueryInsertJobOperator(
        task_id = "silver_dim_customers_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_SILVER, common.SQL_SILVER_DIM_CUSTOMERS]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_SILVER,
                    "tableId": common.BQ_DIM_CUSTOMERS + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    bronze_customer_granularity_check >> silver_dim_customers_load


    # ============================
    # `dim_products`
    # ============================
    silver_dim_products_load = BigQueryInsertJobOperator(
        task_id = "silver_dim_products_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_SILVER, common.SQL_SILVER_DIM_PRODUCTS]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_SILVER,
                    "tableId": common.BQ_DIM_PRODUCTS + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    [bronze_products_granularity_check, bronze_suppliers_granularity_check] >> silver_dim_products_load

    # ============================
    # `fact_line_item_sales`
    # ============================
    silver_fact_line_load = BigQueryInsertJobOperator(
        task_id = "silver_fact_line_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_SILVER, common.SQL_SILVER_FACT_LINE]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_SILVER,
                    "tableId": common.BQ_FACT_LINE_ITEM_SALES + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    [bronze_line_granularity_check, bronze_sales_order_granularity_check] >> silver_fact_line_load


    # ============================
    # `sales_performance_customers_daily`
    # ============================
    gold_customers_perf_load = BigQueryInsertJobOperator(
        task_id = "gold_customers_perf_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_GOLD, common.SQL_GOLD_CUSTOMERS_PERF]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_GOLD,
                    "tableId": common.BQ_GOLD_CUSTOMERS_SALES + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    [silver_dim_customers_load, silver_fact_line_load] >> gold_customers_perf_load


    # ============================
    # `sales_performance_products_daily`
    # ============================
    gold_products_perf_load = BigQueryInsertJobOperator(
        task_id = "gold_products_perf_load",
        gcp_conn_id = common.GCP_SERVICE_ACCT,
        configuration = {
            "query": {
                "query": services.get_query([CUR_DIR, common.SQL_GOLD, common.SQL_GOLD_PRODUCTS_PERF]),
                "useLegacySql": False,
                "priority": "BATCH",
                "writeDisposition": "WRITE_TRUNCATE",
                "destinationTable": {
                    "projectId": common.BQ_PROJECT_ID,
                    "datasetId": common.BQ_DATASET_GOLD,
                    "tableId": common.BQ_GOLD_PRODUCTS_SALES + '$' + "{{ ds_nodash }}",       # specifies partition to modify
                    "timePartitioning": {
                        "field": "execution_ts",
                        "type": "DAY"
                    },
                },
                "allow_large_results": True,
            }
        }
    )

    [silver_dim_products_load, silver_fact_line_load] >> gold_products_perf_load
