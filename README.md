# Intermediate Data Pipeline

## Project Summary
This project contains a data pipeline that models mock product sales data in a data warehouse for efficient, reliable, and scalable analytics.

Data provided simulates a raw extraction from an ERP (enterprise resource planning software) for a retailer that  manufactures some products in-house and purchases others from suppliers.
## Purpose
The purpose of this project is to implement a data model to answer sales-related business questions such as:
- What is the margin by product category?
- What are the products with highest return rate?
- How do discounts impact sales?
- Who are the most profitable customers?
## High Level Architecture
![architecture-diagram](/docs/architecture.jpg)
This data pipeline follows an ELT paradigm using Google Cloud Platform, written in an Airflow framework. Data flows from CSV files in Google Cloud Storage into bronze, silver, and gold layers in BigQuery.
## High Level Data Model
![data-model](/docs/data-model.jpg)
Within BigQuery, data is modeled following a medallion architecture. The logical divide of between the different layers can be summarized as:
- **Bronze**: Unmodified raw data, time-partitioned by each extraction run. 
	- This layer will serve as the "foundation" of all the downstream data, allowing silver and gold tables to be fully rebuilt in case of disaster. 
	- Meant to be a data engineering read/write only.
- **Silver**: Cleaned, deduped data joined into a dimensional model. 
	- This layer will serve as the main building block for analytical functions, with full time-partitioned fact and dimension tables to serve both current day analytical questions, and point-in-time questions.
	- Meant to be data engineering read/write, and analytical function read only.
- **Gold**: Aggregated tables, or any other reporting/dashboard-ready tables. 
	- This layer will serve as the main working layer for analytical functions, with tables built to be pulled into dashboards or sheets quickly.
	- Meant to be read/write for analysts, BI developers, etc.
## Design Decisions
| Decision                                                | **Rationale**                                                                                                                                                                                                                                                   | **Tradeoff**                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| All tables are time-partitioned by execution timestamp. | A couple of key reasons: a) to preserve historical context of data, and b) to enable reconstruction of downstream tables in the event of a disaster.<br><br>                                                                                                    | Main tradeoff is storage space.<br><br>For small tables such as `dim_products` and `dim_customers`, this cost is negligible.<br><br>However for large tables such as `sales_performance_products`, this cost will scale with time.<br><br>A possible way to handle this is to "archive" old partitions into blob storage for a lower cost. |
| Keeping dimensional model in silver layer.              | The silver layer is meant to be treated as a resource to build gold tables with.<br><br>This enables users to move any logic/compute from the front-end dashboards/BI tools into the gold layer in order to leverage cloud compute for dashboard optimizations. | Building directly into the silver layer takes away a "staging" layer where intermediate tables can be kept.<br><br>This means any staging tables would have to be built either directly in bronze layer, or kept at the same level as the silver layer.                                                                                    |
| Checks only applied to bronze tables.                   | An ideal state for this pipeline would have at least granularity checks on all layers.<br><br>Even though it is not expected that a duplicate will make its way from bronze to gold, a bad join might.                                                          | 2 validations are only ran on the bronze tables: a) check for empty partition, and b) check for duplicate IDs.                                                                                                                                                                                                                             |
## Limitations
1. When pulling data from either gold tables, it can be difficult to bucket by sales channels since these are aggregated into fact columns.

```sql
-- This format might be better for some applications
SELECT
	month_start_date,
	sales_channel,
	SUM(revenue)    AS revenue
FROM gold_table
;
```

2. It is not possible to relate aggregate customer performance data back to products, and vice versa when using the gold tables.

```sql
-- Customer and product data are on their respective grains only
SELECT
	gold_customer.data,
	gold_products.data,
FROM gold_customer
LEFT JOIN gold_products
	ON  gold_customer.some_common_id = gold_products.some_common_id
```

3. Product changes are not brought into this data model, however should be present in the silver layer.

```sql
-- This is not supported, silver_product_change does not exist
SELECT
	gold_products.data,
	silver_product_change.change_val,
FROM gold_products
LEFT JOIN silver_product_change
	ON  gold_products.product_id = silver_product_change.product_id
	AND (
		gold_products.sale_date > silver_product_change.last_change AND
		gold_products.sale_date < silver_product_change.next_change
	)
		
```

4. There is no view that allows a user to quickly pull current data from the partitioned tables.

```sql
-- This would save a lot of cost by not scanning all partitions
SELECT
	*
FROM gold_view
```
## Getting Started
Although the keys used to run this project are not provided, Airflow setup is available using the following procedures.
1. Run `cd app` to change to `app/` directory.
2. Run `docker compose build` to build the Docker image.
3. Run `docker compose up -d` to run the Docker image detached.
4. Navigate to `localhost:8080` to view Airflow instance.
	- Username and password are: `airflow`
5. Close with `docker compose down`.