The logical separation of the subdirectory structure in *all* `sql_workflows/` is the following:
- `ddl_sql`: Used to initially create an empty table
- `dml_sql`: Used to handle inserts and upserts
- `test_sql`: Validation checks

PLEASE BEWARE that SQL filenames are referenced in `app/utils/common`. This is how Airflow paths to a specific SQL script to run.
Keep this in mind if making any name changes to the SQL scripts.
