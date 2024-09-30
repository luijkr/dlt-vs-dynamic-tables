# Comparing Databricks Delta Live Tables with Snowflake's Dynamic Tables

The goal of this repos is three-fold
- Generate some data in both Databricks and in Snowflake
- Build a simple a Delta Live Tables pipeline using this data
- Build a few Dynamic Tables

## Repos overview

### `src/data_setup`

- `data_generators.py`: functions used to generate some fictional sales data
- `setup_databricks.py`: create base tables in Databricks
- `setup_snowflake.py`: create base tables in Snowflake

### `src/dlt`

Two Delta Live Table pipelines. One in SQL, and one in PySpark.

### `src/dynamic_tables`

Creation of the same data flow as in the DLT pipeline, but then using Snowflake's Dynamic Tables.
