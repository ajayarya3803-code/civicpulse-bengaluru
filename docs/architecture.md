# Architecture

1. The Kaun.City ward API provides ward-related source records.
2. The Bronze notebook fetches and persists raw responses with request metadata.
3. The Silver notebook parses and structures raw data into domain tables.
4. The Gold notebook creates analytics tables for dashboard use.
5. A Databricks Job runs the notebooks in dependency order.
6. The Airflow DAG triggers the Databricks Job through the Jobs API and waits for completion.

**Databricks:** ingestion, Spark transformations, Delta tables and analytics.  
**Airflow:** orchestration and tracking the Databricks job result.  
**Dashboard:** exploration of Gold tables.

Keep tokens and passwords out of notebooks, DAG files, Git history and screenshots. Document changes to notebook paths, job tasks or table names.
