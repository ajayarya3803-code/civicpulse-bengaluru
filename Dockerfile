
FROM apache/airflow:3.3.2

USER airflow

RUN python -m pip install --no-cache-dir apache-airflow-providers-databricks
