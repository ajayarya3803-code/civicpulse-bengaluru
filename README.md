# CivicPulse Bengaluru — Civic Infrastructure Analytics

An end-to-end data engineering project that transforms Bengaluru civic infrastructure data into analytics-ready datasets and a dashboard using **Databricks, PySpark, Delta Lake, and Apache Airflow**.

## Project Overview

CivicPulse Bengaluru brings together civic datasets to explore infrastructure distribution, ward-level spending, pothole complaints, and public work orders. The pipeline organizes raw data into structured layers and produces Gold tables for analytics and dashboard reporting.

## Architecture

```text
Civic Data APIs / Sources
          |
          v
    Bronze Layer
   Raw data ingestion
          |
          v
     Silver Layer
 Cleaning and transformation
          |
          v
      Gold Layer
 Analytics-ready datasets
          |
          v
 Databricks Dashboard

Apache Airflow --> Databricks Jobs API --> Databricks Pipeline
```

## Tech Stack

- **Cloud data platform:** Databricks
- **Data processing:** Python, PySpark
- **Storage and table format:** Delta Lake
- **Pipeline orchestration:** Apache Airflow
- **Containerization:** Docker, Docker Compose
- **Version control:** Git, GitHub
- **Analytics:** Databricks SQL and dashboards

## Data Pipeline

The pipeline follows a Medallion Architecture.

### Bronze — Raw ingestion
- Ingests source data into the raw layer.
- Preserves data for downstream processing.

### Silver — Cleaned datasets
- `civicpulse.silver.wards`
- `civicpulse.silver.infrastructure`
- `civicpulse.silver.potholes`
- `civicpulse.silver.spending`
- `civicpulse.silver.work_orders`

### Gold — Analytics-ready datasets
- `civicpulse.gold.ward_infrastructure_overview`
- `civicpulse.gold.ward_spending_analysis`
- `civicpulse.gold.contractor_work_order_analysis`
- `civicpulse.gold.source_work_order_summary`
- `civicpulse.gold.contractor_financial_analysis`

These tables support analysis of ward infrastructure, spending patterns, work orders, and contractor-related financial information.

## Orchestration with Apache Airflow

The Airflow DAG `civicpulse_pipeline` triggers the Databricks Job and waits for its completion.

The Databricks Job runs the Bronze, Silver, and Gold notebooks in sequence. Airflow is run locally using Docker Compose.

## Dashboard

The project includes an exported PDF of the **CivicPulse Bengaluru — Civic Infrastructure Analytics** dashboard.

See the `dashboard/` directory for the exported dashboard.

## Data Quality and Limitations

- Pothole complaint data is from 2022 and may not reflect current conditions.
- Spending analysis covers 2018–2023.
- Ward mappings between BBMP's 198 wards and DataMeet's 243 wards are estimated; ward-level spending results should be interpreted cautiously.
- Work-order financial values may repeat across ward-allocation records, so contractor-level aggregates are preliminary.
- Missing contractor names are grouped under `Unknown`.
- Source refresh frequency is not confirmed.

These limitations should be considered when interpreting the dashboard and Gold-layer metrics.

## Repository Structure

```text
civicpulse-bengaluru/
├── airflow/
│   └── dags/
│       └── civicpulse_pipeline.py
├── dashboard/
├── docs/
│   ├── architecture.md
│   └── data-quality.md
├── notebooks/
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── sql/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
├── tests/
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

## Running Locally

1. Install Docker Desktop and ensure Docker is running.
2. Configure the required Airflow and Databricks connection settings locally.
3. Start the Airflow services using Docker Compose.
4. Configure the `databricks_default` Airflow connection with your Databricks workspace host and access token.
5. Trigger the `civicpulse_pipeline` DAG from the Airflow UI.

**Security:** Never commit `.env` files, access tokens, passwords, or other credentials to GitHub.

## Future Improvements

- Add automated data-quality checks and validation tests.
- Implement incremental ingestion and source freshness monitoring.
- Improve ward mapping accuracy.
- Add pipeline observability, retries, and failure alerts.
- Expand analytical coverage as more reliable source data becomes available.

## Author

**Ajay T.**

[GitHub Repository](https://github.com/ajayarya3803-code/civicpulse-bengaluru)
