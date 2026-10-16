# CivicPulse Bengaluru — Civic Infrastructure Analytics

CivicPulse Bengaluru is a data engineering portfolio project that ingests civic ward data, transforms it into analytics-ready tables, and orchestrates the pipeline with Databricks and Apache Airflow.

> **Important:** This is a portfolio analytics project, not an official BBMP reporting system or a real-time civic monitoring service.

## Goals
- Ingest ward-level data from the Kaun.City data API.
- Store raw API responses in Bronze.
- Transform data into structured Silver tables.
- Build Gold analytics tables for dashboarding.
- Orchestrate the Databricks job using local Apache Airflow.
- Document data-quality limitations and assumptions.

## Architecture
```text
Kaun.City ward API
        |
        v
Bronze: raw API responses
        |
        v
Silver: structured ward, infrastructure, pothole,
        spending and work-order tables
        |
        v
Gold: infrastructure, spending and contractor analytics
        |
        v
Databricks dashboard

Apache Airflow (Docker) -> Databricks Jobs API
                         -> Bronze -> Silver -> Gold
```

## Technology stack
- Databricks notebooks, Delta tables and dashboard
- Apache Airflow 3.3.2
- Docker Compose
- Python and SQL / Spark SQL
- Databricks Jobs API

## Data layers

### Bronze
Raw API responses and request metadata, including ward number, source URL, fetch timestamp, request status and error details.

### Silver
- `civicpulse.silver.wards`
- `civicpulse.silver.infrastructure`
- `civicpulse.silver.potholes`
- `civicpulse.silver.spending`
- `civicpulse.silver.work_orders`

### Gold
- `civicpulse.gold.ward_infrastructure_overview`
- `civicpulse.gold.ward_spending_analysis`
- `civicpulse.gold.contractor_work_order_analysis`
- `civicpulse.gold.source_work_order_summary`
- `civicpulse.gold.contractor_financial_analysis`

## Orchestration
The Airflow DAG `civicpulse_pipeline` triggers the existing Databricks job and waits for it to finish. The Databricks job runs the Bronze, Silver and Gold notebooks in dependency order.

Workspace details and credentials are environment-specific. Never publish access tokens, passwords, secret keys or private configuration values.

## Data quality and limitations
- Some ward-level fields are estimates mapped from older BBMP 198-ward geography to the DataMeet 243-ward geography. These are estimates, not direct measurements for every current ward.
- Pothole complaint data used in the current analysis is from **2022**, not live data.
- Ward-level spending covers **2018–2023** and uses estimated ward mappings.
- Work-order financial amounts can repeat across ward-allocation records. Contractor and work-order aggregates are preliminary until transaction uniqueness and allocation rules are validated.
- Missing contractor names are grouped under `Unknown` in the relevant analysis.
- Some fields are null. If nulls are converted to zero for dashboard KPIs, document that assumption.
- The source refresh frequency is not confirmed; do not describe this as real-time data.

See [`docs/data-quality.md`](docs/data-quality.md).

## Repository structure
```text
civicpulse-bengaluru/
├── README.md
├── .gitignore
├── requirements.txt
├── Dockerfile
├── docker-compose.yaml
├── airflow/dags/civicpulse_pipeline.py
├── docs/
│   ├── architecture.md
│   └── data-quality.md
├── sql/
│   ├── silver/
│   └── gold/
├── src/
│   ├── ingestion/
│   ├── transformations/
│   └── utils/
├── tests/
└── dashboard/screenshots/
```
Adjust the structure to match the files you actually use.

## Local Airflow setup
Prerequisites: Docker Desktop, Docker Compose, and a Databricks workspace with access to the job.

1. Clone the repository.
2. Configure your local Airflow connection named `databricks_default` using your own workspace host and token.
3. Build and start the services:
   ```powershell
   docker compose config --quiet
   docker compose build
   docker compose up -d
   ```
4. Open `http://localhost:8081` if your Compose port mapping is `8081:8080`.
5. Trigger `civicpulse_pipeline` and confirm the Databricks run completes.

**Security:** Review `docker-compose.yaml` before committing. Replace local demo passwords and secret values with environment variables or a secrets mechanism. Do not commit `.env` files, tokens or credentials.

## Dashboard
Add screenshots to `dashboard/screenshots/` after checking that they contain no sensitive information.

## Future improvements
- Add automated data-quality tests and task-level validation.
- Add retries and failure notifications.
- Validate work-order uniqueness before publishing financial totals.
- Consider incremental ingestion if supported by the source and needed by the project.
- Document the source refresh cadence when verified.
