# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE civicpulse.gold.ward_infrastructure_overview
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     w.ward_no,
# MAGIC     w.ward_name,
# MAGIC     w.assembly_constituency,
# MAGIC     w.zone,
# MAGIC     i.signal_count,
# MAGIC     i.bus_stop_count,
# MAGIC     i.daily_trips,
# MAGIC     p.complaints AS pothole_complaints,
# MAGIC     p.data_year AS pothole_data_year,
# MAGIC     p.estimate_method AS pothole_estimate_method
# MAGIC FROM civicpulse.silver.wards AS w
# MAGIC LEFT JOIN civicpulse.silver.infrastructure AS i
# MAGIC     ON w.ward_no = i.ward_no
# MAGIC LEFT JOIN civicpulse.silver.potholes AS p
# MAGIC     ON w.ward_no = p.ward_no;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE civicpulse.gold.ward_spending_analysis
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     w.ward_no,
# MAGIC     w.ward_name,
# MAGIC     w.assembly_constituency,
# MAGIC     s.buildings_facilities,
# MAGIC     s.drainage,
# MAGIC     s.roads_and_drains,
# MAGIC     s.roads_and_infrastructure,
# MAGIC     s.streetlighting,
# MAGIC     s.waste_management,
# MAGIC     s.water_and_sanitation,
# MAGIC     s.grand_total AS reported_spending_total,
# MAGIC     s.spending_period,
# MAGIC     s.estimate_method,
# MAGIC     s.crosswalk_version
# MAGIC FROM civicpulse.silver.wards AS w
# MAGIC LEFT JOIN civicpulse.silver.spending AS s
# MAGIC     ON w.ward_no = s.ward_no;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE civicpulse.gold.contractor_work_order_analysis
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     api_ward_no,
# MAGIC     contractor_name,
# MAGIC     data_source,
# MAGIC     financial_year,
# MAGIC     COUNT(*) AS work_order_record_count,
# MAGIC     COUNT(DISTINCT work_order_id) AS distinct_work_order_ids,
# MAGIC     SUM(sanctioned_amount) AS total_sanctioned_amount,
# MAGIC     SUM(net_paid) AS total_recorded_net_paid,
# MAGIC     SUM(deduction) AS total_recorded_deductions,
# MAGIC     COUNT_IF(net_paid IS NULL) AS records_missing_net_paid
# MAGIC FROM civicpulse.silver.work_orders
# MAGIC GROUP BY
# MAGIC     api_ward_no,
# MAGIC     contractor_name,
# MAGIC     data_source,
# MAGIC     financial_year;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC     civicpulse.gold.source_work_order_summary
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH source_work_orders AS (
# MAGIC     SELECT
# MAGIC         work_order_id,
# MAGIC         source_ward_no,
# MAGIC         contractor_name,
# MAGIC         financial_year,
# MAGIC         data_source,
# MAGIC         MAX(sanctioned_amount) AS sanctioned_amount,
# MAGIC         MAX(net_paid) AS net_paid,
# MAGIC         MAX(deduction) AS deduction,
# MAGIC         COUNT(*) AS ward_allocation_record_count,
# MAGIC         SUM(overlap_share) AS total_overlap_share,
# MAGIC         COUNT_IF(net_paid IS NULL) AS records_missing_net_paid
# MAGIC     FROM civicpulse.silver.work_orders
# MAGIC     WHERE work_order_id IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         work_order_id,
# MAGIC         source_ward_no,
# MAGIC         contractor_name,
# MAGIC         financial_year,
# MAGIC         data_source
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM source_work_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC     civicpulse.gold.contractor_financial_analysis
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     COALESCE(contractor_name, 'Unknown')
# MAGIC         AS contractor_name,
# MAGIC     financial_year,
# MAGIC     data_source,
# MAGIC     COUNT(*) AS source_work_order_count,
# MAGIC     SUM(sanctioned_amount) AS total_sanctioned_amount,
# MAGIC     SUM(net_paid) AS total_net_paid,
# MAGIC     SUM(deduction) AS total_deductions,
# MAGIC     COUNT_IF(net_paid IS NULL) AS groups_missing_net_paid
# MAGIC FROM civicpulse.gold.source_work_order_summary
# MAGIC GROUP BY
# MAGIC     COALESCE(contractor_name, 'Unknown'),
# MAGIC     financial_year,
# MAGIC     data_source;