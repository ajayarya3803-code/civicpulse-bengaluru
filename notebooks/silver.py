# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

bronze_df=spark.read.table('civicpulse.bronze.ward_api_raw')

window_df=Window.partitionBy('ward_no').orderBy(col('fetched_at').desc())

latest_df=bronze_df.filter(col('request_status')=='SUCCESS').withColumn('rn',row_number().over(window_df)).filter('rn=1').drop('rn')

silver_wards = (
    latest_df
    .select(
        col("ward_no"),
        get_json_object("raw_json", "$.ward.ward_name").alias("ward_name"),
        get_json_object(
            "raw_json", "$.ward.assembly_constituency"
        ).alias("assembly_constituency"),
        get_json_object("raw_json", "$.ward.zone").alias("zone"),
        current_timestamp().alias("processed_at")
    )
)

display(silver_wards)

# COMMAND ----------

silver_wards.write.format('delta').mode('overwrite').saveAsTable('civicpulse.silver.wards')

# COMMAND ----------


from pyspark.sql.functions import (
    col, get_json_object, current_timestamp
)
from pyspark.sql.window import Window

bronze_df = spark.table("civicpulse.bronze.ward_api_raw")

latest_df = (
    bronze_df
    .filter(col("request_status") == "SUCCESS")
    .select(
        "*",
        __import__("pyspark").sql.functions.row_number().over(
            Window.partitionBy("ward_no").orderBy(col("fetched_at").desc())
        ).alias("rn")
    )
    .filter(col("rn") == 1)
    .drop("rn")
)

silver_infrastructure = latest_df.select(
    col("ward_no"),
    get_json_object("raw_json", "$.infrastructure.signal_count")
        .cast("int").alias("signal_count"),
    get_json_object("raw_json", "$.infrastructure.bus_stop_count")
        .cast("int").alias("bus_stop_count"),
    get_json_object("raw_json", "$.infrastructure.daily_trips")
        .cast("long").alias("daily_trips"),
    current_timestamp().alias("processed_at")
)

display(silver_infrastructure)


# COMMAND ----------

silver_infrastructure.write.format("delta").mode("overwrite").saveAsTable(
    "civicpulse.silver.infrastructure"
)

# COMMAND ----------


from pyspark.sql.functions import (
    col, get_json_object, current_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

bronze_df = spark.table("civicpulse.bronze.ward_api_raw")

window_spec = Window.partitionBy("ward_no").orderBy(
    col("fetched_at").desc()
)

latest_df = (
    bronze_df
    .filter(col("request_status") == "SUCCESS")
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

silver_potholes = latest_df.select(
    col("ward_no"),
    get_json_object("raw_json", "$.potholes.complaints")
        .cast("int").alias("complaints"),
    get_json_object("raw_json", "$.potholes.data_year")
        .alias("data_year"),
    get_json_object("raw_json", "$.potholes.estimate.method")
        .alias("estimate_method"),
    get_json_object("raw_json", "$.potholes.estimate.crosswalk_version")
        .alias("crosswalk_version"),
    get_json_object("raw_json", "$.potholes.estimate.crosswalk")
        .alias("crosswalk_url"),
    current_timestamp().alias("processed_at")
)

display(silver_potholes)


# COMMAND ----------

silver_potholes.write.format("delta").mode("overwrite").saveAsTable(
    "civicpulse.silver.potholes"
)

# COMMAND ----------


from pyspark.sql.functions import (
    col, get_json_object, current_timestamp, row_number
)
from pyspark.sql.window import Window

bronze_df = spark.table("civicpulse.bronze.ward_api_raw")

window_spec = Window.partitionBy("ward_no").orderBy(
    col("fetched_at").desc()
)

latest_df = (
    bronze_df
    .filter(col("request_status") == "SUCCESS")
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

silver_spending = latest_df.select(
    col("ward_no"),
    get_json_object("raw_json", "$.spending.buildings_facilities")
        .cast("long").alias("buildings_facilities"),
    get_json_object("raw_json", "$.spending.drainage")
        .cast("long").alias("drainage"),
    get_json_object("raw_json", "$.spending.roads_and_drains")
        .cast("long").alias("roads_and_drains"),
    get_json_object("raw_json", "$.spending.roads_and_infrastructure")
        .cast("long").alias("roads_and_infrastructure"),
    get_json_object("raw_json", "$.spending.streetlighting")
        .cast("long").alias("streetlighting"),
    get_json_object("raw_json", "$.spending.waste_management")
        .cast("long").alias("waste_management"),
    get_json_object("raw_json", "$.spending.water_and_sanitation")
        .cast("long").alias("water_and_sanitation"),
    get_json_object("raw_json", "$.spending.grand_total")
        .cast("long").alias("grand_total"),
    get_json_object("raw_json", "$.spending.period")
        .alias("spending_period"),
    get_json_object("raw_json", "$.spending.estimate.method")
        .alias("estimate_method"),
    get_json_object("raw_json", "$.spending.estimate.crosswalk_version")
        .alias("crosswalk_version"),
    current_timestamp().alias("processed_at")
)

display(silver_spending)


# COMMAND ----------

silver_spending.write.format("delta").mode("overwrite").saveAsTable(
    "civicpulse.silver.spending"
)

# COMMAND ----------


from pyspark.sql.functions import (
    col, from_json, explode, current_timestamp, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, DoubleType, BooleanType,
    ArrayType
)
from pyspark.sql.window import Window

bronze_df = spark.table("civicpulse.bronze.ward_api_raw")

# Select the latest successful API response per ward
window_spec = Window.partitionBy("ward_no").orderBy(
    col("fetched_at").desc()
)

latest_df = (
    bronze_df
    .filter(col("request_status") == "SUCCESS")
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Define the structure of each work order
work_order_schema = ArrayType(StructType([
    StructField("work_order_id", StringType(), True),
    StructField("ward_no", IntegerType(), True),
    StructField("source_ward_name", StringType(), True),
    StructField("datameet243_no", IntegerType(), True),
    StructField("overlap_share", DoubleType(), True),
    StructField("is_primary", BooleanType(), True),
    StructField("description", StringType(), True),
    StructField("contractor_name", StringType(), True),
    StructField("sanctioned_amount", LongType(), True),
    StructField("net_paid", LongType(), True),
    StructField("deduction", LongType(), True),
    StructField("fy", StringType(), True),
    StructField("data_source", StringType(), True)
]))

parsed_df = latest_df.withColumn(
    "work_orders",
    from_json(col("raw_json"), 
              StructType([
                  StructField("work_orders", work_order_schema, True)
              ]))
    .getField("work_orders")
)

# Convert each array element into a separate row
exploded_df = parsed_df.select(
    col("ward_no").alias("api_ward_no"),
    explode(col("work_orders")).alias("wo")
)

silver_work_orders = exploded_df.select(
    col("api_ward_no"),
    col("wo.work_order_id").alias("work_order_id"),
    col("wo.ward_no").alias("source_ward_no"),
    col("wo.datameet243_no").alias("datameet243_no"),
    col("wo.overlap_share").alias("overlap_share"),
    col("wo.is_primary").alias("is_primary"),
    col("wo.description").alias("description"),
    col("wo.contractor_name").alias("contractor_name"),
    col("wo.sanctioned_amount").alias("sanctioned_amount"),
    col("wo.net_paid").alias("net_paid"),
    col("wo.deduction").alias("deduction"),
    col("wo.fy").alias("financial_year"),
    col("wo.data_source").alias("data_source"),
    current_timestamp().alias("processed_at")
)

display(silver_work_orders)


# COMMAND ----------

silver_work_orders.write.format("delta").mode("overwrite").saveAsTable(
    "civicpulse.silver.work_orders"
)