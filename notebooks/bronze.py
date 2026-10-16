# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG civicpulse;
# MAGIC
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS civicpulse.bronze.ward_api_raw (
# MAGIC     ward_no INT,
# MAGIC     source_url STRING,
# MAGIC     raw_json STRING,
# MAGIC     fetched_at TIMESTAMP,
# MAGIC     request_status STRING,
# MAGIC     error_message STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------


import requests
from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    StringType, TimestampType
)

ward_no = 42
url = f"https://kaun.city/api/data/wards?ward={ward_no}"

response = requests.get(url, timeout=30)
response.raise_for_status()

raw_json = response.text

# Define the schema explicitly
schema = StructType([
    StructField("ward_no", IntegerType(), False),
    StructField("source_url", StringType(), False),
    StructField("raw_json", StringType(), False),
    StructField("fetched_at", TimestampType(), False),
    StructField("request_status", StringType(), False),
    StructField("error_message", StringType(), True)
])

row = [(
    ward_no,
    url,
    raw_json,
    datetime.now(timezone.utc).replace(tzinfo=None),
    "SUCCESS",
    None
)]

df = spark.createDataFrame(row, schema=schema)

df.write.format("delta").mode("append").saveAsTable(
    "civicpulse.bronze.ward_api_raw"
)

print("Ward 42 successfully inserted into Bronze.")


# COMMAND ----------


import requests
import json
import time
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    StringType, TimestampType
)

TABLE = "civicpulse.bronze.ward_api_raw"
BASE_URL = "https://kaun.city/api/data/wards"

# Configure retries for temporary HTTP failures
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    respect_retry_after_header=True
)

session = requests.Session()
session.mount(
    "https://",
    HTTPAdapter(max_retries=retry_strategy)
)

# Discover available wards
response = session.get(BASE_URL, timeout=30)
response.raise_for_status()

wards_data = response.json()
ward_numbers = sorted({
    int(item["ward_no"])
    for item in wards_data["data"]
    if item.get("ward_no") is not None
})

print("Wards discovered:", len(ward_numbers))

schema = StructType([
    StructField("ward_no", IntegerType(), False),
    StructField("source_url", StringType(), False),
    StructField("raw_json", StringType(), False),
    StructField("fetched_at", TimestampType(), False),
    StructField("request_status", StringType(), False),
    StructField("error_message", StringType(), True)
])

# Exclude wards already successfully stored in Bronze.
# This avoids duplicating ward 42 from our earlier test.
existing_success = {
    row["ward_no"]
    for row in (
        spark.table(TABLE)
        .filter("request_status = 'SUCCESS'")
        .select("ward_no")
        .distinct()
        .collect()
    )
}

pending_wards = [
    ward for ward in ward_numbers
    if ward not in existing_success
]

print("Already ingested successfully:", len(existing_success))
print("Wards to fetch:", len(pending_wards))

success_count = 0
failure_count = 0

for index, ward_no in enumerate(pending_wards, start=1):
    url = f"{BASE_URL}?ward={ward_no}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()

        # Validate that the response is JSON before storing it.
        raw_json = response.text
        json.loads(raw_json)

        row = [(
            ward_no,
            url,
            raw_json,
            datetime.now(timezone.utc).replace(tzinfo=None),
            "SUCCESS",
            None
        )]

        df = spark.createDataFrame(row, schema=schema)

        df.write.format("delta").mode("append").saveAsTable(TABLE)

        success_count += 1

    except Exception as exc:
        row = [(
            ward_no,
            url,
            None,
            datetime.now(timezone.utc).replace(tzinfo=None),
            "FAILED",
            str(exc)[:2000]
        )]

        df = spark.createDataFrame(row, schema=schema)
        df.write.format("delta").mode("append").saveAsTable(TABLE)

        failure_count += 1
        print(f"Failed ward {ward_no}: {exc}")

    if index % 25 == 0 or index == len(pending_wards):
        print(
            f"Progress: {index}/{len(pending_wards)} | "
            f"Success: {success_count} | Failures: {failure_count}"
        )

    # Conservative pacing between requests
    time.sleep(1.2)

print("Ingestion completed.")
print("Successful:", success_count)
print("Failed:", failure_count)

session.close()
