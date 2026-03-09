"""
02_advanced_transformations.py
------------------------------
AWS Glue ETL Job — Advanced transformations with joins and aggregations.

What this script does:
  1. Reads the raw customers CSV and orders CSV from S3.
  2. Filters orders to keep only COMPLETE orders.
  3. Joins customers with their COMPLETE orders.
  4. Aggregates the total order value and order count per customer.
  5. Writes the enriched dataset to S3 as Parquet, partitioned by country.

Job parameters (passed via --default-arguments or at run time):
  --customers_path : S3 prefix containing the raw customers CSV.
  --orders_path    : S3 prefix containing the raw orders CSV.
  --output_path    : S3 prefix where Parquet output will be written.
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 1. Initialise Glue context
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "customers_path", "orders_path", "output_path"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# 2. Read source data
# ---------------------------------------------------------------------------
def read_csv(path: str) -> "DynamicFrame":
    """Helper: read a CSV file from S3 into a DynamicFrame."""
    return glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [path], "recurse": True},
        format="csv",
        format_options={"withHeader": True, "separator": ","},
    )


customers_dyf = read_csv(args["customers_path"])
orders_dyf    = read_csv(args["orders_path"])

# ---------------------------------------------------------------------------
# 3. Apply schema mappings
# ---------------------------------------------------------------------------
customers_dyf = ApplyMapping.apply(
    frame=customers_dyf,
    mappings=[
        ("customer_id", "string", "customer_id", "int"),
        ("first_name",  "string", "first_name",  "string"),
        ("last_name",   "string", "last_name",   "string"),
        ("email",       "string", "email",        "string"),
        ("country",     "string", "country",      "string"),
        ("signup_date", "string", "signup_date",  "string"),
    ],
)

orders_dyf = ApplyMapping.apply(
    frame=orders_dyf,
    mappings=[
        ("order_id",    "string", "order_id",    "int"),
        ("customer_id", "string", "customer_id", "int"),
        ("product",     "string", "product",     "string"),
        ("amount",      "string", "amount",      "double"),
        ("status",      "string", "status",      "string"),
        ("order_date",  "string", "order_date",  "string"),
    ],
)

# ---------------------------------------------------------------------------
# 4. Convert to Spark DataFrames
# ---------------------------------------------------------------------------
customers_df = customers_dyf.toDF()
orders_df    = orders_dyf.toDF()

print(f"[INFO] Customers: {customers_df.count()} rows")
print(f"[INFO] Orders:    {orders_df.count()} rows")

# ---------------------------------------------------------------------------
# 5. Filter orders — keep only COMPLETE orders
# ---------------------------------------------------------------------------
complete_orders_df = orders_df.filter(F.col("status") == "COMPLETE")
print(f"[INFO] Complete orders: {complete_orders_df.count()} rows")

# ---------------------------------------------------------------------------
# 6. Aggregate orders per customer
# ---------------------------------------------------------------------------
order_summary_df = complete_orders_df.groupBy("customer_id").agg(
    F.count("order_id").alias("order_count"),
    F.round(F.sum("amount"), 2).alias("total_spent"),
    F.max("order_date").alias("last_order_date"),
)

# ---------------------------------------------------------------------------
# 7. Join customers with order summary (left join to keep all customers)
# ---------------------------------------------------------------------------
enriched_df = customers_df.join(order_summary_df, on="customer_id", how="left")

# Replace nulls for customers who have no complete orders
enriched_df = enriched_df.fillna({"order_count": 0, "total_spent": 0.0})

# Add a derived full_name column
enriched_df = enriched_df.withColumn(
    "full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
)

# Select final columns in a clean order
enriched_df = enriched_df.select(
    "customer_id",
    "full_name",
    "email",
    "country",
    "signup_date",
    "order_count",
    "total_spent",
    "last_order_date",
)

enriched_df.show(10, truncate=False)

# ---------------------------------------------------------------------------
# 8. Write output partitioned by country
# ---------------------------------------------------------------------------
output_dyf = DynamicFrame.fromDF(enriched_df, glueContext, "output_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": args["output_path"],
        "partitionKeys": ["country"],
    },
    format="parquet",
    format_options={"compression": "snappy"},
)

print(f"[INFO] Enriched data written to {args['output_path']} (partitioned by country)")

# ---------------------------------------------------------------------------
# 9. Commit job
# ---------------------------------------------------------------------------
job.commit()
