"""
01_basic_etl.py
---------------
AWS Glue ETL Job — Basic S3-to-S3 transformation.

What this script does:
  1. Reads a CSV file from S3 (the raw customers dataset).
  2. Drops any rows where 'email' is null.
  3. Adds a derived column 'full_name' combining first and last name.
  4. Writes the cleaned data to S3 in Parquet format.

Job parameters (passed via --default-arguments or at run time):
  --input_path   : S3 prefix containing the raw CSV files.
  --output_path  : S3 prefix where Parquet output will be written.
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import concat_ws, lit

# ---------------------------------------------------------------------------
# 1. Initialise Glue context
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# 2. Read raw CSV data from S3 as a Glue DynamicFrame
# ---------------------------------------------------------------------------
customers_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["input_path"]], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "separator": ","},
)

print(f"[INFO] Raw record count: {customers_dyf.count()}")
customers_dyf.printSchema()

# ---------------------------------------------------------------------------
# 3. Apply schema mapping — rename and cast columns
# ---------------------------------------------------------------------------
mapped_dyf = ApplyMapping.apply(
    frame=customers_dyf,
    mappings=[
        ("customer_id", "string", "customer_id", "int"),
        ("first_name",  "string", "first_name",  "string"),
        ("last_name",   "string", "last_name",   "string"),
        ("email",       "string", "email",        "string"),
        ("country",     "string", "country",      "string"),
        ("signup_date", "string", "signup_date",  "date"),
    ],
)

# ---------------------------------------------------------------------------
# 4. Convert to Spark DataFrame for richer transformations
# ---------------------------------------------------------------------------
customers_df = mapped_dyf.toDF()

# Drop rows where email is null or empty
customers_df = customers_df.filter(
    customers_df["email"].isNotNull() & (customers_df["email"] != lit(""))
)

# Add a derived 'full_name' column
customers_df = customers_df.withColumn(
    "full_name", concat_ws(" ", customers_df["first_name"], customers_df["last_name"])
)

print(f"[INFO] Cleaned record count: {customers_df.count()}")
customers_df.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 5. Convert back to DynamicFrame and write as Parquet
# ---------------------------------------------------------------------------
output_dyf = DynamicFrame.fromDF(customers_df, glueContext, "output_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={"path": args["output_path"]},
    format="parquet",
    format_options={"compression": "snappy"},
)

print(f"[INFO] Data written to {args['output_path']}")

# ---------------------------------------------------------------------------
# 6. Commit the job bookmark
# ---------------------------------------------------------------------------
job.commit()
