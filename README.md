# AWS Glue Tutorial

A step-by-step guide to getting started with **AWS Glue** — Amazon's fully managed extract, transform, and load (ETL) service. This tutorial walks through the core concepts, hands-on examples, and sample PySpark scripts you can use in your own ETL pipelines.

---

## Table of Contents

1. [What is AWS Glue?](#1-what-is-aws-glue)
2. [Key Concepts](#2-key-concepts)
3. [Prerequisites](#3-prerequisites)
4. [Repository Structure](#4-repository-structure)
5. [Tutorial Part 1 — Set Up the Data Catalog with a Crawler](#5-tutorial-part-1--set-up-the-data-catalog-with-a-crawler)
6. [Tutorial Part 2 — Write and Run a Glue ETL Job](#6-tutorial-part-2--write-and-run-a-glue-etl-job)
7. [Tutorial Part 3 — Advanced Transformations](#7-tutorial-part-3--advanced-transformations)
8. [Tutorial Part 4 — Deploy Infrastructure with CloudFormation](#8-tutorial-part-4--deploy-infrastructure-with-cloudformation)
9. [Cost Considerations](#9-cost-considerations)
10. [Further Reading](#10-further-reading)

---

## 1. What is AWS Glue?

AWS Glue is a serverless data integration service that makes it easy to discover, prepare, and combine data for analytics, machine learning, and application development. You can:

- **Catalog** your data across multiple data stores using the *Glue Data Catalog*.
- **Crawl** data sources (S3, RDS, Redshift, DynamoDB, and more) to automatically infer schemas.
- **Author ETL jobs** using auto-generated or custom PySpark / Scala scripts.
- **Schedule and orchestrate** workflows with Glue Triggers and Workflows.
- **Query** catalogued data directly from Amazon Athena or Amazon Redshift Spectrum.

---

## 2. Key Concepts

| Concept | Description |
|---|---|
| **Data Catalog** | Central metadata repository. Stores table definitions, schemas, and connection information. Compatible with the Apache Hive Metastore. |
| **Database** | A logical grouping of tables inside the Data Catalog. |
| **Table** | Metadata definition of a data store (schema, location, format). |
| **Crawler** | Connects to a data store, determines the schema, and writes table definitions to the Data Catalog. |
| **ETL Job** | A Spark (PySpark / Scala) or Python Shell script that reads, transforms, and writes data. |
| **DynamicFrame** | A Glue-specific distributed data structure (similar to a Spark DataFrame) with additional schema resolution capabilities. |
| **Connection** | Stores connection details (JDBC URL, credentials) for data stores. |
| **Trigger** | Starts jobs or crawlers on a schedule, on demand, or based on job events. |
| **Workflow** | Chains multiple jobs and crawlers into a directed acyclic graph (DAG). |
| **Bookmark** | Tracks the data that has already been processed so incremental loads work correctly. |

---

## 3. Prerequisites

Before following this tutorial you will need:

1. An **AWS account** with permissions for: `glue:*`, `s3:*`, `iam:PassRole`, `logs:*`.
2. The **AWS CLI** installed and configured (`aws configure`).
3. An **S3 bucket** to store input data, scripts, and output. Replace `<YOUR-BUCKET>` throughout the tutorial.
4. An **IAM role** for Glue with the following managed policies attached:
   - `AWSGlueServiceRole`
   - `AmazonS3FullAccess` (or a least-privilege policy scoped to your bucket)
5. *(Optional)* Python 3.8+ with `boto3` installed for running helper scripts locally.

### Create the IAM role (CLI)

```bash
# Create the role
aws iam create-role \
  --role-name GlueTutorialRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": { "Service": "glue.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach managed policies
aws iam attach-role-policy \
  --role-name GlueTutorialRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
  --role-name GlueTutorialRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

---

## 4. Repository Structure

```
AWS-GLUE-TUTORIAL/
├── README.md                              # This tutorial
├── data/
│   ├── customers.csv                      # Sample customers dataset
│   └── orders.csv                         # Sample orders dataset
├── scripts/
│   ├── 01_basic_etl.py                    # Basic S3-to-S3 ETL job
│   └── 02_advanced_transformations.py     # Joins, filters, and aggregations
└── cloudformation/
    └── glue_infrastructure.yaml           # CloudFormation stack for Glue resources
```

---

## 5. Tutorial Part 1 — Set Up the Data Catalog with a Crawler

### Step 1 — Upload the sample data to S3

```bash
aws s3 cp data/customers.csv s3://<YOUR-BUCKET>/raw/customers/customers.csv
aws s3 cp data/orders.csv    s3://<YOUR-BUCKET>/raw/orders/orders.csv
```

### Step 2 — Create a Glue Database

```bash
aws glue create-database \
  --database-input '{"Name": "glue_tutorial_db", "Description": "AWS Glue Tutorial database"}'
```

### Step 3 — Create a Crawler

```bash
aws glue create-crawler \
  --name glue-tutorial-crawler \
  --role GlueTutorialRole \
  --database-name glue_tutorial_db \
  --targets '{
    "S3Targets": [
      { "Path": "s3://<YOUR-BUCKET>/raw/customers/" },
      { "Path": "s3://<YOUR-BUCKET>/raw/orders/" }
    ]
  }' \
  --table-prefix "raw_"
```

### Step 4 — Run the Crawler and wait for completion

```bash
aws glue start-crawler --name glue-tutorial-crawler

# Poll until the crawler finishes (State becomes READY)
aws glue get-crawler --name glue-tutorial-crawler \
  --query 'Crawler.State' --output text
```

Once the crawler finishes you should see two new tables in the Data Catalog — `raw_customers` and `raw_orders` — inside the `glue_tutorial_db` database.

### Step 5 — Verify the tables

```bash
aws glue get-tables \
  --database-name glue_tutorial_db \
  --query 'TableList[*].{Name:Name,Location:StorageDescriptor.Location}'
```

---

## 6. Tutorial Part 2 — Write and Run a Glue ETL Job

The script [`scripts/01_basic_etl.py`](scripts/01_basic_etl.py) reads the raw customers CSV from S3, applies a simple filter, and writes the result as Parquet back to S3.

### Step 1 — Upload the script to S3

```bash
aws s3 cp scripts/01_basic_etl.py s3://<YOUR-BUCKET>/scripts/01_basic_etl.py
```

### Step 2 — Create the ETL Job

```bash
aws glue create-job \
  --name glue-tutorial-basic-etl \
  --role GlueTutorialRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://<YOUR-BUCKET>/scripts/01_basic_etl.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--job-bookmark-option": "job-bookmark-enable",
    "--TempDir": "s3://<YOUR-BUCKET>/tmp/",
    "--input_path": "s3://<YOUR-BUCKET>/raw/customers/",
    "--output_path": "s3://<YOUR-BUCKET>/processed/customers/"
  }' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X
```

### Step 3 — Run the Job

```bash
aws glue start-job-run --job-name glue-tutorial-basic-etl
```

### Step 4 — Monitor the Job

```bash
# List recent job runs
aws glue get-job-runs \
  --job-name glue-tutorial-basic-etl \
  --query 'JobRuns[0].{RunId:Id,Status:JobRunState,StartedOn:StartedOn}'
```

### Step 5 — Verify the output

```bash
aws s3 ls s3://<YOUR-BUCKET>/processed/customers/ --recursive
```

---

## 7. Tutorial Part 3 — Advanced Transformations

The script [`scripts/02_advanced_transformations.py`](scripts/02_advanced_transformations.py) demonstrates:

- **Join** — customers joined with orders on `customer_id`.
- **Filter** — keep only orders with `status = 'COMPLETE'`.
- **Aggregate** — total order value per customer.
- **Repartition** — write output partitioned by `country`.

### Step 1 — Upload the script to S3

```bash
aws s3 cp scripts/02_advanced_transformations.py \
  s3://<YOUR-BUCKET>/scripts/02_advanced_transformations.py
```

### Step 2 — Create and run the advanced job

```bash
aws glue create-job \
  --name glue-tutorial-advanced \
  --role GlueTutorialRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://<YOUR-BUCKET>/scripts/02_advanced_transformations.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--TempDir": "s3://<YOUR-BUCKET>/tmp/",
    "--customers_path": "s3://<YOUR-BUCKET>/raw/customers/",
    "--orders_path": "s3://<YOUR-BUCKET>/raw/orders/",
    "--output_path": "s3://<YOUR-BUCKET>/processed/customer_orders/"
  }' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X

aws glue start-job-run --job-name glue-tutorial-advanced
```

---

## 8. Tutorial Part 4 — Deploy Infrastructure with CloudFormation

The file [`cloudformation/glue_infrastructure.yaml`](cloudformation/glue_infrastructure.yaml) creates:

- The Glue Data Catalog database
- The S3 crawler
- Both Glue ETL jobs

Deploy the stack:

```bash
aws cloudformation deploy \
  --template-file cloudformation/glue_infrastructure.yaml \
  --stack-name glue-tutorial-stack \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    BucketName=<YOUR-BUCKET> \
    GlueRoleArn=arn:aws:iam::<ACCOUNT-ID>:role/GlueTutorialRole
```

Delete the stack when you are done:

```bash
aws cloudformation delete-stack --stack-name glue-tutorial-stack
```

---

## 9. Cost Considerations

AWS Glue charges based on **Data Processing Units (DPUs)** consumed while a job or crawler is running.

| Resource | Pricing Unit |
|---|---|
| ETL Job (Spark) | Per DPU-hour (billed per second, 1-minute minimum) |
| Python Shell Job | Per DPU-hour (0.0625 DPU fixed) |
| Crawler | Per DPU-hour |
| Data Catalog | First 1 million objects free, then per 100K objects/month |
| Glue Studio | No additional charge |

> **Tip:** Use the `--number-of-workers` and `--worker-type` options to control costs. For small datasets `G.1X` with 2 workers is sufficient.

Always delete jobs, crawlers, and databases you are no longer using to avoid unexpected charges.

---

## 10. Further Reading

- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
- [AWS Glue PySpark Extensions API](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-extensions.html)
- [AWS Glue Pricing](https://aws.amazon.com/glue/pricing/)
- [AWS Glue GitHub Samples](https://github.com/aws-samples/aws-glue-samples)
- [Boto3 Glue Reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html)

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
