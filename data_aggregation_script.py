import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col

# Initialize Spark session and Glue context
spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "50").getOrCreate()
glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Define your S3 path and RDS connection options
s3_path = 's3://transactionsdata97/'  # Update with your S3 path
rds_connection_options = {
    "url": "jdbc:mysql://customersdata97.c56a4ecykcvm.us-east-1.rds.amazonaws.com:3306/customersdata97",
    "user": "rutuja",
    "password": "rutuja051997",
    "dbtable": "aggregated_transactions",
    "customJdbcDriverS3Path": "s3://transactionsdata97/mysql-connector-java.jar",
    "customJdbcDriverClassName": "com.mysql.jdbc.Driver"
}

# Load daily transactions from S3
daily_transactions_df = glueContext.create_dynamic_frame.from_catalog(
    database="customersdata97",  # Update with your Glue catalog database
    table_name="transactionsdata97"    # Update with your Glue catalog table name
).toDF()

# Read existing data from RDS MySQL
existing_data_df = spark.read.format("jdbc").options(
    url=rds_connection_options["url"],
    driver=rds_connection_options["customJdbcDriverClassName"],
    dbtable=rds_connection_options["dbtable"],
    user=rds_connection_options["user"],
    password=rds_connection_options["password"]
).load()

# Perform aggregation
aggregated_daily_df = daily_transactions_df.groupBy("customer_id", "debit_card_number", "bank_name") \
    .agg(spark_sum(col("amount_spend")).alias("total_amount_spend"))

# Merge new data with existing data
merged_df = existing_data_df.alias("existing_data").join(
    aggregated_daily_df.alias("aggregated_daily"),
    existing_data_df["customer_id"] == aggregated_daily_df["customer_id"],
    "full_outer"
).select(
    coalesce(existing_data_df["customer_id"], aggregated_daily_df["customer_id"]).alias("customer_id"),
    coalesce(existing_data_df["debit_card_number"], aggregated_daily_df["debit_card_number"]).alias("debit_card_number"),
    coalesce(existing_data_df["bank_name"], aggregated_daily_df["bank_name"]).alias("bank_name"),
    (coalesce(existing_data_df["total_amount_spend"], 0) + coalesce(aggregated_daily_df["total_amount_spend"], 0)).alias("total_amount_spend")
)

# Write updated data back to RDS MySQL
merged_df.write.format("jdbc").options(
    url=rds_connection_options["url"],
    driver=rds_connection_options["customJdbcDriverClassName"],
    dbtable=rds_connection_options["dbtable"],
    user=rds_connection_options["user"],
    password=rds_connection_options["password"]
).mode("overwrite").save()

job.commit()