import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Glue catalog
input_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="covid-db",
    table_name="covid_data_raw",
    transformation_ctx="input_dyf"
)

# Convert to DataFrame
df = input_dyf.toDF()

# Select relevant columns
selected_cols = [
    "iso_code", "continent", "location", "date", "total_cases",
    "new_cases", "total_deaths", "new_deaths", "people_vaccinated",
    "people_fully_vaccinated", "population"
]
df = df.select(*selected_cols)

# Convert data types
df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
for col_name in [
    "total_cases", "new_cases", "total_deaths", "new_deaths",
    "people_vaccinated", "people_fully_vaccinated", "population"
]:
    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

# Drop rows with nulls in essential fields and filter iso-code for country
df = df.dropna(subset=["location", "date", "total_cases","continent"])
df = df.filter(
    (df['iso_code'].isNotNull()) & (~df['iso_code'].startswith('OWID_')))
    
# Convert back to DynamicFrame
final_dyf = DynamicFrame.fromDF(df, glueContext, "final_dyf")

# Write the cleaned data to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={"path": "s3://my-covid-etl-bucket/cleaned/"},
    format="parquet"
)

job.commit()
