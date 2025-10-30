import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import concat_ws, lit, col, current_date
from awsglue import DynamicFrame
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [args['INPUT_PATH']], "recurse": True},
    transformation_ctx="AmazonS3_node"
)

# Convert Dynamic Frame to Data Frame
df = AmazonS3_node.toDF()
print("Columns in DataFrame:", df.columns)

# Expected columns
expected_cols = ['edited', 'spoiler', 'stickied']

# Only include columns that actually exist in the DataFrame
existing_cols = [c for c in expected_cols if c in df.columns]

if existing_cols:
    new_df = df.withColumn('E-S-S', concat_ws('-', *[col(c) for c in existing_cols]))
else:
    new_df = df.withColumn('E-S-S', lit(None))

# Drop the ones that existed to avoid duplication
for c in existing_cols:
    new_df = new_df.drop(c)

# Add a "date" column for partitioning
new_df = new_df.withColumn("date", current_date())

# Convert back to Dynamic Frame
S3bucket_node_combined = DynamicFrame.fromDF(new_df, glueContext, 'S3bucket_node_combined')

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=AmazonS3_node, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761213683690", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
    )

AmazonS3_node_output = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node_combined,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": args['OUTPUT_PATH'], "partitionKeys": ["date"]},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node_output"
)

job.commit()