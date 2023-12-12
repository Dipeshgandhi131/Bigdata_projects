from awsglue.transforms import *
import memory_profiler
import seaborn
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as func
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import LongType
from awsglue.job import Job
import os
import sys 

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#declaring constant variables
BUCKET_NAME="project1.2-consumer-finance-data"
DYNAMODB_TABLE_NAME="project1.2-consumer-finance-data"
INPUT_FILE_PATH=f"s3://{BUCKET_NAME}/inbox/*json"

#getting logger object to log the progress
logger  = glueContext.get_logger()
logger.info(f"Started reading json file from {INPUT_FILE_PATH}")

fcDf = glueContext.create_dynamic_frame.from_catalog(
        database="project-1.2-consumer-finance-data",
        table_name="project1.2-finance-complaint-inbox",
        transformation_ctx = "s3_finance_new_json"
        )
        
df_sparkdf2 = fcDf.toDF()

dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={"dynamodb.input.tableName": "project1.2-consumer-finance-data",
        "dynamodb.throughput.read.percent": "1.0",
        "dynamodb.splits": "100"
    }
)
dyf_sparkdf=dyf.toDF()

new_sparkdf=None
if dyf_sparkdf.count()!=0:
    existing_complaint_spark_df = dyf_sparkdf.select("complaint_id").withColumnRenamed("complaint_id","existing_complaint_id")
    joined_sparkdf = df_sparkdf.join(existing_complaint_spark_df,df_sparkdf.complaint_id==existing_complaint_spark_df.existing_complaint_id,"left")
    new_sparkdf = joined_sparkdf.filter("existing_complaint_id is null")
    new_sparkdf.drop("existing_complaint_id")
    new_sparkdf=new_sparkdf.repartition(5)
else:
    new_sparkdf=df_sparkdf.repartition(5)


newDynamicFrame= DynamicFrame.fromDF(new_sparkdf, glueContext, "new_sparkdf")

glueContext.write_dynamic_frame_from_options(
    frame=newDynamicFrame,
    connection_type="dynamodb",
    connection_options={"dynamodb.output.tableName":  "project1.2-consumer-finance-data",
        "dynamodb.throughput.write.percent": "1.0"
    }
)
job.commit()
