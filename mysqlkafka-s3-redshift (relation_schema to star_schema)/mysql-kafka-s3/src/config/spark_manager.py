from pyspark.sql import SparkSession
from .environment import EnvironmentVariable
env = EnvironmentVariable()
spark_session =(SparkSession.builder.master('local[*]').appName('bigdata') 
.config("spark.executor.instances", "1") 
.config("spark.executor.memory", "2g") 
.config("spark.driver.memory", "2g") 
.config("spark.executor.memoryOverhead", "2g")
.config('spark.jars.packages',"org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,mysql:mysql-connector-java:8.0.11,com.amazonaws:aws-java-sdk:1.12.346,org.apache.hadoop:hadoop-aws:3.2.2,com.google.guava:guava:31.1-jre,org.apache.httpcomponents:httpcore:4.4.15")
.getOrCreate())

# setting java system property 
spark_session.sparkContext.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

accessKeyId=env.aws.access_key_id
secretAccessKey=env.aws.secret_access_key

# hadoop-aws configuration 
spark_session._jsc.hadoopConfiguration().set("fs.s3a.access.key", env.aws.access_key_id)
spark_session._jsc.hadoopConfiguration().set("fs.s3a.secret.key", env.aws.secret_access_key)
spark_session._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3-ap-south-1.amazonaws.com')
spark_session._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
spark_session._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
