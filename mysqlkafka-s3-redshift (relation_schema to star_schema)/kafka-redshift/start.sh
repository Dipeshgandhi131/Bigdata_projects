spark-submit 
--jars RedshiftJDBC4-no-awssdk-1.2.20.1043.jar 
--packages com.databricks:spark-redshift_2.10:2.0.0, # A library to load data into Spark SQL 
                            #DataFrames from Amazon Redshift, and write them back to Redshift tables
           org.apache.spark:spark-avro_2.11:2.4.0, # To load/save data in Avro format,
           com.eclipsesource.minimal-json:minimal-json:0.9.4 # A Minimal JSON Parser and Writer 
           main.py 