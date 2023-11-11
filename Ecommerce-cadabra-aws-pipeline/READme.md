<h1>AWS End-to-End Data Engineering Batch Pipeline for Ecommerce-cadabra Application</h1>

<h2>Overview</h2>
This project aims to securely manage, store, and perform analysis on the structured and semi-structured Cadabra server logs. 

## Project Goals
1. Data Ingestion — Build a mechanism to ingest data from different sources
2. ETL System — We are getting data in raw format, transforming this data into the proper format
3. Data lake — We will be getting data from multiple sources so we need centralized repo to store them
4. Scalability — As the size of our data increases, we need to make sure our system scales with it
5. Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS
6. Reporting — Build a dashboard to get answers to the question we asked earlier

<h2>Architecture Diagram</h2>

![aws pipeline](https://github.com/Dipeshgandhi131/Bigdata_projects/assets/91051383/0d396f1c-512f-4341-9b09-ff6658cb7eb0)

## Services we will be using
1. EC2 - Compute that generates Server logs for ecommerce-cadabra backend server
2. AWS Lambda - AWS serverless compute service . Usage to consume from Kinesis data stream and in Kinesis data firehose for apachelog-to-json transformation
3. Redshift - DataWarehouse. Used to process purchaselogs present in S3
4. Athena - Perform Ad-hoc SQL queries on S3 data
5. AWS S3 - AWS highly scalable & durable data lake storage
6. AWS Glue - Serverless ETL tool for running spark scripts and also storing schema in glue data catalog
7. AWS DynamoDB - AWS NoSQL database that supports ACID properties. Used to store ordershistory data
8. Kinesis Data Streams
9. Kinesis Data Firehose
10. OpenSearch Service
11. QuickSight

