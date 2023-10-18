<h1>AWS End-to-End Data Engineering Batch Pipeline for Consumer Finance complaint Data</h1>

<h2>Overview</h2>
This project aims to securely manage, store, and perform analysis on the structured and semi-structured consumer finance complaint data 

## Project Goals
1. Data Ingestion — Build a mechanism to ingest data from different sources
2. ETL System — We are getting data in raw format, transforming this data into the proper format
3. Data lake — We will be getting data from multiple sources so we need centralized repo to store them
4. Scalability — As the size of our data increases, we need to make sure our system scales with it
5. Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS
6. Reporting — Build a dashboard to get answers to the question we asked earlier

<h2>Architecture Diagram</h2>

![project 1 2](https://user-images.githubusercontent.com/91051383/226414916-ddbced76-cedf-44c5-9c89-38679ad89263.png)

## Services we will be using
1. AWS Lambda - AWS serverless compute service. We used it to consume API and store data into S3
2. AWS EventBridge - To Schedule execution of lambda function
3. AWS S3 - AWS highly scalable & durable data lake storage
4. AWS Glue - Serverless ETL tool for running spark scripts and also storing schema in glue data catalog
5. MongoDB - NoSQL database
6. AWS DynamoDB - AWS NoSQL database that supports ACID properties

<h2>Dataset Used</h2>
 
1. This dataset was obtain from api : "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=<todate>&date_received_min=<fromdate>&field=all&format=json"
