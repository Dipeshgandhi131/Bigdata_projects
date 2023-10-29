# Azure End-to-End Batch Pipeline for Formula1 race (Built on Lakehouse)




<h2>Overview</h2>

This project aims to ingest cutover and delta files present in bronze layer to Delta-table in delta-lake's Silver layer and then join delta tables to store final data into Gold layer's delta tables. BI tool connects to this Gold layer for creating visuals 


<h2>Architecture Diagram</h2>

![architecture diagram](https://github.com/Dipeshgandhi131/Bigdata_projects/assets/91051383/6acea139-c2eb-4eb3-9b05-10bd06c76bfe)

## Project Goals
1. Data Ingestion — Build a mechanism to ingest data from different sources
2. ETL System — We are getting data in raw format, transforming this data into the proper format
3. Data lake — We will be getting data from multiple sources so we need centralized repo to store them
4. Scalability — As the size of our data increases, we need to make sure our system scales with it
5. Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS
6. Reporting — Build a dashboard to get answers to the question we asked earlier


## Services we will be using
1. Azure Data Lake Storage Gen2 (ADLS GEN 2)- Azure's highly scalable & durable data lake storage 
2. Azure Data Factory - orchestration tool to create pipeline to co-ordinate data ingestion and data transformation components
3. Azure Databricks - Notebook for writting & running Pyspark script for data transformations and ingestion
4. Delta Lake - Provides Acid support on top of ADLS GEN 2.
5. Dashboarding tool - PowerBI, Looker, etc connect to delta table through databricks compute cluster

<h2>Dataset Used</h2>
Formula 1 dataset - by Ramesh Retnaswamy

## Delta table created in Gold Layer
1. Race Results
2. Driver standings
3. Constructors Standings

