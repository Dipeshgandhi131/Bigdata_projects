<h1>Azure End-to-End Data Engineering Pipeline for Online Hospitality Agency</h1>

<h2>Overview</h2>
This project aims to securely manage, streamline, and perform analysis on the structured and semi-structured Online Hospitality data based on the hotel room categories and the trending metrics.

## Project Goals
1. Data Ingestion — Build a mechanism to ingest data from different sources
2. ETL System — We are getting data in raw format, transforming this data into the proper format
3. Data lake — We will be getting data from multiple sources so we need centralized repo to store them
4. Scalability — As the size of our data increases, we need to make sure our system scales with it
5. Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS
6. Reporting — Build a dashboard to get answers to the question we asked earlier

<h2>Architecture Diagram</h2>

![Screenshot (443)](https://github.com/Dipeshgandhi131/Bigdata_projects/assets/91051383/ad877ae2-53b7-4274-8c51-c94923819422)

## Services we will be using
1. Azure Data Lake Storage Gen2 - Azure's highly scalable & durable data lake storage 
2. Azure Data Factory - orchestration tool to create pipeline to co-ordinate data ingestion and data transformation components
3. Azure Databricks - Notebook for writting & running Pyspark script for data transformations
4. Azure Synapse analytics - Service to create DataWareHouse to which dashboarding tool connects.
5. Dashboarding tool - PowerBI, Looker, etc 

<h2>Dataset Used</h2>

This dataset contains statistics (CSV files) on daily transactions (bookings & sales of agency's chain of room) happening over the course of many months.
This dataset was obtain from codebasics resume challenge 1 : https://codebasics.io/challenge/codebasics-resume-project-challenge


<h2>Dashboard on power bi</h2>

![powerbi-2](https://github.com/Dipeshgandhi131/Bigdata_projects/assets/91051383/6b39d69e-7ec9-4162-a969-cc54304df795)
