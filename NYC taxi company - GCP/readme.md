<h1>GCP End-to-End Data Engineering Pipeline for NYC-taxi-Company</h1>

<h2>Overview</h2>
This project aims to securely manage, streamline, and perform analysis on the structured and semi-structured NYC taxi company data based on the rides and the trending metrics.

## Project Goals
1. Data Ingestion — Build a mechanism to ingest data from different sources
2. ETL System — We are getting data in raw format, transforming this data into the proper format
3. Data lake — We will be getting data from multiple sources so we need centralized repo to store them
4. Scalability — As the size of our data increases, we need to make sure our system scales with it
5. Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS
6. Reporting — Build a dashboard to get answers to the question we asked earlier

<h2>Architecture Diagram</h2>

![architecture](https://github.com/Dipeshgandhi131/Bigdata_projects/assets/91051383/9ad0de81-c8cd-4b1d-8461-b2150bc89a95)

## Services we will be using
1. GCP Cloud Storage - GCP's highly scalable & durable data lake storage 
2. GCP VM - orchestration tool to create pipeline to co-ordinate data ingestion and data transformation components
3. Mage - Modern orchestration and ETL tool hosted on GCP VM
4. GCP BigQuery - Serverless GCP DataWareHouse Service which contains fact and dimension table 
5. Dashboarding tool - PowerBI, Looker, etc connects to GCP BigQuery

<h2>Dataset Used</h2>

This dataset contains statistics (CSV files) on daily transactions (rides for cabs belonging to company) happening over the course of many months.
1. This dataset was obtain from website : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Dataset dictionary : https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf


<h2> Dashboard on Looker</h2>

![looker-2](https://github.com/Dipeshgandhi131/Bigdata_projects/assets/91051383/261543b1-de77-4382-af1e-3ad0955b2870)
