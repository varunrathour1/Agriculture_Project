Agriculture Data Pipeline

This project automates the processing of agriculture crop production data using AWS S3 for storage and Snowflake for data processing. It includes data ingestion, transformation, and dimensional modeling for efficient analysis and reporting.

Features

Automated Data Ingestion: Upload CSV data to AWS S3 and automatically ingest into Snowflake.

Data Transformation: Populate dimensional and fact tables in a star schema for advanced analytics.

KPI Calculation: Generate insights such as yield efficiency, seasonal production trends, and area utilization efficiency.

Scalability: Designed for real-time data ingestion and historical tracking.

Architecture

System Components

AWS S3: Used to store raw CSV files.

Bucket: s3://agriculturebucket/

Folder: upload_csv/

Snowflake: For data staging, transformation, and analysis.

Staging Schema:

agri_stage_tbl

temp_agri_stage_stream_tbl

Dimensions:

state_dim

district_dim

crop_dim

season_dim

year_dim

Fact Table:

agri_fact

Star Schema Representation

Fact Table: Links to all dimensions and stores metrics like area, production, and yield.

Dimension Tables: Include data on states, districts, crops, seasons, and years.

Prerequisites

Python 3.8+

Snowflake account

AWS account with S3 permissions

Required Python libraries:

pandas

boto3

Setup

AWS S3 Configuration

Create an S3 bucket named agriculturebucket.

Grant appropriate permissions to the bucket.

Use the provided Python script to upload your CSV data.

Snowflake Configuration

Create a Snowflake account.

Set up the following:

Storage Integration for accessing S3 data.

Schemas: STAGING and DIMENSIONS.

Tables: Staging, dimension, and fact tables.

Pipes and Streams: Enable automated data ingestion.

Usage

1. Upload CSV Data to S3

import pandas as pd
import boto3

# AWS S3 access
s3 = boto3.client('s3')
bucket_name = 'agriculturebucket'
file_path = 'path_to_your_csv_file.csv'

# Load and upload data
data = pd.read_csv(file_path)
data.to_csv('processed_file.csv', index=False)
s3.upload_file('processed_file.csv', bucket_name, 'upload_csv/processed_file.csv')

2. Snowflake Data Pipeline

Execute the provided SQL scripts in Snowflake:

Staging Setup: Load data into staging tables.

Stream and Pipe: Automate data ingestion.

Dimension and Fact Tables: Populate analytical tables.

3. Generate Insights

Run the provided KPI SQL queries to calculate:

Top 5 districts with the highest yield efficiency.

Seasonal production trends.

State-wise area utilization efficiency.

Arecanut-specific productivity.

Key Performance Indicators (KPIs)

Yield Efficiency by District:

SELECT d.district_name, s.state_name,
       (SUM(f.production) / SUM(f.area)) AS yield_efficiency
FROM AGRICULTURE.DIMENSIONS.agri_fact f
JOIN AGRICULTURE.DIMENSIONS.district_dim d ON f.district_id = d.district_id
JOIN AGRICULTURE.DIMENSIONS.state_dim s ON d.state_id = s.state_id
GROUP BY d.district_name, s.state_name
ORDER BY yield_efficiency DESC
LIMIT 5;

Seasonal Production Trends:

SELECT s.season_name, SUM(f.production) AS total_production
FROM AGRICULTURE.DIMENSIONS.agri_fact f
JOIN AGRICULTURE.DIMENSIONS.season_dim s ON f.season_id = s.season_id
GROUP BY s.season_name
ORDER BY total_production DESC;

Future Enhancements

Integrate climate data for enhanced analysis.

Automate anomaly detection in production trends.

Add predictive modeling for crop yield forecasts.

Contributors

Project Lead: Varun Rathour

Data Source: Kaggle (Uploaded by Ajaypal Singh, Punjab University)

For more details, refer to the project documentation or contact the contributor.

Varun Rathourvarunrathour1@gmail.com
