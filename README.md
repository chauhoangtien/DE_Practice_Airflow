# Practice_Airflow
This project demonstrates an end-to-end layered ETL pipeline orchestrated by Apache Airflow, where data is extracted from **AWS DynamoDB**,
processed through multiple **AWS S3** layers and finally loaded into **PostgreSQL**.

The architecture follows a structed multi-layer design:
- Source Layer – AWS DynamoDB
- Cleansed Layer - AWS S3
- Transform Layer - AWS S3
- Target Layer - PostgreSQL

The goal of this project is to practice:
- Designing producion-like ETL workflows
- Orchestrating pipelines using Airflow DAGs

1. ## Detailed description of the ETL process
For this project, the ETL processing flow will consist of 2 steps:
1. Step 1: Extract raw data from DynamoDB and then load it into the cleansed folder on AWS S3.
2. Step 2: Extract cleansed data from S3, then perform simple transform and finally load into transformed folder on AWS S3, PostgreSQL database

2. ## Architecture Overview
DynamoDB → S3 (Cleansed) → S3 (Transformed) → PostgreSQL
