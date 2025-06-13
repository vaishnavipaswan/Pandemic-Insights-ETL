# COVID-19 Data Analysis using AWS Glue & Redshift Serverless

This project demonstrates an end-to-end ETL pipeline to analyze COVID-19 data using AWS Glue for transformation and Amazon Redshift Serverless for querying.

## 🔧 Technologies Used
- AWS S3
- AWS Glue Crawler & Glue ETL Job
- AWS Glue Data Catalog
- Amazon Redshift Serverless
- SQL (Redshift)
- Pandas (for initial testing)

## 📦 ETL Pipeline Steps
1. Uploaded raw `owid-covid-data.csv` to S3.
2. Created a Glue Crawler to scan and generate metadata.
3. Designed a Glue Job to clean and write Parquet data back to S3.
4. Linked cleaned data to Redshift using External Schema.
5. Performed analysis using Redshift SQL Editor.

## 📊 Key Queries
- Top countries by total cases
- Death-to-case ratio
- Peak daily and 7-day average new cases
- Cases per 100 people

See `sql/SQL QUERIES.txt` for complete query set.

## 📁 Structure
See project structure and file details in the repository.

## 📄 Report
Detailed project steps, issues, resolutions, and outcomes are documented in `docs/ETL.pdf`.
