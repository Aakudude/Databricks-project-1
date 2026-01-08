# End-to-End E-Commerce Analytics using Databricks & PySpark

## About This Project
This project represents my hands-on learning journey with Databricks and PySpark.
I wanted to go beyond simple data analysis and understand how real-world data
pipelines are built and structured in industry.

To achieve this, I implemented a Bronze–Silver–Gold architecture to process
e-commerce sales data from raw ingestion to business-ready insights.

## Tools Used
- Databricks Community Edition
- PySpark
- Delta Lake

## Project Structure
The pipeline is divided into three layers:

### Bronze Layer (Raw Data)
- Ingested raw CSV data into Databricks
- Stored the data as Delta tables without modifications

### Silver Layer (Cleaned Data)
- Removed duplicate records
- Corrected data types
- Added derived columns like total revenue and order month

### Gold Layer (Analytics & Insights)
- Monthly revenue trends
- Top products by revenue
- City-wise sales performance
- Payment method distribution

## Data Quality Checks
To make the pipeline more realistic, I added basic data validation checks:
- Null value detection
- Duplicate order checks
- Invalid quantity checks

## What I Learned
- How PySpark processes large datasets efficiently
- How Delta Lake helps maintain reliable data layers
- How to design analytics projects using industry best practices
- How to translate raw data into meaningful business insights

## Future Improvements
- Add window functions for advanced analytics
- Schedule pipelines using Databricks Jobs
- Extend the pipeline to support streaming data
