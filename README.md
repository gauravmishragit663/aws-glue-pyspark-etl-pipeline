# Serverless Data Lake ETL Pipeline using AWS Glue and PySpark

## Overview

This project demonstrates a serverless ETL pipeline built using AWS Glue and PySpark. The pipeline processes transactional data stored in Amazon S3, applies data cleansing and transformation logic, and stores optimized Parquet output partitioned for analytical workloads.

## Architecture

Raw Data (S3) → Glue Crawler → AWS Glue ETL Job (PySpark) → Processed Data (Parquet, Partitioned)

## Key Features

- Built AWS Glue ETL job using PySpark
- Implemented data cleansing and type casting
- Applied partitioning strategy (year/month/day)
- Optimized performance using repartitioning
- Stored output in Parquet format for analytics
- Schema managed using AWS Glue Data Catalog

## Technologies Used

- AWS Glue
- PySpark
- Amazon S3
- Parquet
- Git

## Performance Optimization

- Used partitioning to reduce scan cost
- Applied repartitioning to minimize shuffle
- Leveraged lazy evaluation in Spark transformations

## Author

Gaurav Mishra
