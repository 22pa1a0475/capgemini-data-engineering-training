Retail Store Sales Lakehouse Project using Databricks DLT

#Objective
The objective of this project is to build an end to end Lakehouse data pipeline using Databricks Delta Live Tables. It focuses on implementing Medallion Architecture, applying data transformations, ensuring data quality, and generating business insights, KPIs, and alerts.

#Problem Summary
This project implements a complete data pipeline using Medallion Architecture with three layers:

#Bronze Layer
Raw data ingestion from multiple sources such as customers, products, sales, stores, and inventory events
Data is stored in its original format along with metadata like ingestion timestamp and source details

#Silver Layer
Data cleaning and validation
Handling null values and type casting
Removing duplicates
Applying data quality checks
Implementing Slowly Changing Dimensions Type 2 for customers, products, and stores
Generating surrogate keys

#Gold Layer
Creation of fact and dimension tables
Generating analytical tables and KPIs
Producing business insights for reporting and dashboards

#Approach

#Data Ingestion
Loaded raw data from storage using Auto Loader
Captured metadata such as ingestion timestamp and source file

#Bronze Layer
Stored raw data without transformations
Maintained original structure for traceability

#Silver Layer
Cleaned and validated data
Applied schema corrections and transformations
Handled missing values and duplicates
Implemented SCD Type 2 logic
Generated surrogate keys using hashing

#Gold Layer
Created fact table for sales transactions
Created dimension tables for customer, product, and store
Performed aggregations and analytics
Generated KPIs and insights

#Key Transformations Used
Data ingestion using Auto Loader
Filtering and validation
groupBy for aggregations
sum, count, and average calculations
Window functions for SCD handling
Hashing for surrogate key generation

#Business Insights Generated
Monthly revenue trends
Daily store sales performance
Top performing products
Customer segmentation based on spending
Payment method analysis
Discount impact on revenue
Inventory risk and stock alerts

#Alert System
Detected out of stock, low stock, and critical inventory levels
Triggered alerts using email notification system

#Optimization Strategy
Converted views into physical tables
Applied optimization techniques for performance
Used file compaction and data clustering for faster queries

#Dashboard Insights
Revenue by category
Top products
Store performance
Customer segmentation
Payment distribution
Discount impact
Inventory alerts

#Technologies Used
Databricks
Delta Live Tables
PySpark
Delta Lake
SQL
AWS S3
SMTP for email alerts

#Limitations
Limited optimization features in free Databricks version
No advanced execution engine support
Limited scheduling capabilities

#Future Enhancements
Real time streaming dashboards
Integration with visualization tools
Implementation of machine learning models for forecasting
Advanced alerting integrations

#Learnings
Understanding of Lakehouse architecture
Experience with Delta Live Tables
Knowledge of SCD Type 2 implementation
Data quality enforcement techniques
Performance optimization strategies
Ability to build end to end data pipelines

#Files in this Folder
Med_Architecture_Assignment contains overall project implementation
medallion_architecture_solution contains Medallion Architecture pipeline logic
Sample dataset contains raw retail data
sample_dataset_solution contains dataset processing and transformations
README.md contains project documentation