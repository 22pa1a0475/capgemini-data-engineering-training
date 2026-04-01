# Objective

In this phase, we are required to do end-to-end data processing using PySpark, and we have to follow the ETL approach and create insights from the data.

# Problem Summary

We were provided with various types of data, such as customer data, sales data, and other data in different formats like JSON and Parquet.
The tasks we were required to perform were:
Reading data from different sources like CSV, JSON, and Parquet.
Data cleaning and filtering.
Data transformation and aggregation.
Creating insights like daily sales and city revenues.
Identifying repeat and top customers
# Approach

Loaded datasets using PySpark (CSV files)
Inspected data using show() and printSchema()
Checked missing values using aggregation expressions
Performed data cleaning:
o Removed null values using dropna()
o Filtered invalid records (quantity and amount > 0)
Read additional data formats (JSON and Parquet)

# Applied transformations:
o Calculated daily sales
o Joined datasets for city-level insights
o Used window functions for ranking
Generated final aggregated report

# Key Transformations Used
groupBy() → for aggregation
agg() → to calculate sum and count
join() → to combine datasets
filter() → to remove invalid records
dropna() → to handle missing values
Window functions → for ranking (top customers)
withColumn() → to create new columns
when() → for conditional logic

# Output / Results

The following outputs were generated:

Daily sales summary
City-wise total revenue
Repeat customers based on order count
Highest spending customer in each city
Final aggregated customer report

All outputs are saved in the OUTPUTS folder.

# Data Engineering Considerations
Handled missing values before processing
Filtered invalid records to maintain data quality
Used joins carefully to avoid duplication
Applied window functions for advanced analytics
Ensured schema correctness using inferSchema

# Challenges Faced
Handling missing and inconsistent data
Writing window functions for ranking
Managing multiple data sources (CSV, JSON, Parquet)
Understanding complex transformations in PySpark

# Learnings
Complete ETL workflow using PySpark
Handling real-world datasets with cleaning and validation
Using window functions for advanced analysis
Working with multiple file formats
Building final reports from raw data

# Files in this Folder
solution - PySpark ETL implementation
phase3_problem_statement - Problem description
OUTPUTS - Output screenshots
README.md - Documentation