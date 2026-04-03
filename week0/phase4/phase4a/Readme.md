Phase 4A – Bucketing and Segmentation in PySpark

# Objective

In this phase, we are required to understand how continuous data can be converted into categories using PySpark and apply different segmentation techniques to generate meaningful insights.

# Problem Summary

We were provided with customer and sales datasets.
The tasks we were required to perform were:
Reading data from CSV files
Joining datasets to create a unified view
Performing aggregation to calculate total spending
Applying different segmentation techniques such as conditional logic, quantiles, bucketizer, and window functions
Comparing different segmentation methods

# Approach

Loaded datasets using PySpark
Inspected data using show() and printSchema()
Joined customer and sales data using customer_id
Created a new column full_name by combining first and last name

Performed aggregation:
o Calculated total_spend using sum()
o Calculated total_orders using count()

Applied segmentation techniques:
o Conditional segmentation using when()
o Quantile-based segmentation using approxQuantile()
o Bucketizer method using MLlib
o Window-based ranking using percent_rank()

Grouped data to count number of customers in each segment
Compared results of different segmentation methods

# Key Transformations Used

groupBy() → for aggregation
agg() → to calculate sum and count
join() → to combine datasets
withColumn() → to create new columns
when() → for segmentation logic
approxQuantile() → for quantile segmentation
Bucketizer → for range-based segmentation
Window functions → for ranking using percent_rank()

# Output / Results

The following outputs were generated:

Customer segmentation using conditional logic
Segment-wise customer count
Quantile-based segmentation results
Bucketizer segmentation output
Window-based ranking output

# Data Engineering Considerations

Ensured correct data types using inferSchema
Handled aggregation carefully to avoid duplication
Used appropriate segmentation logic for meaningful grouping
Maintained clean and structured dataset after transformations

# Challenges Faced

Understanding different segmentation techniques
Implementing quantile-based segmentation
Applying window functions correctly
Comparing outputs from multiple methods

# Learnings

Understanding of bucketing and segmentation concepts
Practical implementation of PySpark transformations
Difference between fixed and data-driven segmentation
Use of window functions for ranking
Ability to analyze and compare different approaches

# Files in this Folder

solution - PySpark implementation for segmentation
phase4a_problem_statement - Problem description
OUTPUTS - Output results
README.md - Documentation
