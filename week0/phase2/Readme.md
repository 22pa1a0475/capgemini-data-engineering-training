# Objective

In this phase, the goal is to perform data transformations using PySpark and understand how to apply SQL concepts like joins and aggregations on real datasets.

# Problem Summary

We worked with customer and sales datasets to perform data analysis and generate insights.
The tasks included:

Calculating total amount spent by each customer
Identifying top 3 customers
Finding customers with no orders
Generating city-wise revenue
Calculating average order amount
Finding customers with multiple orders
Sorting customers based on total spend
# Approach

Loaded datasets using PySpark
Performed data cleaning:
o Removed null values in customer_id
Applied transformations:
o Used groupBy for aggregations
o Calculated sum, average, and count
Performed joins between customers and sales datasets
Applied sorting and filtering to generate final results

# Key Transformations Used
groupBy() → for grouping data
agg() → to calculate sum, avg, count
join() → to combine datasets
filter() → to apply conditions
orderBy() → for sorting
round() → to handle decimal precision

# Output / Results

The following outputs were generated:

Total amount spent by each customer
Top 3 customers based on spending
Customers with no orders
City-wise total revenue
Average order amount per customer
Customers with more than one order
Sorted customer list by total spend

All outputs are saved in the outputs folder.

# Data Engineering Considerations
Removed null values to ensure accurate processing
Maintained consistent column usage across transformations
Applied rounding to avoid floating-point precision issues
Verified results after each step

# Challenges Faced
Confusion between column names (order_amount vs total_amount)
Writing correct aggregation logic
Handling decimal precision in outputs
Understanding join behavior in PySpark

# Learnings
How to perform aggregations in PySpark
Practical use of joins in real datasets
Importance of data cleaning before transformation
Improved understanding of PySpark workflow

# Files in this Folder
pyspark code → PySpark implementation
phase2_problem_statement → Problem description
outputs → Output screenshots
README.md → Documentation