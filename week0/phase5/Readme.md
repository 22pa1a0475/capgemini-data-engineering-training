Phase 5 – Databricks and Olist End to End Data Engineering Pipeline

1. Objective
   In this phase, we worked with a real world dataset and built a complete data engineering pipeline using PySpark in Databricks. The goal was to perform data ingestion, transformation, analysis, and generate meaningful insights using multiple datasets.

2. Problem Summary
   We were provided with the Olist e commerce dataset containing multiple tables such as customers, orders, products, order items, and payments. 
   The tasks we were required to perform were:
   Reading multiple datasets from CSV files
   Joining different tables to create meaningful relationships
   Performing aggregations and calculations
   Using window functions for advanced analytics
   Creating customer segmentation
   Building a final reporting dataset

3. Approach
   Loaded all datasets using PySpark from Databricks storage
   Verified data using show() and schema inspection

Performed data integration:
Joined customers, orders, and payments for customer level analysis
Joined order items and products for product level insights

Applied transformations:
Calculated total spend per customer
Computed daily sales
Aggregated product sales by category

Used window functions:
Ranked customers within each city
Calculated running total of sales
Identified top products per category

Created segmentation:
Categorized customers into Gold, Silver, and Bronze based on total spend

Generated final dataset:
Combined customer, city, spend, segment, and order count into a single report

4. Key Transformations Used
   groupBy() to perform aggregations
   agg() to calculate sum and count
   join() to combine multiple datasets
   withColumn() to create new columns
   when() to apply conditional logic
   Window functions to perform ranking and cumulative calculations
   dense_rank() and rank() for ordering results
   to_date() for date conversion

5. Output and Results
   The following outputs were generated:
   Top 3 customers in each city based on total spending
   Daily sales along with running total
   Top selling products in each category
   Customer lifetime value showing total spend
   Customer segmentation into Gold, Silver, and Bronze
   Final reporting table containing customer insights

6. Data Engineering Considerations
   Handled joins carefully to maintain correct relationships
   Ensured proper aggregation to avoid duplication
   Used window functions for advanced insights
   Maintained consistency in column naming and data types
   Validated intermediate outputs for correctness

7. Challenges Faced
   Working with multiple large datasets
   Understanding relationships between different tables
   Implementing window functions correctly
   Managing joins without creating duplicate data
   Ensuring accurate aggregation results

8. Learnings
   Understanding of end to end data pipeline using PySpark
   Hands on experience with real world dataset
   Ability to perform complex joins and aggregations
   Use of window functions for ranking and cumulative analysis
   Building final reporting datasets from multiple sources

9. Files in this Folder
   solution contains PySpark implementation
   phase5_problem_statement contains task details
   OUTPUTS contains results and screenshots
   README.md contains project documentation
