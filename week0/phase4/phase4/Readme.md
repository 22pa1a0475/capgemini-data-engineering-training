1. Objective

In this phase, the goal is to perform end-to-end data processing using PySpark by following the ETL approach and generating meaningful insights from sales and customer data.

2. Problem Summary

We worked with customer and sales datasets to analyze data and generate insights. The tasks included:

Calculating daily sales
Generating city-wise revenue
Identifying top 5 customers based on spending
Finding repeat customers
Performing customer segmentation (Gold, Silver, Bronze)
Creating a final aggregated reporting table
3. Approach
Loaded datasets using PySpark (CSV format)
Displayed data using show() for verification
Performed transformations:
Calculated daily sales using groupBy
Joined datasets to generate city-wise revenue
Identified top customers using sorting and limit
Found repeat customers using aggregation and filtering
Applied customer segmentation using conditional logic (when)
Combined all results to create a final reporting table
Saved output as CSV file
4. Key Transformations Used
groupBy() → for aggregation
agg() → to calculate sum and count
join() → to combine datasets
filter() → to apply conditions
orderBy() → for sorting
limit() → to get top records
withColumn() → to create new columns
when() → for conditional logic
concat() → to combine first and last names
5. Output / Results

The following outputs were generated:

Daily sales summary
City-wise total revenue
Top 5 customers based on spending
Repeat customers (order count > 1)
Customer segmentation (Gold, Silver, Bronze)
Final reporting table with customer details

The final report is saved in:
/tmp/output/report

6. Data Engineering Considerations
Used joins to combine customer and sales data
Applied aggregation functions for accurate insights
Used filtering to identify repeat customers
Applied conditional logic for segmentation
Ensured proper column selection in final output
Saved output in structured format (CSV with header)

7. Challenges Faced
Understanding join operations between datasets
Writing correct aggregation queries
Applying conditional logic for segmentation
Managing multiple transformations in a single pipeline
Handling column names consistently

8. Learnings
Learned complete ETL workflow using PySpark
Gained hands-on experience with joins and aggregations
Understood how to generate business insights from raw data
Learned customer segmentation techniques
Improved ability to build end-to-end data pipelines

9. Files in this Folder
solution.py → PySpark implementation of Phase 4
README.md → Documentation of Phase 4
OUTPUTS → Screenshots of results