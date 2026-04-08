Phase 6 – Spark Playground Exit Sprint Advanced Practice Lab

1. Objective
   In this phase, the goal is to build fluency and confidence in PySpark by practicing joins, window functions, date operations, and executing a complete data pipeline before moving to Databricks.

2. Problem Summary
   This phase focuses on hands on practice using a starter dataset.
   The tasks include:
   Practicing different types of joins
   Applying window functions for ranking and analysis
   Performing date based transformations and aggregations
   Executing a complete data pipeline within a time limit
   Validating results at each step

3. Approach
   Loaded dataset using PySpark
   Inspected data using show() and printSchema()

Performed join operations:
Applied inner join to get valid matching records
Used left join to identify missing values
Applied left anti join to detect invalid foreign keys
Compared row counts to validate joins

Applied window functions:
Ranked top customers within each city
Calculated running total of sales
Ranked customers based on total spend
Used lag function to track previous transactions

Performed date analysis:
Extracted month from date column
Calculated monthly sales aggregation
Computed difference between dates
Analyzed trends based on monthly data

Executed pipeline task:
Loaded and cleaned data
Filtered invalid records
Validated referential integrity
Joined datasets
Applied aggregations and window functions
Saved final output within time limit

4. Key Transformations Used
   join() for combining datasets
   groupBy() for aggregation
   agg() for sum and count calculations
   withColumn() for creating new columns
   filter() for removing invalid data
   Window functions for ranking and running totals
   lag() for accessing previous records
   date functions for extracting and transforming date values

5. Output and Results
   The following outputs were generated:
   Valid joined dataset
   Top customers per city
   Running total of sales
   Monthly sales aggregation
   Trend analysis results
   Final pipeline output stored as phase6_final_detailed_v2

6. Data Engineering Considerations
   Validated joins using row counts and null checks
   Ensured referential integrity between datasets
   Handled missing and invalid data before processing
   Maintained correct data types for date operations
   Verified intermediate results at each stage

7. Challenges Faced
   Understanding different join types and their outputs
   Implementing window functions correctly
   Working with date transformations
   Managing time during pipeline execution
   Debugging errors efficiently

8. Learnings
   Improved understanding of PySpark transformations
   Gained confidence in joins and window functions
   Learned how to debug and validate data
   Improved speed and accuracy in solving problems
   Developed ability to build complete pipelines independently

9. Reflection Questions
   Which task took the most time
   What mistakes were made during implementation
   How issues were debugged
   Ability to build pipeline independently
   Areas that need improvement

10. Files in this Folder
    phase6_problem_statement
    phase6_dirty_starter contains initial dataset and raw practice files
    solution contains PySpark implementation
    OUTPUTS contains screenshots of results
    Readme contains project documentation
