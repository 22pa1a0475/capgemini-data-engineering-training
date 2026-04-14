Insurance Data Engineering Pipeline

Project Overview
This project builds a data pipeline using PySpark and SQL to clean validate and analyze insurance data to find customer risk and agent performance.

Dataset Description
customers contains customer details
policies contains policy information
claims contains claim records
agents contains agent details
policy_agent maps policies to agents

Project Files
insurance_pipeline_problem_statement contains the assignment requirements
insurance_pipeline_starter contains the initial setup code
insurance_data_pipeline_solution contains the complete pipeline implementation
insurance_expected_outputs_validation is used to verify results

Phase 1 Data Understanding
Loaded all tables and checked schema and row counts
No null values found
Negative values and invalid keys identified
Understood customer to policy to claim to agent relationship

Phase 2 Data Cleaning
Removed negative premium and claim values
Standardized string columns
Converted date columns to proper format

Phase 3 Data Validation
Used left anti joins to find invalid records
Removed policies without customers and claims without policies
Validated using record counts

Phase 4 Data Transformation
Joined tables carefully
Calculated total premium and total claim per customer
Computed risk score using claim and premium
Generated city wise summary

Phase 5 Advanced SQL CTE
Created temporary views
Used CTE to rank customers by risk
Found top risky customers per city
Used lag to detect increasing claims

Phase 6 Window Functions
Used row number rank and dense rank
Ranked customers by risk score within city
Ranked agents based on total premium

Phase 7 Final Output
Prepared final datasets and saved outputs

Key Insights
Identified high risk customers
Observed differences across cities
Found top performing agents

Challenges Faced
Handled ambiguous columns after joins
Managed duplicate columns
Fixed NoneType errors

Learnings
Learned data cleaning and validation
Improved understanding of joins and window functions
Built step by step pipeline

Technologies Used
PySpark
SQL
Databricks

Conclusion
The pipeline improved data quality and helped generate useful insights from insurance data.
