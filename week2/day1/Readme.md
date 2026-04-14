#Data Engineering Practice Projects

#Project Overview
This repository contains three important SQL and PySpark projects covering core data engineering concepts including subqueries, common table expressions, window functions, and end to end data pipeline design. These projects help in building strong fundamentals required for real world data engineering tasks.

#Projects Included

#Sales Data Analysis using Subqueries and CTE
This project focuses on SQL query writing using subqueries and common table expressions. It involves analyzing employee, department, and order data to generate insights such as salary comparison, department analysis, and order tracking.

#Window Functions using Lead and Lag
This project demonstrates the use of window functions such as lead and lag to analyze sequential data. It helps in understanding trends, comparing rows, and calculating differences between records such as month over month analysis.

#Insurance Data Engineering Pipeline
This project builds a complete data pipeline using PySpark and SQL. It processes customer, policy, claims, and agent data to calculate premium, claims, and risk scores while ensuring data quality and avoiding duplication.

#Technologies Used
PySpark
Spark SQL
Window Functions
Common Table Expressions
Subqueries

Sales Data Analysis

#Tables Used
Employees
Departments
Orders

#Key Concepts
Subqueries for filtering and comparison
CTE for structuring complex queries
Aggregation and grouping
Join operations

#Key Learnings
Understanding nested queries
Improving query readability using CTE
Handling SQL errors and schema validation

#Window Functions

#Key Concepts
Lead function to access next row values
Lag function to access previous row values
Partitioning and ordering in window functions

#Use Cases
Month over month sales comparison
Finding differences between consecutive records
Tracking trends over time

#Key Learnings
Understanding row level comparison
Using partition by for grouped analysis
Applying window functions in real scenarios

Insurance Data Pipeline

#Tables Used
Customers
Policies
Claims
Agents
Policy Agent

Pipeline Steps

#Data Understanding
Loaded data and checked schema and counts

#Data Cleaning
Removed negative premium and claim values
Standardized data formats

#Data Validation
Used left anti join to detect invalid records

#Transformation
Calculated total premium and claims per customer
Computed risk score

#Analysis
Performed city level and customer level analysis
Analyzed agent performance

#Key Learnings
Avoid joining all tables directly to prevent duplication
Aggregate data before joining
Build pipeline step by step
Validate data at every stage


#Challenges Faced

Handling incorrect joins leading to duplicate data
Understanding relationships between multiple tables
Fixing SQL errors and unsupported functions

#Solutions

Used aggregation before joins
Validated data using anti joins
Replaced unsupported SQL operators with alternatives
Used proper column names and schema

#Conclusion
These projects collectively provide a strong foundation in SQL and PySpark. They cover important real world scenarios such as data cleaning, validation, transformation, and analysis. Completing these tasks improves problem solving skills and prepares for data engineering interviews and practical applications
