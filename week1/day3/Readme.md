Week 1 Day 3 – Case When and Window Functions

1. Objective
   In this session, the objective is to understand and apply conditional logic using CASE WHEN and perform advanced analysis using window functions in SQL and PySpark.

2. Problem Summary
   This day consists of two practice modules:

Case When Practice:
Apply conditional logic to classify and segment data
Create new columns based on multiple business rules
Handle complex conditions using logical operators

Window Functions Practice:
Perform ranking and analytical operations on data
Use functions like row number, rank, dense rank, and percent rank
Analyze data without grouping using window specifications

3. Approach
   Worked on case when and window functions as separate tasks

Case When Practice:
Created dataset and applied multiple conditions
Used CASE WHEN in SQL and when() in PySpark
Generated new columns based on business rules

Window Functions Practice:
Defined window specifications using partition and order
Applied ranking functions such as row number, rank, and dense rank
Used analytical functions like lag and lead for comparisons

4. Key Transformations Used
   CASE WHEN for conditional logic in SQL
   withColumn() and when() in PySpark
   Window functions for advanced analysis
   row_number(), rank(), dense_rank(), percent_rank()
   lag() and lead() for accessing previous and next records
   partitionBy() and orderBy() for defining windows

5. Data Engineering Considerations
   Ensured correct order of conditions in CASE WHEN
   Avoided overlapping conditions
   Defined proper partition and ordering in window functions
   Validated results after applying transformations

6. Challenges Faced
   Writing complex conditional logic
   Understanding window function behavior
   Choosing correct window function for each problem
   Debugging incorrect rankings

7. Learnings
   Strong understanding of conditional logic
   Ability to implement business rules using code
   Knowledge of window functions for advanced analytics
   Improved problem solving and analytical skills

8. Files in this Folder
   case and when contains solutions for conditional logic
   case_and_when_problem_statement contains questions
   window_functions contains window function solutions
   WFunctions1 contains window function practice questions
   README.md contains documentation
