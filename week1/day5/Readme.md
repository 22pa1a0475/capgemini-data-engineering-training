#Week 1 Day 5 – Task 1 README
1. Objective

The objective of this task is to analyze student submission data using:

SQL Joins
Window Functions
Data Cleaning techniques

This helps in identifying inconsistencies, duplicates, and participation.

2. Problem Overview

You are given three datasets:

Master Table
Contains 56 unique students with student_id, college_email, and personal_email
Task1_Responses
Contains 51 submission records based on emails
Task1_File2
Contains 60 records including duplicates, invalid, and extra entries

The goal is to analyze and classify student submissions.

3. Approach
3.1 Data Preparation
Standardized email columns using:
LOWER() to convert to lowercase
TRIM() to remove spaces
Created a unified email mapping table:
Mapped both college and personal emails to a single student_id
3.2 Core Analysis
Students Not Submitted
Used LEFT JOIN between master and submission tables
Valid Submissions
Used INNER JOIN with master table
Invalid Submissions
Identified emails not present in master table
3.3 Duplicate Detection (Window Functions)
Used ROW_NUMBER() with:
PARTITION BY student_id
ORDER BY timestamp
Logic applied:
Row number = 1 → valid submission
Row number > 1 → duplicate submissions
Compared:
GROUP BY (reduces rows)
Window functions (does not reduce rows)
3.4 Advanced Analysis
Counted number of submissions per student
Identified students using both emails
Created final classification:
Submitted
Not Submitted
Duplicate
Invalid
4. Key Concepts Used
4.1 SQL Functions
LOWER()
TRIM()
COALESCE()
JOIN (INNER JOIN, LEFT JOIN)
GROUP BY
4.2 Window Functions
ROW_NUMBER()
PARTITION BY
ORDER BY
5. Important Considerations
Window functions do not reduce rows
GROUP BY reduces rows
Email normalization is important for correct joins
Proper partitioning ensures accurate duplicate detection
6. Challenges Faced
Handling multiple email formats
Identifying duplicates correctly
Differentiating valid and invalid records
Writing efficient join queries
7. Learnings
Understanding real-world data cleaning
Practical use of joins for analysis
Use of window functions for deduplication
Ability to classify and validate datasets
8. Files in Task 1
task1_problem_statement
Contains assignment description
task1_solution
Contains SQL or PySpark implementation
README.md
Contains documentation for Task 1
9. Conclusion

This task represents a real-world data engineering scenario where:

Data contains duplicates and invalid entries
Multiple datasets must be combined
Accurate analysis requires data cleaning, joins, and window functions