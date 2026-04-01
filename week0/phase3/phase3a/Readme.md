
## 1. Objective

In this phase, the goal is to work with messy data and apply data cleaning techniques using PySpark. This helps in understanding the importance of data quality before performing any analysis.


## 2. Problem Summary

We worked with a dataset containing customer details with intentional errors.
The tasks included:

1. Identifying null values, duplicates, and invalid data
2. Cleaning the dataset by handling missing and incorrect values
3. Validating the cleaned data
4. Performing aggregation to find customers per city


## 3. Approach

1. Loaded dataset using PySpark
2. Identified data issues:

   * Null values in columns
   * Duplicate records
   * Invalid age values
3. Performed data cleaning:

   * Removed rows with null `customer_id`
   * Replaced missing values with "Unknown"
   * Removed duplicate records
   * Filtered invalid age values (age > 0)
4. Validated results using row counts
5. Performed aggregation using groupBy


## 4. Key Transformations Used

1. `dropna()` → to remove null key values
2. `fillna()` → to handle missing values
3. `dropDuplicates()` → to remove duplicate rows
4. `filter()` → to remove invalid data
5. `groupBy()` → to perform aggregation
6. `count()` → to count customers per city


## 5. Output / Results

The following outputs were generated:

1. Identified null values, duplicates, and invalid records
2. Cleaned dataset with only valid entries
3. Row count before and after cleaning
4. Number of customers in each city


## 6. Data Engineering Considerations

1. Removed null values to maintain data integrity
2. Ensured primary key (`customer_id`) is valid
3. Handled missing values to avoid data loss
4. Removed duplicates to prevent incorrect counts
5. Filtered invalid data to ensure accurate analysis


## 7. Challenges Faced

1. Identifying all types of data issues in the dataset
2. Deciding how to handle missing values properly
3. Ensuring no important data is lost during cleaning
4. Understanding the impact of bad data on results


## 8. Learnings

1. Real-world data is often messy and requires cleaning
2. Data cleaning is a critical step before processing
3. Invalid data can lead to incorrect insights
4. Validation helps ensure correctness of results



## 9. Files in this Folder

1. pyspark code → Implementation of data cleaning in PySpark
2. outputs → Screenshots of results
3. README.md → Documentation of Phase 3A
4.phase3a_problem_statement →
