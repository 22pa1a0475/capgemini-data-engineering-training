#Medallion Architecture Assignment

#Objective
The objective of this assignment is to understand and implement Medallion Architecture using PySpark and Delta Lake by processing a sample dataset and generating business insights through layered data transformation.

#Problem Summary
This assignment involves working with raw data and building a structured data pipeline using Medallion Architecture. It includes dataset creation, data processing, and generating insights.

#Bronze Layer
Raw dataset ingestion from source
Data is stored without any transformations

#Silver Layer
Data cleaning and preprocessing
Handling null values and incorrect data types

#Gold Layer
Data aggregation and analysis
Generating business insights from processed data

The dataset contains order details such as order id, customer id, product, category, city, order date, amount, and quantity.

#Approach

#Dataset Creation
Created a sample dataset with sales records
Included different products, cities, customers, and transactions

#Data Loading
Loaded dataset into PySpark DataFrame
Verified schema and structure

#Bronze Layer
Stored raw data as it is without applying transformations

#Silver Layer
Handled missing values in dataset
Converted amount and quantity into proper numeric types
Filtered invalid records
Prepared clean dataset for analysis

#Gold Layer
Performed aggregations on cleaned data
Calculated total revenue
Computed total sales by product
Computed total sales by city
Identified top customer
Identified top product

#Key Transformations Used
Data loading using PySpark
Filtering using where conditions
groupBy for aggregation
sum, count for calculations
orderBy for sorting and ranking
Type casting for schema correction

#Business Insights Generated
Total revenue of all orders
Total sales grouped by product
Total sales grouped by city
Top customer based on total spending
Top product based on revenue

#Data Engineering Considerations
Maintained separation of Bronze, Silver, and Gold layers
Ensured schema consistency
Handled null values before aggregation
Validated outputs at each stage
Used structured and readable transformations

#Challenges Faced
Handling null and missing values
Ensuring correct data types
Applying aggregation logic correctly
Understanding Medallion Architecture flow

#Learnings
Understanding of Medallion Architecture
Hands on experience with PySpark and Delta Lake
Ability to clean and transform datasets
Experience in building end to end data pipelines
Improved analytical and problem solving skills

#Files in this Folder
Med_Architecture_Assignment contains overall assignment structure and implementation
medallion_architecture_solution contains Bronze, Silver, and Gold layer implementation
Sample dataset contains raw dataset used for processing
sample_dataset_solution contains dataset processing and transformation steps
README.md contains combined documentation