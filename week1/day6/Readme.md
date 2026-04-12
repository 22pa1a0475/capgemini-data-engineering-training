# Car Sales Advanced Pipeline – PySpark & SQL

## Objective

The objective of this project is to build an end-to-end data pipeline using PySpark and SQL to analyze car sales data and generate meaningful business insights, including dealer-level analytics.

---

## Problem Summary

This project covers a complete data pipeline involving:

* Data understanding and preprocessing
* Data cleaning and validation
* Transformations and aggregations
* Dealer-level analytics
* SQL-based analysis

---

## Approach

The project is implemented in a structured phase-wise manner:

### Phase 1 – Data Understanding:

* Loaded datasets for customers, cars, sales, dealers, and sales_dealer
* Checked schema, counts, and identified data quality issues

### Phase 2 – Data Cleaning:

* Handled null values
* Fixed negative prices and invalid quantities
* Trimmed string columns
* Removed invalid foreign keys

### Phase 3 – Validation:

* Used left_anti joins to identify invalid foreign keys
* Created validation report
* Ensured referential integrity

### Phase 4 – Transformation:

* Joined all tables using proper aliasing
* Created revenue column (price × quantity)
* Generated:

  * Customer revenue
  * Brand-wise sales
  * City-wise revenue

### Phase 5 – Dealer Analytics:

* Calculated revenue per dealer
* Identified top dealers using window functions
* Analyzed dealer city contribution

### Phase 6 – SQL Analysis:

* Top 3 customers per city
* Monthly revenue trends
* Repeat customers

### Phase 7 – Output:

* Saved all outputs to storage paths
* Organized results for further analysis

---

## Key Transformations Used

* Joins (inner join, left_anti join)
* withColumn() for derived columns
* groupBy() and aggregation functions
* Window functions (row_number, dense_rank)
* SQL queries for analytical insights
* Column aliasing to avoid ambiguity

---

## Data Engineering Considerations

* Maintained referential integrity across all tables
* Avoided ambiguous column errors using alias and select
* Ensured clean and consistent data before transformations
* Validated outputs at each stage

---

## Challenges Faced

* Resolving ambiguous column errors after joins
* Handling invalid foreign key relationships
* Managing multi-table joins efficiently
* Debugging transformations and SQL queries

---

## Learnings

* Built an end-to-end PySpark data pipeline
* Gained strong understanding of data cleaning and validation
* Learned advanced joins and window functions
* Improved SQL and analytical problem-solving skills

---

## Files in this Folder

* **car_sales_advanced_starter** → Contains initial dataset setup and starter code
* **car_sales_advanced_solution** → Contains complete solution implementation
* **car_sales_pipeline_advanced** → Contains problem statement and pipeline phases
* **README.md** → Project documentation
