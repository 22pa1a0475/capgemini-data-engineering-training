# ------------------ IMPORTS ------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("Phase3_ETL").getOrCreate()

# ------------------ EXTRACT ------------------
# read customer data from csv
customers_df = spark.read.csv("/samples/customers.csv", header=True, inferSchema=True)

# read customer data from csv
sales_df = spark.read.csv("/samples/sales.csv", header=True, inferSchema=True)

# Inspect schema using show() and printSchema() 
customers_df.show()
sales_df.show()

customers_df.printSchema()
sales_df.printSchema()

# identify missing values
customers_df.selectExpr([
    f"sum(case when {c} is null then 1 else 0 end) as {c}"
    for c in customers_df.columns
]).show()

sales_df.selectExpr([f"sum(case when {c} is null then 1 else 0 end) as{c}"
                    for c in sales_df.columns]).show()

# Clean data using dropna()

customers_clean = customers_df.dropna()
sales_clean = sales_df.dropna()

#Filter invalid records
sales_clean = sales_clean.filter(
    (sales_clean["quantity"] > 0) & (sales_clean["total_amount"] > 0)
)

# ------------------ READ JSON FILE ------------------

# Example JSON read (if file exists)
try:
    products_df = spark.read \
        .option("multiLine", "true") \
        .json("/samples/products.json")
    
    print("Products Data:")
    products_df.show(5)
    
    print("Products Schema:")
    products_df.printSchema()

except:
    print("JSON file not found, skipped.")

# ------------------ READ PARQUET FILE ------------------

# Example Parquet read (if file exists)
try:
    parquet_df = spark.read.format("parquet").load("/samples/titanic.parquet")
    print("Parquet Data:")
    parquet_df.show(5)
    parquet_df.printSchema()
except:
    print("Parquet file not found, skipped.")


#1.calculate daily sales
daily_sales=sales_clean.groupby("sale_date").agg(sum("total_amount").alias("daily_sales"))
daily_sales.show()

# Calculate revenue per city
#join customer and sales
customer_sales=sales_clean.join(customers_clean,"customer_id")
city_revenue=customer_sales.groupBy("city").agg(sum("total_amount").alias("city_revenue")).show()

#Find repeat customers (>2 orders)

repeat_customers = sales_clean.groupBy("customer_id") .agg(count("*").alias("order_count")) .withColumn( "order_count_greater_than_2",when(col("order_count") > 2, col("order_count")).otherwise(0))
repeat_customers.show()

#Find highest spending customer in each city
highest_spending_customer=customer_sales.groupby("customer_id","city").agg(sum("total_amount").alias("total_spend"))
window_spec = Window.partitionBy("city").orderBy(desc("total_spend"))

top_customers = highest_spending_customer.withColumn("rank", rank().over(window_spec)).filter(col("rank") == 1)

top_customers.show()



final_report = customer_sales.groupBy("customer_id", "city") \
    .agg(
        sum("total_amount").alias("total_spend"),
        count("*").alias("order_count")
    )

final_report.show()