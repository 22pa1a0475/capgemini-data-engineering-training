# ------------------ IMPORTS ------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ------------------ SPARK SESSION ------------------
spark = SparkSession.builder.appName("Phase2_Final").getOrCreate()

# ------------------ LOAD DATA ------------------
customers = spark.read.option("header", "true").csv("/samples/customers.csv")
sales = spark.read.option("header", "true").csv("/samples/sales.csv")

# ------------------ CLEANING ------------------
customers = customers.dropna(subset=["customer_id"])
sales = sales.dropna(subset=["customer_id"])


# ------------------ 1. TOTAL AMOUNT ------------------
total_amount = sales.groupBy("customer_id") .agg(round(sum("total_amount"), 2).alias("total_amount"))
total_amount.show()

# ------------------ 2. TOP 3 CUSTOMERS ------------------
top3 = sales.groupBy("customer_id") .agg(round(sum("total_amount"), 2).alias("total_spend")) .orderBy(desc("total_spend")) .limit(3)
top3.show()

# ------------------ 3. CUSTOMERS WITH NO ORDERS ------------------
no_orders = customers.join(sales, "customer_id", "left") .filter(sales.customer_id.isNull()) .select("customer_id", "first_name")
no_orders.show()

# ------------------ 4. CITY-WISE REVENUE ------------------
city_revenue = customers.join(sales, "customer_id") .groupBy("city") .agg(round(sum("total_amount"), 2).alias("total_revenue"))
city_revenue.show()

# ------------------ 5. AVERAGE ORDER AMOUNT ------------------
avg_amount = sales.groupBy("customer_id") .agg(round(avg("order_amount"), 3).alias("avg_amount"))
avg_amount.show()

# ------------------ 6. MORE THAN ONE ORDER ------------------
multi_orders = sales.groupBy("customer_id") .agg(count("*").alias("order_count")) \.filter(col("order_count") > 1)
multi_orders.show()

# ------------------ 7. SORT BY TOTAL SPEND ------------------
sorted_customers = sales.groupBy("customer_id") .agg(round(sum("total_amount"), 2).alias("total_spend")) .orderBy(desc("total_spend"))
sorted_customers.show()
