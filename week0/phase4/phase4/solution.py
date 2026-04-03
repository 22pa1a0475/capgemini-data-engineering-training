#----------imports---------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,count,when,concat
#----------sparksession---------
spark=SparkSession.builder.appName("phase4_mini_project").getOrCreate()
#------------load data----------
sales_df=spark.read.format('csv').option('header','true').load('/samples/sales.csv')
customers_df=spark.read.format('csv').option('header','true').load('/samples/customers.csv')

#------- show original data--------
print("=========#sales data========")
sales_df.show()
print("=========#customers data========")
customers_df.show()

#1.Daily Sales
daily_sales=sales_df.groupBy("sale_date").agg(sum(col("quantity")).alias("total_sales"))
print("========= daily sales========")
daily_sales.show()

#2.City-wise Revenue
city_wise_revenue=customers_df.join(sales_df,"customer_id").groupBy("city").agg(sum(col("total_amount")).alias("Revenue"))
print("===== city-wise Revenue=====")
city_wise_revenue.show()

#3. Top 5 Customers
highest_spend_customers=sales_df.groupBy("customer_id").agg(sum(col("total_amount")).alias("total_spend")).orderBy(col("total_spend").desc()).limit(5)
print("=========Top 5 Customers ========")
highest_spend_customers.show()

#Repeat Customers
repeated_customers=sales_df.groupBy("customer_id").agg(count(col("quantity")).alias("order_count")).filter(col("order_count")>1)
print("=========#Repeat Customers ========")
repeated_customers.show()

#customer segmentation
print("============segmentation===========")
total_spent=sales_df.groupBy("customer_id").agg(sum(col("total_amount")).alias("total_spend"))

customer_segmentation = total_spent.withColumn(
    "segment",
    when(col("total_spend") > 100, "Gold")
    .when((col("total_spend") >= 50) & (col("total_spend") <= 100), "Silver")
    .otherwise("Bronze")
)
customer_segmentation.show()

# Final Reporting Table
print("========= final_table==============")
customer_name=customers_df.withColumn("customer_name",concat(col("first_name"),col("last_name")))
final_table=customer_name.join(customer_segmentation,"customer_id","left").join(repeated_customers,"customer_id","left").select("customer_name", "city", "total_spend", "order_count", "segment")
final_table.show()

final_table.write.mode("overwrite").option("header", "true").csv('/tmp/output/report').show()