# ------------------ IMPORTS ------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ------------------ SPARK SESSION ------------------
spark = SparkSession.builder.appName("Phase3A_DataCleaning").getOrCreate()

# ------------------ CREATE MESSY DATA ------------------
data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("===== ORIGINAL DATA =====")
df.show()

# ------------------ VALIDATION BEFORE CLEANING ------------------
print("Total Rows Before Cleaning:", df.count())

# ------------------ DATA CLEANING ------------------

# 1. Remove rows where customer_id is NULL
df_clean = df.dropna(subset=["customer_id"])

# 2. Handle missing values (fill nulls)
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

# 3. Remove duplicate rows
df_clean = df_clean.dropDuplicates()

# 4. Remove invalid age (age must be > 0)
df_clean = df_clean.filter(col("age") > 0)

# ------------------ CLEANED DATA ------------------
print("===== CLEANED DATA =====")
df_clean.show()

# ------------------ VALIDATION AFTER CLEANING ------------------
print("Total Rows After Cleaning:", df_clean.count())

# ------------------ AGGREGATION ------------------
print("===== CUSTOMERS PER CITY =====")
df_clean.groupBy("city").count().show()

# ------------------ STOP SESSION ------------------
spark.stop()