# Databricks notebook source
silver_df = spark.read.format("delta") \
    .load("/Volumes/pyproject/default/data/silver_ecommerce")
silver_df.display()

# COMMAND ----------

monthly_revenue = silver_df.groupBy("order_year", "order_month") \
    .sum("total_amount") \
    .withColumnRenamed("sum(total_amount)", "monthly_revenue") \
    .orderBy("order_year", "order_month")
monthly_revenue.display()

# COMMAND ----------

from pyspark.sql.functions import col, month, year

top_products = silver_df.groupBy("product") \
    .sum("total_amount") \
    .withColumnRenamed("sum(total_amount)", "product_revenue") \
    .orderBy(col("product_revenue").desc()) \
    .limit(5)
top_products.display()


# COMMAND ----------

city_revenue = silver_df.groupBy("city") \
    .sum("total_amount") \
    .withColumnRenamed("sum(total_amount)", "city_revenue") \
    .orderBy(col("city_revenue").desc())
city_revenue.display()


# COMMAND ----------

payment_dist = silver_df.groupBy("payment_method") \
    .count()
payment_dist.display()
