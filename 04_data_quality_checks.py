# Databricks notebook source
from pyspark.sql.functions import col, count, when

silver_df = spark.read.format("delta") \
    .load("/Volumes/pyproject/default/data/silver_ecommerce")

null_check = silver_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in silver_df.columns
])

null_check.display()

duplicate_orders = silver_df.groupBy("order_id") \
    .count() \
    .filter(col("count") > 1)

duplicate_orders.display()

invalid_quantity = silver_df.filter(col("quantity") <= 0)
invalid_quantity.display()