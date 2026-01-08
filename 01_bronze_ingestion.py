# Databricks notebook source
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/pyproject/default/data/ecommerce_sales.csv")

df.display()


# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .save("/Volumes/pyproject/default/data/bronze_ecommerce")


# COMMAND ----------

bronze_df = spark.read.format("delta") \
    .load("/Volumes/pyproject/default/data/bronze_ecommerce")

bronze_df.display()
