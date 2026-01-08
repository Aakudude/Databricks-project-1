# Databricks notebook source
bronze_df = spark.read.format("delta") \
    .load("/Volumes/pyproject/default/data/bronze_ecommerce")

bronze_df.display()


# COMMAND ----------

bronze_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col, to_date, month, year

silver_df = bronze_df \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("total_amount", col("quantity") * col("price")) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("order_year", year(col("order_date")))
silver_df.display()
silver_df.printSchema()


# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
    .save("/Volumes/pyproject/default/data/silver_ecommerce")
