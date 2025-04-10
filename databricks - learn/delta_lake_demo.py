# Databricks notebook source
display(dbutils.fs.ls("/mnt/azuredbformula1dl"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location "/mnt/azuredbformula1dl/demo"

# COMMAND ----------

results_df = spark.read\
    .option("inferSchema", "true")\
    .json("/mnt/azuredbformula1dl/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").saveAsTable("demo.results_manage_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.results_manage_table;

# COMMAND ----------

