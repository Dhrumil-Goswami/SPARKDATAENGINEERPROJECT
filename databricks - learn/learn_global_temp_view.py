# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

results_df.createGlobalTempView("g_race_results")
results_df.createOrReplaceTempView("t_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.race_results;