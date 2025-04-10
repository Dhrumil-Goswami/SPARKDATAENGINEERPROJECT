-- Databricks notebook source
create database if not exists demo

-- COMMAND ----------

show databases

-- COMMAND ----------

DESCRIBE DATABASE demov

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables from demo

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").mode("overwrite").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo; 
show tables;

-- COMMAND ----------

DESCRIBE race_results_python;

-- COMMAND ----------

select * from demo.race_results_python where race_year = 2019;

-- COMMAND ----------

create table demo.race_results_sql
as
select * from demo.race_results_python where race_year = 2019;

-- COMMAND ----------

desc extended demo.race_results_sql;

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

create or replace temp view v_race_results_sql
as
select * from demo.race_results_python where race_year = 2019;

-- COMMAND ----------

select * from v_race_results_sql;

-- COMMAND ----------

create or replace global temp view global_race_results_sql
as
select * from demo.race_results_python where race_year = 2019;

-- COMMAND ----------

show tables in demo;