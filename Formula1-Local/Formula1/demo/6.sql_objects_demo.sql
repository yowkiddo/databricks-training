-- Databricks notebook source
-- MAGIC %md
-- MAGIC Lesson Objectives:
-- MAGIC 1. Spark SQL documentations
-- MAGIC 2. Create a Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Leaning Objectives
-- MAGIC 1. Create the manage table using Python
-- MAGIC 2. Create the manage table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_result")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df.write.mode("overwrite").format("parquet").saveAsTable("demo.race_result_python")

-- COMMAND ----------

USE demo;
SHOW tables;

-- COMMAND ----------

DESC EXTENDED race_result_python;

-- COMMAND ----------

SELECT * FROM demo.race_result_python

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_result_sql
AS 
SELECT * 
FROM demo.race_result_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_result_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Leaning Objectives
-- MAGIC 1. Create the external table using Python
-- MAGIC 2. Create the external table using SQL
-- MAGIC 3. Effect of dropping a external table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_result")
-- MAGIC race_result_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_result_ext_py").saveAsTable("demo.race_result_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_result_ext_py

-- COMMAND ----------

DROP TABLE IF EXISTS demo.race_result_ext_sql;
CREATE TABLE IF NOT EXISTS demo.race_result_ext_sql
(
race_year INT, 
race_name STRING, 
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap_time INT,
race_time STRING,
points FLOAT,
position INT,
creation_date TIMESTAMP
)
USING parquet
LOCATION "/m/formula1courseev/presentation/race_result_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_result_ext_sql
SELECT 
  race_year,
  race_name,
  race_date,
  circuit_location,
  driver_name,
  driver_number,
  driver_nationality,
  team,
  grid,
  CAST(fastest_lap_time AS int) AS fastest_lap_time,
  race_time,
  points,
  position,
  ingestion_date
FROM demo.race_result_ext_py
WHERE race_year = 2020
