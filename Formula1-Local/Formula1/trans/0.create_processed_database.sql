-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processsed
LOCATION "/mnt/formula1dlcourseev/processsed"

-- COMMAND ----------

DESC DATABASE f1_processsed

-- COMMAND ----------

SHOW TABLES FROM f1_processsed

-- COMMAND ----------

