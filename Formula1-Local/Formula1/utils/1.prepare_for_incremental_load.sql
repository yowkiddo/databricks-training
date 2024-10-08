-- Databricks notebook source
-- MAGIC %md
-- MAGIC Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processsed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processsed
LOCATION "/mnt/formula1dlcourseev/processsed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dlcourseev/presentation"