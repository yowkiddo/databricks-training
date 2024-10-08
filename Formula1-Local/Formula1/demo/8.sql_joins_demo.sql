-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

DESC driver_standings

-- COMMAND ----------

REFRESH TABLE f1_presentation.driver_standings

-- COMMAND ----------

SELECT * FROM f1_presentation.driver_standings

-- COMMAND ----------

SELECT race_year, driver_name, team, total_points, wins, rank
FROM f1_presentation.driver_standings
WHERE race_year = 2020;

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW v_driver_standings_2018 AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020 AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020

-- COMMAND ----------

--Drivers that exists in both 2018 & 2020 using Inner Join
SELECT *
FROM v_driver_standings_2018 d_2018
JOIN v_driver_standings_2020 d_2020
 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--Driver that race in 2018 but not in 2020 using Left Join
SELECT *
FROM v_driver_standings_2018 d_2018
LEFT JOIN v_driver_standings_2020 d_2020
 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--Driver that race in 2020 but not in 2018 from the Left table using Right Join
SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2020 d_2020
 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--Driver that race either in 2020 or in 2018 using Full Join
SELECT *
FROM v_driver_standings_2018 d_2018
FULL JOIN v_driver_standings_2020 d_2020
 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Semi-Join
-- MAGIC

-- COMMAND ----------

--Semi-Join is like Inner Join but only retrieves the rows that race in both 2018 & 2020 BUT only whats in the Left Table
SELECT *
FROM v_driver_standings_2018 d_2018
SEMI JOIN v_driver_standings_2020 d_2020
 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Anti-Join

-- COMMAND ----------

--Anti-Join is like Inner Join but only retrieves the rows that drivers have not race in 2020
SELECT *
FROM v_driver_standings_2018 d_2018
ANTI JOIN v_driver_standings_2020 d_2020
 ON d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CROSS-JOIN

-- COMMAND ----------

--Cross Join is the cartisian product of 2 tables. 
SELECT *
FROM v_driver_standings_2018 d_2018
CROSS JOIN v_driver_standings_2020 d_2020

-- COMMAND ----------

