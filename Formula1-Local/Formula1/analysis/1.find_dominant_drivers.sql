-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

SELECT 
    driver_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
    driver_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN '2011' AND '2020'
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
    driver_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN '2001' AND '2010'
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

