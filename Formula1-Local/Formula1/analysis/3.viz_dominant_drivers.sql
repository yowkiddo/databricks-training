-- Databricks notebook source
SELECT 
    driver_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points,
    RANK() OVER(ORDER BY AVG(points_position) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT 
    driver_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points,
    RANK() OVER(ORDER BY AVG(points_position) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
    race_year,
    driver_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points
    --,RANK() OVER(ORDER BY AVG(points_position) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY race_year, driver_name
--HAVING COUNT(1) >= 50
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT 
    race_year,
    driver_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points
  FROM f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10 
--AND race_year BETWEEN 2017 AND 2021
                      )
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

