-- Databricks notebook source
-- MAGIC
-- MAGIC %python
-- MAGIC html = '<h1 style="color:red;text-align:center;font-family:Calibri">Report on Formula1 Dominant Teams</h1>'
-- MAGIC displayHTML(html)

-- COMMAND ----------

SELECT 
    team_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points,
    RANK() OVER(ORDER BY AVG(points_position) DESC) team_rank
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT 
    team_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points,
    RANK() OVER(ORDER BY AVG(points_position) DESC) team_rank
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT 
    race_year,
    team_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points
    --,RANK() OVER(ORDER BY AVG(points_position) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY race_year, team_name
--HAVING COUNT(1) >= 50
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT 
    race_year,
    team_name,
    COUNT(1) AS total_races,
    SUM(points_position) AS total_points,
    AVG(points_position) AS avg_points
  FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=5 
--AND race_year BETWEEN 2017 AND 2021
                      )
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

