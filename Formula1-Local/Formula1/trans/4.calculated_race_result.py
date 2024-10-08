# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
    (
        race_year INT,
        team_name STRING,
        driver_id INT,
        driver_name STRING,
        race_id INT,
        position INT,
        points INT,
        calculated_points INT,
        created_date TIMESTAMP,
        updated_date TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
   CREATE OR REPLACE TEMP VIEW race_results_updated
   AS
   SELECT
       races.race_year,
       constructors.name AS team_name,
       drivers.driver_id,
       drivers.name AS driver_name,
       races.race_id,
       results.position,
       results.points,
       11 - results.position AS calculated_points
   FROM f1_processsed.results
   JOIN f1_processsed.drivers ON (results.driver_id = drivers.driver_id)
   JOIN f1_processsed.constructors ON (results.constructor_id = constructors.constructor_id)
   JOIN f1_processsed.races ON (results.race_id = races.race_id)
   WHERE results.position <= 10
   AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
MERGE INTO f1_presentation.calculated_race_results AS target
USING race_results_updated AS upd
ON (target.driver_id = upd.driver_id AND target.race_id = upd.race_id)
WHEN MATCHED THEN 
UPDATE SET target.position = upd.position,
           target.points = upd.points,
           target.calculated_points = upd.calculated_points,
           target.updated_date = current_timestamp
WHEN NOT MATCHED
  THEN INSERT (race_year,
        team_name,
        driver_id,
        driver_name,
        race_id,
        position,
        points,
        calculated_points,
        created_date) 
        
        VALUES (race_year,
        team_name,
        driver_id,
        driver_name,
        race_id,
        position,
        points,
        calculated_points,
        current_timestamp)
""")

# COMMAND ----------

