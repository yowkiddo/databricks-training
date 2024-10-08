-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create circuit table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circtuis (
  circuit_id INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat FLOAT,
  lng FLOAT,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dlcourseev/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circtuis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races (
  race_id INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dlcourseev/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create constructors table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
  )

  USING json
  OPTIONS (path "/mnt/formula1dlcourseev/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create driver table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT <forename STRING, surname STRING>,
  dob DATE,
  nationality STRING,
  URL STRING
  )

  USING json
  OPTIONS (path "/mnt/formula1dlcourseev/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create results table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)

USING json
OPTIONS (path "/mnt/formula1dlcourseev/raw/results.json")



-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create table for pit_stops
-- MAGIC 1. Multi-line JSON
-- MAGIC 2. Simple Structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
  driverId INT,
  raceId INT,
  stop STRING,
  lap INT,
  time STRING,
  duraion STRING,
  milliseconds INT
)

USING json
OPTIONS (path "/mnt/formula1dlcourseev/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Lap Time Table
-- MAGIC 1. CSV file
-- MAGIC 2. Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
  )

  USING csv
  OPTIONS (path "/mnt/formula1dlcourseev/raw/lap_times")


-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Lap Time Table
-- MAGIC 1. JSON file
-- MAGIC 2. Multiple Files
-- MAGIC 3. Multiple Lines

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)

USING json
OPTIONS (path "/mnt/formula1dlcourseev/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying