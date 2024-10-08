# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1dlcourseev/demo'

# COMMAND ----------

result_df = spark.read \
    .option("inferSchema", True) \
    .json("dbfs:/mnt/formula1dlcourseev/raw/2021-03-28/")

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").save("/mnt/formula1dlcourseev/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.result_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dlcourseev/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.result_external

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC UPSERT USING MERGE

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchme", True) \
    .json("dbfs:/mnt/formula1dlcourseev/raw/2021-03-28/drivers.json") \
    .filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("driver_day_1")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read \
    .option("inferSchme", True) \
    .json("dbfs:/mnt/formula1dlcourseev/raw/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 6 AND 15") \
    .select("driverId", "dob", upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))
    
display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("driver_day_2")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read \
    .option("inferSchme", True) \
    .json("dbfs:/mnt/formula1dlcourseev/raw/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
    .select("driverId", "dob", upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))
    
display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge AS target
# MAGIC USING driver_day_1 AS upd
# MAGIC ON target.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET target.dob = upd.dob,
# MAGIC            target.forename = upd.forename,
# MAGIC            target.surname = upd.surname,
# MAGIC            target.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge AS target
# MAGIC USING driver_day_2 AS upd
# MAGIC ON target.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET target.dob = upd.dob,
# MAGIC            target.forename = upd.forename,
# MAGIC            target.surname = upd.surname,
# MAGIC            target.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC ORDER BY driverId;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dlcourseev/demo/drivers_merge')

deltaTable.alias('target') \
  .merge(
    drivers_day3_df.alias('upd'),
    'target.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "driverId": "upd.driverId",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "dob": "upd.dob",
      "driverId": "upd.driverId",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC History, Time Travel, Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-09-23T16:25:05.000+00:00';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2024-09-23T16:25:05.000+00:00').load("/mnt/formula1dlcourseev/demo/drivers_merge")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-09-23T16:25:05.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-09-23T16:25:05.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge AS target
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 6 AS src
# MAGIC   ON (target.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge