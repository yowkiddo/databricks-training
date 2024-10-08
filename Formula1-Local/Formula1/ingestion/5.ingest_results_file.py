# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file (with nested json)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(),True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", StringType(), True),
                                    StructField("statusId", IntegerType(), True)
                                ])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename the columns and add new columns
# MAGIC  

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id")\
                            .withColumnRenamed("raceId", "race_id")\
                            .withColumnRenamed("driverId", "driver_id")\
                            .withColumnRenamed("constructorId", "constructor_id")\
                            .withColumnRenamed("positionText", "position_text")\
                            .withColumnRenamed("positionOrder", "position_order")\
                            .withColumnRenamed("fastestLap", "fastest_lap")\
                            .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                            .withColumnRenamed("fastestLapSpeed", "fatest_lap_speed")

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns
# MAGIC ### 1. statusId
# MAGIC

# COMMAND ----------

results_drop_df = results_renamed_df.drop(results_renamed_df['statusId'])

# COMMAND ----------

display(results_drop_df)

# COMMAND ----------

#Import Timestamp
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add columns for ingestion_date

# COMMAND ----------

results_final_df = results_drop_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("data_source", lit(v_data_source)) \
                                  .withColumn("file_date", lit(v_file_date))
display(results_final_df)

# COMMAND ----------

#Drops the duplicated race_id and driver_id
results_dedupped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Write the output to processed container in paruqet format and partition by race_id
# MAGIC
# MAGIC Added the new line of code for Incremental load
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

#Makes sure to remove the duplicated race_id in case there is. And drop the table if exist because ALTER TABLE command won't work if the table doesn't exist

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processsed.results"):
#         spark.sql(f"ALTER TABLE f1_processsed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode('overwrite').partitionBy('race_id').parquet(f"{processed_folder_path}/results")
#results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processsed.results")

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processsed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

#Make sure to define the merging condition that will meet the requirement
merge_condition ="tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_date(results_dedupped_df,'f1_processsed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlcourseev/processsed/results

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processsed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
# MAGIC

# COMMAND ----------

