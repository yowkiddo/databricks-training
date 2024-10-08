# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read the set of CSV file using the spark dataframe API

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType([StructField("raceId", IntegerType(), False),
                               StructField("driverId", IntegerType(), True),
                               StructField("lap", IntegerType(), True),
                               StructField("position", IntegerType(), True),
                               StructField("time", StringType(), True),
                               StructField("milliseconds", IntegerType(), True)
                               ])

# COMMAND ----------

# Added multiLine function for JSON in multiline format.
lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename the columns and add new columns
# MAGIC ### 1. Rename the driverId and raceId
# MAGIC ### 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                       .withColumnRenamed("driverId", "driver_id")\
                       .withColumn("ingestion_date", current_timestamp()) \
                       .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write the output to processed container in parquet format

# COMMAND ----------

#Make sure to define the merging condition that will meet the requirement
merge_condition ="tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.lap = src.lap"
merge_delta_date(final_df,'f1_processsed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")