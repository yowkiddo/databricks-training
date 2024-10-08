# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe API

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

qualify_schema = StructType([StructField("qualifyId", IntegerType(), False),
                               StructField("raceId", IntegerType(), True),
                               StructField("driverId", IntegerType(), True),
                               StructField("constructorId", IntegerType(), True),
                               StructField("number", IntegerType(), True),
                               StructField("position", IntegerType(), True),
                               StructField("q1", StringType(), True),
                               StructField("q2", StringType(), True),
                               StructField("q3", StringType(), True)
                               ])

# COMMAND ----------

# Added multiLine function for JSON in multiline format.
qualify_df = spark.read \
.schema(qualify_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualify_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename the columns and add new columns
# MAGIC ### 1. Rename the driverId and raceId
# MAGIC ### 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualify_final_df = qualify_df.withColumnRenamed("qualifyId","qualify_id") \
                        .withColumnRenamed("raceId", "race_id") \
                       .withColumnRenamed("driverId", "driver_id")\
                       .withColumnRenamed("constructorId", "constructor_id") \
                       .withColumn("ingestion_date", current_timestamp()) \
                       .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(qualify_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write the output to processed container in parquet format

# COMMAND ----------

#Make sure to define the merging condition that will meet the requirement
merge_condition ="tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_date(qualify_final_df,'f1_processsed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")