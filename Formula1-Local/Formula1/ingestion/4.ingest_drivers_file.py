# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest driver.json file (with nested json)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                 ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(),True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename the columns and add new columns
# MAGIC ### 1. driverId renamed to driver_id
# MAGIC ### 2. driverRef renamed to driver_ref
# MAGIC ### 3. Ingestion date added
# MAGIC ### 4. Name added concatination of forename and surname

# COMMAND ----------

# Import current_timestamp for the Ingestion Date part
from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------

# Define the name_schema with the correct attributes
name_schema = StructType([
    StructField("forename", StringType()),
    StructField("surname", StringType())
])

# Fix the code by using the correct attributes from name_schema
drivers_rename_concat_df = drivers_df.withColumnRenamed("driverID", "driver_id") \
                            .withColumnRenamed("driverRef", "driver_ref") \
                            .withColumn("ingestion_date", current_timestamp()) \
                            .withColumn("name",concat(col("name.forename"), lit(' '), col("name.surname"))) \
                            .withColumn("data_source", lit(v_data_source)) \
                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_rename_concat_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop the unwanted columns
# MAGIC ### 1. name.forename
# MAGIC ### 2. name.surname
# MAGIC ### 3. url

# COMMAND ----------

# Dropping unwanted column. In this case the column URL
drivers_final_df = drivers_rename_concat_df.drop(drivers_rename_concat_df['url'])
display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write the output to processed container in paruqet format

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processsed.drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlcourseev/processsed/drivers

# COMMAND ----------

dbutils.notebook.exit("Success")