# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructor_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwated columns from the dataframe

# COMMAND ----------

# Drop the url column
constructor_droppped_df = constructor_df.drop(constructor_df['url'])
display(constructor_droppped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

# Import current_timestamp for the Ingestion Date part
from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

# Rename the columns and adding the columns "ingestion_date" and applying current_timestamp function
constructor_final_df = constructor_droppped_df.withColumnRenamed("constructorId", "constructor_id")\
                                              .withColumnRenamed("constructorRef", "constructor_ref") \
                                              .withColumn("ingestion_date", current_timestamp())   \
                                              .withColumn("data_scource", lit(v_data_source)) \
                                              .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write as parquet into the processed container and create a folder named "constructors"

# COMMAND ----------

# Write the data to parquet and created a separate folder named "constructors"
#constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processsed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlcourseev/processsed/constructors

# COMMAND ----------

dbutils.notebook.exit("Success")