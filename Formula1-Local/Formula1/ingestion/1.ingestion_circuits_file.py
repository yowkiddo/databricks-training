# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1. Step 1 - Read the CSV file using the spark dataframe reader.
# MAGIC ### This was retrieve using the 2 commands below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import the neccesarry parameters

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basically changing the data types of circuitId, lat, lng, etc. Using StructType api

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), True), 
                                     StructField("circuitRef", StringType(), True), 
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df = (
    spark.read
    .option("header", True)
    .schema(circuits_schema)
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Display using .show() api from pyspark

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select the columns needed

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    circuits_df.circuitId,
    circuits_df.circuitRef, 
    circuits_df.name, 
    circuits_df.location, 
    circuits_df.country, 
    circuits_df.lat, 
    circuits_df.lng, 
    circuits_df.alt
    )
display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### From the selected columns to Renamed columns.

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_ref")\
    .withColumnRenamed("lat","latitude")\
    .withColumnRenamed("lng","longitutde")\
    .withColumnRenamed("alt","altitude")\
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding column/s with Timestamp it was ingested

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
display(circuits_final_df)

# COMMAND ----------

display(circuits_final_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### To add a column with Literal value, in this case "Production"
# MAGIC ### from pyspark.sql.functions import current_timestamp, lit
# MAGIC ###circuits_final_df = circuits_renamed_df.withColumn("Ingestion Date", current_timestamp()).withColumn("env",lit("Production"))
# MAGIC ### display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### To write in Datake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processsed.circuits")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlcourseev/processsed/circuits

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processsed.circuits

# COMMAND ----------

