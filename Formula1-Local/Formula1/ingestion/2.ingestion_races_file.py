# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# COMMAND ----------

from pyspark.sql.functions import current_timestamp 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basically changing the data types of circuitId, lat, lng, etc. Using StructType api

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False), 
                                     StructField("year", IntegerType(), True), 
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

races_df = (
    spark.read
    .option("header", True)
    .schema(races_schema)
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Display using .show() api from pyspark

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select the columns needed; Dropping URL column (unnecessary columns)

# COMMAND ----------

races_selected_df = races_df.select(
    races_df.raceId,
    races_df.year, 
    races_df.round, 
    races_df.circuitId, 
    races_df.name, 
    races_df.date, 
    races_df.time
    )
display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### From the selected columns to Renamed columns.

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("year","race_year")\
    .withColumnRenamed("circuitId","circuit_id")
    
   

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding column/s with Timestamp it was ingested

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, to_timestamp

# COMMAND ----------

# Split the datetime string into two separate columns
races_concat_df = races_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("data_source", lit(v_data_source))

display(races_concat_df)

# COMMAND ----------

races_final_select_df = races_concat_df.withColumn("ingestion_date", current_timestamp()) \
                                        .withColumn("file_date", lit(v_file_date))
display(races_final_select_df)

# COMMAND ----------

races_final_df = races_final_select_df.select(col('race_id'),col('race_year'),col('round'),col('circuit_id'),
                                              col('name'),col('ingestion_date'),col('race_timestamp'),col('data_source'),col('file_date')
)
display(races_final_df)

# COMMAND ----------

races_final_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### To write in Datake as parquet with partition per race_year

# COMMAND ----------

#races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")
races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processsed.races")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlcourseev/processsed/races

# COMMAND ----------

dbutils.notebook.exit("Success")