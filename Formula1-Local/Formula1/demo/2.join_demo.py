# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

#Read the file and rename the particular Column
race_df = spark.read.parquet(f"{processed_folder_path}/races") \
                    .withColumnRenamed("name", "race_name")

#Filter the Dateframe to 2019
race_filter_df = race_df.filter(race_df.race_year == 2019)

# COMMAND ----------

display(race_filter_df)

# COMMAND ----------

#Read the file and rename the particular Column
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# Inner Join the 2 dataframe and select the following columns from 2 dataframes.
race_circuit_df = circuits_df.join(race_filter_df, circuits_df.circuit_id == race_filter_df.circuit_id, "inner") \
                             .select(circuits_df.circuit_name,
                                    circuits_df.location,
                                    circuits_df.country,
                                    race_df.race_name,
                                    race_df.round)

display(race_circuit_df)

# COMMAND ----------

