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

# Read the file
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# Filter the dataframe
circuits_filtered_df = circuits_df.filter(circuits_df.circuit_id < 70)

# Rename the column
circuits_filtered_renamed_df = circuits_filtered_df.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_filtered_renamed_df)

# COMMAND ----------

# Left Join the 2 dataframe and select the following columns from 2 dataframes.
race_circuit_df = circuits_filtered_renamed_df.join(race_filter_df, circuits_filtered_renamed_df.circuit_id == race_filter_df.circuit_id, "left") \
                                              .select(circuits_filtered_renamed_df.circuit_name,
                                                 circuits_filtered_renamed_df.location,
                                                 circuits_filtered_renamed_df.country,
                                                 race_df.race_name,
                                                 race_df.round)

display(race_circuit_df)

# COMMAND ----------

# Right Join the 2 dataframe and select the following columns from 2 dataframes.
race_circuit_df = circuits_filtered_renamed_df.join(race_filter_df, circuits_filtered_renamed_df.circuit_id == race_filter_df.circuit_id, "right") \
                                              .select(circuits_filtered_renamed_df.circuit_name,
                                                 circuits_filtered_renamed_df.location,
                                                 circuits_filtered_renamed_df.country,
                                                 race_df.race_name,
                                                 race_df.round)

display(race_circuit_df)

# COMMAND ----------

# Right Join the 2 dataframe and select the following columns from 2 dataframes.
race_circuit_df = circuits_filtered_renamed_df.join(race_filter_df, circuits_filtered_renamed_df.circuit_id == race_filter_df.circuit_id, "full") \
                                              .select(circuits_filtered_renamed_df.circuit_name,
                                                 circuits_filtered_renamed_df.location,
                                                 circuits_filtered_renamed_df.country,
                                                 race_df.race_name,
                                                 race_df.round)

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Semi-Join - 
# MAGIC ### is just like Inner Join but you will only get the columns from Left Table and nothing from the Right Table

# COMMAND ----------

# Semi Join the 2 dataframe and select the following columns from 2 dataframes.
race_circuit_df = circuits_filtered_renamed_df.join(race_filter_df, circuits_filtered_renamed_df.circuit_id == race_filter_df.circuit_id, "semi") \
                                              .select(circuits_filtered_renamed_df.circuit_name,
                                                 circuits_filtered_renamed_df.location,
                                                 circuits_filtered_renamed_df.country)
                                               

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti-Join

# COMMAND ----------

race_circuit_df = circuits_filtered_renamed_df.join(race_filter_df, circuits_filtered_renamed_df.circuit_id == race_filter_df.circuit_id, "anti") 
                                        
                                               

display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-Join

# COMMAND ----------

race_circuit_df = race_filter_df.crossJoin(circuits_filtered_renamed_df)
display(race_circuit_df)

# COMMAND ----------

race_circuit_df.count()

# COMMAND ----------

int(race_filter_df.count()) * int(circuits_filtered_renamed_df.count())

# COMMAND ----------

