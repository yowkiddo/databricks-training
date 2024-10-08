# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

race_df = spark.read.format("delta").load(f"{processed_folder_path}/races")


# COMMAND ----------

race_filtered_df = race_df.filter(race_df.race_year == 2019)
display(race_df)

# COMMAND ----------

race_rename_df = race_df.withColumnRenamed("race_timestamp", "race_date") \
                                          .withColumnRenamed("name", "race_name")

race_selected_df = race_rename_df.select(race_rename_df.race_year, race_rename_df.race_name, race_rename_df.race_date,race_rename_df.race_id,race_rename_df.circuit_id)
display(race_selected_df)

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
                        .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuit_location,circuits_df.circuit_id)
display(circuits_selected_df)

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
                       .withColumnRenamed("name", "driver_name") \
                       .withColumnRenamed("number", "driver_number") \
                       .withColumnRenamed("nationality", "driver_nationality")
drivers_selected_df = drivers_df.select(drivers_df.driver_name,drivers_df.driver_number,drivers_df.driver_nationality,drivers_df.driver_id)
display(drivers_selected_df)

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
                            .withColumnRenamed("name", "team")
constructors_selected_df = constructors_df.select(constructors_df.constructor_id, constructors_df.team)
display(constructors_selected_df)

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
                       .filter(f"file_date = '{v_file_date}'") \
                       .withColumnRenamed("time", "race_time")
results_selected_df = results_df.select(results_df.result_id,
                                        results_df.race_id,
                                        results_df.driver_id,
                                        results_df.constructor_id,
                                        results_df.number,
                                        results_df.grid,
                                        results_df.points,
                                        results_df.race_time,
                                        results_df.fastest_lap_time,
                                        results_df.position,
                                        results_df.file_date
                                        )
display(results_df)

# COMMAND ----------

race_circuit_df = race_selected_df.join(circuits_selected_df , race_selected_df.circuit_id == circuits_selected_df.circuit_id, "inner")

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------

join_final_df = results_selected_df.join(constructors_selected_df, results_selected_df.constructor_id == constructors_selected_df.constructor_id, "left") \
                              .join(drivers_selected_df, results_selected_df.driver_id == drivers_selected_df.driver_id, "left") \
                              .join(race_circuit_df , results_selected_df.race_id == race_circuit_df.race_id, "left")
selected_final_df = join_final_df.select(race_circuit_df.race_year,  
                                race_circuit_df.race_name, 
                                race_circuit_df.race_date,
                                race_circuit_df.circuit_location,
                                drivers_selected_df.driver_name,
                                drivers_selected_df.driver_number,
                                drivers_selected_df.driver_nationality,
                                constructors_selected_df.team,
                                results_selected_df.grid,
                                results_selected_df.fastest_lap_time,
                                results_selected_df.race_time,
                                results_selected_df.points,
                                results_selected_df.position,
                                results_selected_df.race_id,
                                results_selected_df.file_date
                                )
display(selected_final_df)

# COMMAND ----------

final_df = add_ingestion_date(selected_final_df)

# COMMAND ----------

#display(final_df.filter((final_df.race_year == 2020) & (final_df.race_name == 'Abu Dhabi Grand Prix')).orderBy(final_df.points.desc()))

# COMMAND ----------

#final_df.write.mode('overwrite').parquet(f"{presentation_folder_path}/race_result")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_result")
#overwrite_partition(final_df, 'f1_presentation','race_result', 'race_id')

merge_condition ="tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta_date(final_df,'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT file_date FROM f1_presentation.race_results