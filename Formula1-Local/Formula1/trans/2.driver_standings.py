# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

race_result_list = (
    spark.read.format("delta").load(f"{presentation_folder_path}/race_results")
    .filter(f"file_date = '{v_file_date}'")
    .select("race_year")
    .distinct()
    .collect()
)

# COMMAND ----------

race_result_list

# COMMAND ----------

race_year_list = []
for race_year in race_result_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col
race_result_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_result_df)

# COMMAND ----------

race_result_filter_df = race_result_df.filter(race_result_df.race_year == '2020')

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count


driver_standings_df = race_result_df \
    .groupBy("race_year", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"), \
         count(when(col("position") == 1, True)).alias("wins")) \
    

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.functions import desc, rank, asc
from pyspark.sql.window import Window

race_standing_specs = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
driver_standings_final_df = driver_standings_df.withColumn("rank", rank().over(race_standing_specs))

# COMMAND ----------

final_df = driver_standings_final_df

# COMMAND ----------

display(final_df)

# COMMAND ----------

#final_df.write.mode('overwrite').parquet(f"{presentation_folder_path}/driver_standings")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
#overwrite_partition(final_df, 'f1_presentation','driver_standings', 'race_year')
merge_condition ="tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name"
merge_delta_date(final_df,'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT race_year FROM f1_presentation.driver_standings