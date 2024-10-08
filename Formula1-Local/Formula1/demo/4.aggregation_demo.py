# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.race_year == "2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name"))\
    .withColumnRenamed("sum(points)","total_points") \
    .withColumnRenamed("count(DISTINCT race_name)","number_of_races") \
    .show()

# COMMAND ----------

demo_df.groupBy("driver_name") \
    .agg(sum("points").alias("total_points"), \
            countDistinct("race_name").alias("number_of_races")) \
    .orderBy(sum("points") \
    .desc()) \
    .show()

# COMMAND ----------

demo_grouped_df = race_results_df.filter("race_year IN (2019 , 2020)")

# COMMAND ----------

demo_grouped_df.groupBy("race_year","driver_name") \
    .agg(sum("points").alias("total_points"), \
            countDistinct("race_name").alias("number_of_races")) \
    .orderBy(sum("points") \
    .desc()) \
    .show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

#Uses windows function partitionBy and orderBy to rank the drivers by total points
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

#Group By race year, and driver name. Then creates a new column called rank, which is the rank of the driver in the group
demo_grouped_df.groupBy("race_year","driver_name") \
    .agg(sum("points").alias("total_points"), \
            countDistinct("race_name").alias("number_of_races")) \
    .withColumn("rank", rank().over(driverRankSpec)) \
    .show(100)

# COMMAND ----------

