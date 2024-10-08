# Databricks notebook source
# MAGIC %pip install requests==2.25.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, StringType, DateType, FloatType, TimestampType 

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("API_JSON_Data").getOrCreate()

# API endpoint (replace this with the actual API you're using)
api_url = "https://data.sfgov.org/resource/nuek-vuh3.json"

# Send a GET request to the API
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Convert the response to a JSON object
    json_data = response.json()
    
    # If necessary, convert the JSON object to a string
    json_str = json.dumps(json_data)
    
    # Load the JSON string into a PySpark DataFrame
    #df = spark.read.json(spark.sparkContext.parallelize([json_str]))
    df = spark.read \
    .format("json") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load([json_str])
    

    # Show the DataFrame
    df.show()
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("API_JSON_Data").getOrCreate()

# API endpoint (replace this with the actual API you're using)
api_url = "https://data.sfgov.org/resource/nuek-vuh3.json"

# Send a GET request to the API
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Convert the response to a JSON object
    json_data = response.json()
    
    # If necessary, convert the JSON object to a string
    json_str = json.dumps(json_data)
    
    # Load the JSON string into a PySpark DataFrame
    df = spark.read.json(spark.sparkContext.parallelize([json_str]))
    
    # Show the DataFrame
    df.show()
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

# COMMAND ----------

display(df)

# COMMAND ----------

df.createGlobalTempView("fire_service_calls_global_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.fire_service_calls_global_view
# MAGIC where als_unit = true

# COMMAND ----------


