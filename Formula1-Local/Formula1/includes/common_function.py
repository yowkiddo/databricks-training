# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_partition(df, partition_column):
    # Check if the partition_id column exists in the DataFrame
    if partition_column not in df.columns:
        raise ValueError(f"{partition_column} is not in list")

    # Rearrange the columns to make sure race_id column is at the end
    columns = df.columns
    id_index = columns.index(partition_column)
    rearranged_columns = columns[:id_index] + columns[id_index+1:] + [columns[id_index]]
    
    return df.select(rearranged_columns)

# COMMAND ----------

def merge_delta_date(input_df, db_name, table_name, folder_path, merge_condition, partition_column):   
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    from delta.tables import DeltaTable

    #Check if db_name, table_name exists, if so then creates a table accordingly including the folder path.
    #Then merges the new data with the existing table with the ff. conditions whether records exist or not
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition)\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
        
    #If table already exists it will only overwrite the partition with the new rows to be added
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")