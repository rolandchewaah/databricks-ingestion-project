# Databricks notebook source
import os
from pyspark.sql import SparkSession

# Optional: Use this to create separate cells in the UI
# COMMAND ----------

print("Starting ingestion...")
input_path = "abfss://raw-data@mystorage.dfs.core.windows.net/uploads/"
checkpoint_path = "abfss://checkpoints@mystorage.dfs.core.windows.net/ingestion_job/"

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(input_path)
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable("main.default.raw_ingested_data"))