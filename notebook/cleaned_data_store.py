# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/Credit-Case-Study/notebook/application(df)_data_preparation"

# COMMAND ----------

df.write.mode("overwrite").csv("wasbs://creditcasestudydata@project99110.blob.core.windows.net/raw-datasets/cleaned-datasets/application-datasets/", header=True)
