# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/Credit-Case-Study/notebook/Data_Ingestion"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Loading the datasets

# COMMAND ----------

previous_application_data = spark.read.csv(
                                                previous_application_data_link,
                                                header=True,
                                                inferSchema=True
)

# COMMAND ----------

application_data = spark.read.csv(
                                    application_data_link,
                                        header=True,
                                            inferSchema=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creation of Duplicate of original datasets

# COMMAND ----------

df = application_data.alias("copy")
df1 = previous_application_data.alias("copy")

# COMMAND ----------

df.display()

# COMMAND ----------

df1.display()
