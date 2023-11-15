# Databricks notebook source
# MAGIC %run "/Repos/akashpandey74@outlook.com/Credit-Case-Study/notebook/application(df)_data_preparation"

# COMMAND ----------

display(df)

# COMMAND ----------

# Importing the libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df.groupBy(col("TARGET")).count().display()

# COMMAND ----------

df.groupBy(col('CODE_GENDER')).count().display()

# COMMAND ----------

df.select("AMT_INCOME_TOTAL").display()

# COMMAND ----------

df.groupBy(col("NAME_CONTRACT_TYPE")).count().display()

# COMMAND ----------

df.groupBy(col("NAME_TYPE_SUITE")).count().show()

# COMMAND ----------

df.groupBy(col("NAME_EDUCATION_TYPE")).count().show()

# COMMAND ----------

df.groupBy(col("NAME_FAMILY_STATUS")).count().show()

# COMMAND ----------

df.groupBy(col("NAME_HOUSING_TYPE")).count().show()

# COMMAND ----------

df.groupBy(col("OCCUPATION_TYPE")).count().show()

# COMMAND ----------

df.groupBy(col("OCCUPATION_TYPE")).count().show()

# COMMAND ----------

df.display()
