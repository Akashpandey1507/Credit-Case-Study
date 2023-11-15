# Databricks notebook source
# MAGIC %run "/Repos/akashpandey74@outlook.com/Credit-Case-Study/notebook/application(df)_data_preparation"

# COMMAND ----------

server = secret_sql_server
database = secret_database
username = secret_sql_user
password = secret_sql_pass
#driver= '{ODBC Driver 17 for SQL Server}'
#connection = pyodbc.connect(f'SERVER={server};DATABASE={database};UID={username};PWD={password};Driver={driver}')

# COMMAND ----------

df.write \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{server};databaseName={database}") \
  .option("dbtable", "credit_case_study") \
  .option("user", username) \
  .option("password", password) \
  .mode("overwrite") \
  .save()
