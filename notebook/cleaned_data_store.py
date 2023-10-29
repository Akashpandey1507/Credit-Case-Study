# Databricks notebook source
# MAGIC %run "/Repos/akashpandey15071996@outlook.com/Credit-Case-Study/notebook/application(df)_data_preparation"

# COMMAND ----------

df.write.mode("overwrite").csv("wasbs://creditcasestudydata@project99110.blob.core.windows.net/raw-datasets/cleaned-datasets/application-datasets/", header=True)

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://your-sql-server-name.database.windows.net:1433;databaseName=your-database-name;"
connection_properties = {
    "user" : "your-username",
    "password" : "your-password",
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

