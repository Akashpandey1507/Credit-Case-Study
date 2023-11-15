# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the datasets

# COMMAND ----------

# Create the Screts Scope
secret_value = dbutils.secrets.get(scope="AzureScope", key="AccessKey")
secret_storage_name = dbutils.secrets.get(scope="AzureScope", key="storageAccountName")
secret_sql_user = dbutils.secrets.get(scope="AzureScope", key="SQLuser")
secret_sql_pass = dbutils.secrets.get(scope="AzureScope", key="SQLpass")
secret_sql_server = dbutils.secrets.get(scope="AzureScope", key="SQLserver")
secret_database = dbutils.secrets.get(scope="AzureScope", key="SQLdatabase")

# COMMAND ----------

# Getting all details from Azure
storage_account_name = secret_storage_name
storage_account_access_key = secret_value
blob_container = 'creditcasestudy'

# COMMAND ----------

# set the configure
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

# COMMAND ----------

# create the azure link
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/"
display(dbutils.fs.ls(filePath))

# COMMAND ----------

# get the link of application data
application_data_link = dbutils.fs.ls(filePath)[1][0]
previous_application_data_link = dbutils.fs.ls(filePath)[3][0]

# COMMAND ----------

application_data_link

# COMMAND ----------

# load the application data
application_data = spark.read.csv(application_data_link, header=True, inferSchema=True)

# COMMAND ----------

# craete the duplicate of application data
df = application_data.alias('copy')
display(df)

# COMMAND ----------

previous_application_data_link

# COMMAND ----------

# Load the previous_application_data
pre_application_data = spark.read.csv(previous_application_data_link, header=True, inferSchema=True)

# COMMAND ----------

#Create the duplicate of previous_application_data
df1 = pre_application_data.alias('copy')
display(df1)
