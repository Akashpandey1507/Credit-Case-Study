# Databricks notebook source
#create the scope
secret_value = dbutils.secrets.get(scope="AkashScope", key="adlskey")
secret_storage_name = dbutils.secrets.get(scope="AkashScope", key="storagename")

# COMMAND ----------

# Getting all details from Azure
storage_account_name = secret_storage_name
storage_account_access_key = secret_value
blob_container = 'supply-chain-datasets'

# COMMAND ----------

spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

# COMMAND ----------

filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/rawdatasets/"

# COMMAND ----------

display(dbutils.fs.ls(filePath))
