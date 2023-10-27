# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Credit loan Case Study - Data Cleaning with Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Download the datasets: https://www.kaggle.com/datasets/venkatasubramanian/credit-eda-case-study

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Introduction
# MAGIC
# MAGIC ###### This assignment aims to give you an idea of applying EDA in a real business scenario. In this assignment, apart from applying the techniques that you have learnt in the EDA module, you will also develop a basic understanding of risk analytics in banking and financial services and understand how data is used to minimise the risk of losing money while lending to customers.
# MAGIC
# MAGIC # Business Understanding
# MAGIC ###### The loan providing companies find it hard to give loans to the people due to their insufficient or non-existent credit history. Because of that, some consumers use it to their advantage by becoming a defaulter. Suppose you work for a consumer finance company which specialises in lending various types of loans to urban customers. You have to use EDA to analyse the patterns present in the data. This will ensure that the applicants capable of repaying the loan are not rejected.
# MAGIC
# MAGIC ###### When the company receives a loan application, the company has to decide for loan approval based on the applicant’s profile. Two types of risks are associated with the bank’s decision
# MAGIC
# MAGIC # Business Objectives
# MAGIC ###### This case study aims to identify patterns which indicate if a client has difficulty paying their instalments which may be used for taking actions such as denying the loan, reducing the amount of loan, lending (to risky applicants) at a higher interest rate, etc. This will ensure that the consumers capable of repaying the loan are not rejected. Identification of such applicants using EDA is the aim of this case study. In other words, the company wants to understand the driving factors (or driver variables) behind loan default, i.e. the variables which are strong indicators of default. The company can utilise this knowledge for its portfolio and risk assessment.
# MAGIC
# MAGIC ###### To develop your understanding of the domain, you are advised to independently research a little about risk analytics - understanding the types of variables and their significance should be enough.

# COMMAND ----------

# MAGIC %fs  ls 

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/

# COMMAND ----------

# File path
file_path = "dbfs:/FileStore/tables/application_data.csv"

# COMMAND ----------

# Loading the original datasets
credit_load_application_datasets = spark.read.csv(
                                                        "dbfs:/FileStore/tables/application_data.csv", 
                                                        header=True,
                                                        inferSchema=True
)

# COMMAND ----------

# Create the duplicate copy of original datasets in df
df = credit_load_application_datasets.alias('copy')

# COMMAND ----------

# Reading the datasets
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Importing the important libraries for Analysis or data cleaning

# COMMAND ----------

# Importing the libraries
from pyspark.sql import functions as f
from pyspark.sql import types as t
import matplotlib.pyplot as plt 
import seaborn as sns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Processing or Data Cleaning

# COMMAND ----------

# Checking the size of the datasets
df.count()

# COMMAND ----------

# Checking the len of columns of the datasets
len(df.columns)

# COMMAND ----------

# checking the dataypes of each columns of the datasets

df.printSchema()

# COMMAND ----------

# checking the null values
df.select([f.sum(f.col(column).isNull().cast('int')).alias(column) for column in df.columns]).display()

# COMMAND ----------

# Get access of all string columns of the datasets on uncleaned datasets
[column for column, datatype in df.dtypes if datatype in ["string"]]

# COMMAND ----------

# get show of all string columns
string_df = df.select([column for column, datatype in df.dtypes if datatype in ["string"]])
string_df.display()

# COMMAND ----------

# checking the null values of only string columns of datasets
string_df.select([f.sum(f.col(column).isNull().cast('int')).alias(column) for column in string_df.columns]).display()

# COMMAND ----------

# pick the unwanted columns form string dataype columns
unwanted_column =  'FONDKAPREMONT_MODE', "HOUSETYPE_MODE", "WALLSMATERIAL_MODE", "EMERGENCYSTATE_MODE"
unwanted_column

# COMMAND ----------

# drop from the original datasets of unwanted columns
df = df.drop('FONDKAPREMONT_MODE', "HOUSETYPE_MODE", "WALLSMATERIAL_MODE", "EMERGENCYSTATE_MODE")

# COMMAND ----------

# again checking the null values of only string columns of datasets
string_df = df.select([column for column, datatype in df.dtypes if datatype in ["string"]])
string_df.select([f.sum(f.col(column).isNull().cast('int')).alias(column) for column in string_df.columns]).display()

# COMMAND ----------

# replace null values to "Unknown" in "NAME_TYPE_SUITE" columns
df = df.withColumn(
    "NAME_TYPE_SUITE",
    f.when(df["NAME_TYPE_SUITE"].isNull(), "Unknown").otherwise(df["NAME_TYPE_SUITE"])
)

# COMMAND ----------

# remove the null value from "OCCUPATION_TYPE" column of the datasets
df = df.filter(df["OCCUPATION_TYPE"] != 'null')

# COMMAND ----------

# again checking the null values of only string columns of datasets
string_df = df.select([column for column, datatype in df.dtypes if datatype in ["string"]])
string_df.select([f.sum(f.col(column).isNull().cast('int')).alias(column) for column in string_df.columns]).display()

# COMMAND ----------

# check the "XNA" in "ORGANIZATION_TYPE"
df.filter(df["ORGANIZATION_TYPE"] == 'XNA').display()

# COMMAND ----------

# Handling "ORGANIZATION_TYPE" of "XNA values in the datasets"

df1 = df.groupBy(f.col("ORGANIZATION_TYPE")).count().show()

# COMMAND ----------

# replace "XNA" with "Un-Identified" in "ORGANIZATION_TYPE" columns
df = df.withColumn(
                            "ORGANIZATION_TYPE",
                            f.when(df["ORGANIZATION_TYPE"] == "XNA" , 'Un-Identified')
                            .otherwise(df["ORGANIZATION_TYPE"])
                   )

# COMMAND ----------

# again check the "XNA" in "ORGANIZATION_TYPE"
df.filter(df["ORGANIZATION_TYPE"] == 'XNA').display()

# COMMAND ----------

# again get show of all numeric columns
numeric_df = df.select([column for column, datatype in df.dtypes if datatype in ["int","double"]])
numeric_df.display()

# COMMAND ----------

column = numeric_df.columns
column

# COMMAND ----------

# Get the all columns where the columns name is starting with "FLAG"
[column_name for column_name in column if column_name.startswith('FLAG')]

# COMMAND ----------

# Drop the all those columns where the columns are starting with "FLAG"
df = df.drop('FLAG_MOBIL',
                                                'FLAG_EMP_PHONE',
                                                'FLAG_WORK_PHONE',
                                                'FLAG_CONT_MOBILE',
                                                'FLAG_PHONE',
                                                'FLAG_EMAIL',
                                                'FLAG_DOCUMENT_2',
                                                'FLAG_DOCUMENT_3',
                                                'FLAG_DOCUMENT_4',
                                                'FLAG_DOCUMENT_5',
                                                'FLAG_DOCUMENT_6',
                                                'FLAG_DOCUMENT_7',
                                                'FLAG_DOCUMENT_8',
                                                'FLAG_DOCUMENT_9',
                                                'FLAG_DOCUMENT_10',
                                                'FLAG_DOCUMENT_11',
                                                'FLAG_DOCUMENT_12',
                                                'FLAG_DOCUMENT_13',
                                                'FLAG_DOCUMENT_14',
                                                'FLAG_DOCUMENT_15',
                                                'FLAG_DOCUMENT_16',
                                                'FLAG_DOCUMENT_17',
                                                'FLAG_DOCUMENT_18',
                                                'FLAG_DOCUMENT_19',
                                                'FLAG_DOCUMENT_20',
                                                'FLAG_DOCUMENT_21'
                                                )

# COMMAND ----------

# again get show of all numeric columns
numeric_df = df.select([column for column, datatype in df.dtypes if datatype in ["int","double"]])
numeric_df.display()

# COMMAND ----------

# check the null values in only numeric data type columns 
numeric_df.select([f.sum(f.col(column).isNull().cast('int')).alias(column) for column in numeric_df.columns]).display()

# COMMAND ----------

# unwanted columns for delete
unwanted_col = ["OWN_CAR_AGE", "EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "APARTMENTS_AVG", "BASEMENTAREA_AVG", "YEARS_BEGINEXPLUATATION_AVG",
                "YEARS_BUILD_AVG", "COMMONAREA_AVG", "ELEVATORS_AVG", "ENTRANCES_AVG", "FLOORSMAX_AVG", "FLOORSMIN_AVG", "FLOORSMIN_AVG", "LIVINGAPARTMENTS_AVG",
                "LIVINGAREA_AVG", "NONLIVINGAPARTMENTS_AVG","NONLIVINGAREA_AVG", "APARTMENTS_MODE", "BASEMENTAREA_MODE", "YEARS_BEGINEXPLUATATION_MODE",
                "YEARS_BUILD_MODE", "COMMONAREA_MODE", "ELEVATORS_MODE", "ENTRANCES_MODE", "FLOORSMAX_MODE", "FLOORSMIN_MODE", "LANDAREA_MODE", "LIVINGAPARTMENTS_MODE"
                ,"LIVINGAREA_MODE", "NONLIVINGAPARTMENTS_MODE", "NONLIVINGAREA_MODE", "APARTMENTS_MEDI", "BASEMENTAREA_MEDI", "YEARS_BEGINEXPLUATATION_MEDI", "YEARS_BEGINEXPLUATATION_MEDI", "YEARS_BUILD_MEDI", "COMMONAREA_MEDI", "ELEVATORS_MEDI", "ENTRANCES_MEDI", "FLOORSMAX_MEDI",'FLOORSMIN_MEDI', "FLOORSMIN_MEDI", "LIVINGAPARTMENTS_MEDI", "LIVINGAREA_MEDI", "NONLIVINGAPARTMENTS_MEDI", "NONLIVINGAREA_MEDI", "TOTALAREA_MODE", "OBS_30_CNT_SOCIAL_CIRCLE", "DEF_30_CNT_SOCIAL_CIRCLE", "OBS_60_CNT_SOCIAL_CIRCLE", "DEF_60_CNT_SOCIAL_CIRCLE" ,"DAYS_LAST_PHONE_CHANGE", "AMT_REQ_CREDIT_BUREAU_HOUR", "AMT_REQ_CREDIT_BUREAU_DAY", "AMT_REQ_CREDIT_BUREAU_WEEK", "AMT_REQ_CREDIT_BUREAU_WEEK", "AMT_REQ_CREDIT_BUREAU_QRT", "AMT_REQ_CREDIT_BUREAU_YEAR"
                ]
unwanted_col

# COMMAND ----------

# delete the all unwanted columns from the datasets
df = df.drop("OWN_CAR_AGE", "EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3", "APARTMENTS_AVG", "BASEMENTAREA_AVG", "YEARS_BEGINEXPLUATATION_AVG",
                "YEARS_BUILD_AVG", "COMMONAREA_AVG", "ELEVATORS_AVG", "ENTRANCES_AVG", "FLOORSMAX_AVG", "FLOORSMIN_AVG", "FLOORSMIN_AVG", "LIVINGAPARTMENTS_AVG",
                "LIVINGAREA_AVG", "NONLIVINGAPARTMENTS_AVG","NONLIVINGAREA_AVG", "APARTMENTS_MODE", "BASEMENTAREA_MODE", "YEARS_BEGINEXPLUATATION_MODE",
                "YEARS_BUILD_MODE", "COMMONAREA_MODE", "ELEVATORS_MODE", "ENTRANCES_MODE", "FLOORSMAX_MODE", "FLOORSMIN_MODE", "LANDAREA_MODE", "LIVINGAPARTMENTS_MODE"
                ,"LIVINGAREA_MODE", "NONLIVINGAPARTMENTS_MODE", "NONLIVINGAREA_MODE", "APARTMENTS_MEDI", "BASEMENTAREA_MEDI", "YEARS_BEGINEXPLUATATION_MEDI", "YEARS_BEGINEXPLUATATION_MEDI", "YEARS_BUILD_MEDI", "COMMONAREA_MEDI", "ELEVATORS_MEDI", "ENTRANCES_MEDI", "FLOORSMAX_MEDI",'FLOORSMIN_MEDI', "FLOORSMIN_MEDI", "LIVINGAPARTMENTS_MEDI", "LIVINGAREA_MEDI", "NONLIVINGAPARTMENTS_MEDI", "NONLIVINGAREA_MEDI", "TOTALAREA_MODE", "OBS_30_CNT_SOCIAL_CIRCLE", "DEF_30_CNT_SOCIAL_CIRCLE", "OBS_60_CNT_SOCIAL_CIRCLE", "DEF_60_CNT_SOCIAL_CIRCLE" ,"DAYS_LAST_PHONE_CHANGE", "AMT_REQ_CREDIT_BUREAU_HOUR", "AMT_REQ_CREDIT_BUREAU_DAY", "AMT_REQ_CREDIT_BUREAU_WEEK", "AMT_REQ_CREDIT_BUREAU_WEEK", "AMT_REQ_CREDIT_BUREAU_QRT", "AMT_REQ_CREDIT_BUREAU_YEAR", "LANDAREA_AVG", "LANDAREA_MEDI", "AMT_REQ_CREDIT_BUREAU_MON")

# COMMAND ----------

# rest of null values replace with 0
df = df.fillna(0)

# COMMAND ----------

# Againt checking the null values in numeric data type from the datasets
numeric_df = df.select([column for column, datatype in df.dtypes if datatype in ["int","double"]])
numeric_df.select([f.sum(f.col(column).isNull().cast('int')).alias(column) for column in numeric_df.columns]).display()

# COMMAND ----------

# Again we are checking teh null values from the main datasets
df.select([f.sum(f.col(column).isNull().cast('int')).alias(column) for column in df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Now, There are zero null values in the original datasets

# COMMAND ----------

# values count of the "TARGET" columns
df.groupBy(f.col("TARGET")).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Dealing teh XNA Values in "CODE_GENDER" Columns of the datasets

# COMMAND ----------

# value scount of "CODE_GENDER" of the datasets
df.groupBy(f.col("CODE_GENDER")).count().show()

# COMMAND ----------

# Update the "CODE_GENDER" and replace the value XNA with "F" because female number is larger than Male in the datasets
df = df.withColumn("CODE_GENDER", f.when(df["CODE_GENDER"] == "XNA", "F").
                   otherwise(df["CODE_GENDER"]))

# COMMAND ----------

# Again value scount of "CODE_GENDER" of the datasets
df.groupBy(f.col("CODE_GENDER")).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### The datasets have cleaned.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Now, Its time to work on finding the outlier and dealing with them on "AMT_INCOME_TOTAL" and "AMT_CREDIT" columns of the datasets.

# COMMAND ----------

# Display the all cleaned datasets
df.display()

# COMMAND ----------

# Seperated the columns for visulisation
income_col = df.select("AMT_INCOME_TOTAL")
credit_col = df.select("AMT_CREDIT")

# COMMAND ----------

# Changing teh pyspark dataframe to pandas dataframe for create the box plot of both to check the outlier is having or not
income_col_pd = income_col.toPandas()
credit_col_pd = credit_col.toPandas()

# COMMAND ----------

# Visualisation of box plot on AMT_INCOME_TOTAL

plt.figure(figsize=(25,4))
plt.title(" Checking the Outlier in AMT_INCOME_TOTAL columns from the datasets ")


sns.boxplot(data=income_col_pd, x= "AMT_INCOME_TOTAL")

# COMMAND ----------

# Finding the max and min number of "AMT_INCOME_TOTAL" column
max_of_income = df.select(f.max(df['AMT_INCOME_TOTAL']))
min_of_income = df.select(f.min(df['AMT_INCOME_TOTAL']))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #--------------------------------- Outlier for Income Columns ---------------------------

# COMMAND ----------

max_of_income.show(truncate=False)

# COMMAND ----------

min_of_income.show()

# COMMAND ----------

# finding the percentile of AMT_INCOME_TOTAL
percentiles = [0.25, 0.5, 0.75]
quantiles = df.approxQuantile("AMT_INCOME_TOTAL", percentiles, 0.01)

q1, q2, q3 = quantiles


# COMMAND ----------

q1

# COMMAND ----------

q2

# COMMAND ----------

q3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### The Interquartile Range (IQR) is a measure of statistical dispersion that is calculated as the difference between the third quartile (Q3) and the first quartile (Q1) of a dataset. In mathematical notation, it can be expressed as:
# MAGIC
# MAGIC # IQR = Q3 - Q1
# MAGIC
# MAGIC ### Where:
# MAGIC
# MAGIC * Q3 is the third quartile (75th percentile), representing the value below which 75% of the data falls.
# MAGIC * Q1 is the first quartile (25th percentile), representing the value below which 25% of the data falls.
# MAGIC ### The IQR is often used to identify the spread or variability of data in the middle 50% of a dataset. It is less sensitive to outliers than the range and is a useful measure in statistics and data analysis.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Finding the The Interquartile Range (IQR)
iqr = q3 - q1 
iqr

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Lower Fence: Lower bound for potential outliers.
# MAGIC
# MAGIC * Lower Fence = Q1 - 1.5 * IQR
# MAGIC ### Upper Fence: Upper bound for potential outliers.
# MAGIC
# MAGIC * Upper Fence = Q3 + 1.5 * IQR
# MAGIC ### Where:
# MAGIC
# MAGIC * Q1 is the first quartile (25th percentile).
# MAGIC * Q3 is the third quartile (75th percentile).
# MAGIC * IQR is the Interquartile Range (Q3 - Q1).

# COMMAND ----------

lower_fence = q1 - 1.5 * iqr
upper_fence = q3 + 1.5 * iqr

# COMMAND ----------

lower_fence

# COMMAND ----------

upper_fence

# COMMAND ----------

# removing the outlier from the original datasets from AMT_INCOME_TOTAL column
df = df.filter(df["AMT_INCOME_TOTAL"] < upper_fence)

# COMMAND ----------

# Again finding the max values of AMT_INCOME_TOTAL columns after removing outlier
max_of_income = df.select(f.max(df['AMT_INCOME_TOTAL']))
max_of_income.show()

# COMMAND ----------

# change into pandas data frma for visualisation
income_col_pd = df.select("AMT_INCOME_TOTAL").toPandas()

# Again Visualisation of box plot on AMT_INCOME_TOTAL after removing the outlier
plt.figure(figsize=(25,4))
plt.title(" Checking the Outlier in AMT_INCOME_TOTAL columns from the datasets ")


sns.boxplot(data=income_col_pd, x= "AMT_INCOME_TOTAL")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #--------------------------------- Outlier for Income Columns ---------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #--------------------------------- Outlier for Credit Columns ---------------------------

# COMMAND ----------

df.display()

# COMMAND ----------

# Visualisation of box plot on AMT_CREDIT

plt.figure(figsize=(25,4))
plt.title(" Checking the Outlier in AMT_CREDIT columns from the datasets ")


sns.boxplot(data=credit_col_pd, x= "AMT_CREDIT")

# COMMAND ----------

# finding the max and min of credit amount
max_of_credit_amt = df.select(f.max("AMT_CREDIT"))
min_of_credit_amt = df.select(f.min("AMT_CREDIT"))

# COMMAND ----------

max_of_credit_amt.show()

# COMMAND ----------

min_of_credit_amt.show()

# COMMAND ----------

# finding the percentile of AMT_INCOME_TOTAL
percentiles = [0.25, 0.5, 0.75]
quantile = df.approxQuantile("AMT_CREDIT", percentiles, 0.01)

q1C, q2C, q3C = quantile

# COMMAND ----------

q1C

# COMMAND ----------

q2C

# COMMAND ----------

q3C

# COMMAND ----------

# finding the IQR
iqrC = q3C -q1C
iqrC

# COMMAND ----------

lower_fence_credit = q1C - 1.5 * iqrC
upper_fence_credit = q3C + 1.5 * iqrC

# COMMAND ----------

lower_fence_credit

# COMMAND ----------

upper_fence_credit

# COMMAND ----------

# removing the outlier from AMT_CREDIT columns of the datasets
df = df.filter(df["AMT_CREDIT"] < upper_fence_credit)

# COMMAND ----------

credit_col_pd = df.select("AMT_CREDIT").toPandas() # chenge to pandas df

# COMMAND ----------

# Visualisation of box plot on AMT_CREDIT

plt.figure(figsize=(25,4))
plt.title(" Checking the Outlier in AMT_CREDIT columns from the datasets ")


sns.boxplot(data=credit_col_pd, x= "AMT_CREDIT")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #--------------------------------- Outlier for Credit Columns ---------------------------

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # The Datasets has been cleaned and ready for EDA

# COMMAND ----------

# exporting the cleaned datasets

df.write.parquet("Credit_case_cleaned_data")

# COMMAND ----------

df.count()

# COMMAND ----------

len(df.columns)
