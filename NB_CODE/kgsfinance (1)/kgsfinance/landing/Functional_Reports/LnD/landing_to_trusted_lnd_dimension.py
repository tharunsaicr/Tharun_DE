# Databricks notebook source
# DBTITLE 1,Importing Required Functions
from pyspark.sql.functions import substring,lower,col, lit,when,concat,trim,to_timestamp
from datetime import datetime
import re
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "Year", defaultValue = "")
File_Year = dbutils.widgets.get("Year")

dbutils.widgets.text(name = "Month", defaultValue = "")
File_Month = dbutils.widgets.get("Month")

print(filePath)
print(tableName)
print(processName)
print(reportName)
print(File_Year)
print(File_Month)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

dbutils.fs.ls(finance_landing_path_url+filePath)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
currentdatetime= datetime.now()
print("File Path : ",finance_landing_path_url+filePath)

#read the csv file
currentDf =spark.read.format("csv").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)


#replace special characters values with "_"
for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
    

 # Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

#Dropping empty columns
drop_col_df=currentDf.select(currentDf.colRegex("`_c.+`"))

# display(drop_df)
col_list=drop_col_df.columns
print(col_list)

currentDf=currentDf.drop(*col_list,"_c")

# COMMAND ----------

for name in currentDf.columns:
    rename_df = currentDf.withColumnRenamed(name,colNameTrim(name,TrimValue=" "))

# COMMAND ----------

for name in rename_df.columns:
    rename_df = rename_df.withColumnRenamed(name,colNameTrim(name,TrimValue="_"))

# COMMAND ----------

# DBTITLE 1,Dropping Empty Columns and Renaming repeated Columns
import string

#Renaming the repeating columns
#column length
columnLen = len(currentDf.columns)
print("no of columns:",columnLen)
rename_df=colNameRepeating(currentDf,columnLen)

#triming all the space values at start and of column values
trimDf=leadtrailremove(rename_df)

# #Dropping empty columns
# drop_col_df=trimDf.select(trimDf.colRegex("`_c.+`"))

# # display(drop_df)
# col_list=drop_col_df.columns
# #print(col_list)

# final_df=trimDf.drop(*col_list,"_c")

#remove "_" at start and end of column namwe
for name in trimDf.columns:
        final_df = trimDf.withColumnRenamed(name,colNameTrim(name,"_"))
display(final_df)

# COMMAND ----------

if tableName=="dim_geo_mapping":
    geo_Df=final_df.select("Geo","Geo_mapping_1")
    geo_Df1=geo_Df.dropna(how="all")
    display(geo_Df1)
else:
    geo_Df1=None


# COMMAND ----------

if tableName=="dim_training_mapping":
    training_mapping_df=final_df.select("Training","Training_Mapping","Training_Mapping_1","New")
    training_mapping_df1=training_mapping_df.dropna(how="all")
    display(training_mapping_df1)
else:
    training_mapping_df1=None


# COMMAND ----------

if tableName=="dim_account_code_category":
    account_code_category_df=final_df.select("Account_Code_1","Account_name_as_per_GL_1","Category_2","Category_3")
    account_code_category_df1=account_code_category_df.dropna(how="all")
    display(account_code_category_df1)
else:
    account_code_category_df1=None


# COMMAND ----------

if tableName=="dim_client_geo_mapping":
    client_geo_df=final_df.select("Client_Geo","Geo_Mapping")
    client_geo_df1=client_geo_df.dropna(how="all")
    display(client_geo_df1)
else:
    client_geo_df1=None


# COMMAND ----------


if tableName=="dim_account_category":
    account_category_df=final_df.select('Account_name_as_per_GL',"Category")
    account_category_df1=account_category_df.dropna(how="all")
    display(account_category_df1)
else:
    account_category_df1=None


# COMMAND ----------


if tableName=="dim_account_name_category":
    account_name_category_df=final_df.select('Account_Code',"Account_Name",'Category_Mapping')
    account_name_category_df1=account_name_category_df.dropna(how="all")
    display(account_name_category_df1)
else:
    account_name_category_df1=None

# COMMAND ----------


if tableName=="dim_designation":
    designation_df=final_df.select('Job_Title',"Designation",'Designation_for_Reporting')
    designation_df1=designation_df.dropna(how="all")
    display(designation_df1)
else:
    designation_df1=None

# COMMAND ----------


# designation_df1 
# account_name_category_df1
# geo_Df1,
# training_mapping_df1,
# account_code_category_df1,
# client_geo_df1,
# account_category_df1,

df_list=[designation_df1,account_name_category_df1,geo_Df1,training_mapping_df1,account_code_category_df1,client_geo_df1,account_category_df1,]

for df in df_list:
        if df is not None and isinstance(df,DataFrame):

            
            df=df.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
                 .withColumn("File_Year", lit(File_Year))\
                 .withColumn("File_Month", lit(File_Month))

            display(df)

            df.write \
            .mode("overwrite") \
            .format("delta") \
            .option("overwriteSchema", "True") \
            .option("path",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
            .option("compression","snappy") \
            .saveAsTable("kgsfinancedb.trusted_curr_"+ reportName + "_"+ processName + "_" +tableName) 

            # extracting number of rows from the Dataframe
            row = df.count()
            print("Row ",row)

            # extracting number of columns from the Dataframe
            column = len(df.columns)
            print("Column ",column)

# COMMAND ----------

saveTableName = "kgsfinancedb.trusted_curr_"+ reportName + "_" +processName + "_"+ tableName
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName)

# COMMAND ----------

curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+reportName + "_" +processName + "_"+ tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load data to final trusted table
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':reportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})

# COMMAND ----------

# DBTITLE 1,Loading the trusted table to SQL Database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})

# COMMAND ----------

# Geo	Geo mapping    landinglayer/pending/testexcel_csv/Mapping.csv
# Training	Mapping	Mapping2	New 
# Account Code	Account name as per GL	Category	Category2
# df_list=['dim_geo_mapping','dim_training_mapping','dim_account_code_category','dim_client_geo_mapping','dim_account_category','dim_account_name_category','dim_designation']