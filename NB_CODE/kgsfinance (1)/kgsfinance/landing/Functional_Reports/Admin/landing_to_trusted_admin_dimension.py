# Databricks notebook source
# Have a SQL table or JSON config file to store details of which column to be handled or key value pairs to be used for dynamic processing
# Check no. of records in source vs delta tables#Read files from landing and write into RAW curr(overwrite) & history(append) tables

# COMMAND ----------

# DBTITLE 1,Importing Required Functions
from datetime import datetime
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
from pyspark.sql import functions as F
import re,string
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

# DBTITLE 1,Format Headers Data
currentdatetime= datetime.now()
print("File Path : ",finance_landing_path_url+filePath)

#read the csv file
currentDf =spark.read.format("csv").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

#column length
columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

#replace special characters values with "_"
for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
    

 # Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

# DBTITLE 1,Dropping Empty Columns and Renaming repeated Columns
import string

#Renaming the repeating columns
rename_df=colNameRepeating(currentDf,columnLen)

#triming all the space values at start and of column values
trimDf=leadtrailremove(rename_df)

#Dropping empty columns
drop_col_df=trimDf.select(trimDf.colRegex("`_c_.+`"))
# display(drop_df)
col_list=drop_col_df.columns
# print(col_list)

final_df=trimDf.drop(*col_list,"_c")

#remove "_" at start and end of column namwe
for name in final_df.columns:
        final_df = final_df.withColumnRenamed(name,colNameTrim(name,"_"))
display(final_df)

# COMMAND ----------

if tableName=="dim_account_mapping":
    account_df=final_df.select("Account","Mapping")
    account_df1=account_df.dropna(how="any")
    display(account_df1)
else:
    account_df1=None


# COMMAND ----------

if tableName=="dim_geo_mapping":
    geo_Df=final_df.select("Geo","Geo_Mapping","Geo_Facility")
    geo_Df1=geo_Df.dropna(how="any")
    display(geo_Df1)
else:
    geo_Df1=None


# COMMAND ----------

if tableName=="dim_location":
    location_df=final_df.select("Operating_Unit","Location_2")
    location_df1=location_df.dropna(how="any")
    display(location_df1)
else:
    location_df1=None


# COMMAND ----------

if tableName=="dim_designation":
    designation_df=final_df.select("as_per_HR_report","Final_Report")
    designation_df1=designation_df.dropna(how="any")
    display(designation_df1)
else:
    designation_df1=None


# COMMAND ----------

list_1=[account_df1,geo_Df1,location_df1,designation_df1]
from pyspark.sql import DataFrame
for df in list_1:
        if df is not None and isinstance(df,DataFrame):
            
            df=df.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
                 .withColumn("File_Year", lit(File_Year).cast("int"))\
                 .withColumn("File_Month", lit(File_Month).cast("int"))

            display(df)

            df.write \
            .mode("overwrite") \
            .format("delta") \
            .option("overwriteSchema", "True") \
            .option("path",finance_trusted_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
            .option("compression","snappy") \
            .saveAsTable("kgsfinancedb.trusted_curr_"+ reportName + "_"+ processName + "_" +tableName) 
            #compare the count from configuration table - process name, file name , header/column count

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

if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':reportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})

# COMMAND ----------

# DBTITLE 1,Delta to SQL Load
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})