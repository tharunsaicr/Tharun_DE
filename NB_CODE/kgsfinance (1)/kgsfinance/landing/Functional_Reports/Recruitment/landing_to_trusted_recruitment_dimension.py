# Databricks notebook source
# Have a SQL table or JSON config file to store details of which column to be handled or key value pairs to be used for dynamic processing
# Check no. of records in source vs delta tables#Read files from landing and write into trusted CURR and history(append) tables



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

col_list=drop_col_df.columns


final_df=trimDf.drop(*col_list,"_c")

#remove "_" at start and end of column namwe
for name in final_df.columns:
        final_df = final_df.withColumnRenamed(name,colNameTrim(name,"_"))


# COMMAND ----------

# DBTITLE 1,Creating Geo Dataframe from main Dataframe
if tableName=="dim_geo_mapping":
    geo_df=final_df.select("Geo","Geo_Mapping")
    geo_df=geo_df.dropna(how="all")
    display(geo_df)
else:
    geo_df=None


# COMMAND ----------

# DBTITLE 1,Creating Account Dataframe from main Dataframe
if tableName=="dim_account_mapping":
    account_Df=final_df.select("Account_Code","Account_Name_1","Category")
    account_Df=account_Df.dropna(how="all")
    display(account_Df)
else:
    account_Df=None


# COMMAND ----------

# DBTITLE 1,Creating Source Dataframe from main Dataframe
if tableName=="dim_source_mapping":
    source_df=final_df.select("Source","Mapping")
    source_df=source_df.dropna(how="all")
    display(source_df)
else:
    source_df=None


# COMMAND ----------

# DBTITLE 1,Creating Category Dataframe from main Dataframe
if tableName=="dim_category_mapping":
    category_df=final_df.select("Category_2","Category_Mapping")
    category_df=category_df.dropna(how="all")
    display(category_df)
else:
    category_df=None


# COMMAND ----------

# DBTITLE 1,Creating Designation Dataframe from main Dataframe
if tableName=="dim_designation":
    designation_df=final_df.select("Designation","Designation_for_Cost_line")
    designation_df=designation_df.dropna(how="any")
    display(designation_df)
else:
    designation_df=None


# COMMAND ----------

# DBTITLE 1,Creating MI Mapping Dataframe from main Dataframe
if tableName=="dim_mi_mapping":
    mi_df=final_df.select("MI_Grouping","Trimmed","MI_Mapping")
    mi_df=mi_df.dropna(how="all")
    display(mi_df)
else:
    mi_df=None


# COMMAND ----------

# DBTITLE 1,Writing Dataframe to Trusted Current Layer

list_1=[geo_df,account_Df,source_df,category_df,designation_df,mi_df]
from pyspark.sql import DataFrame
for df in list_1:
        if df is not None and isinstance(df,DataFrame):
            display(df)
            df=df.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
                  .withColumn("File_Year", lit(File_Year).cast("int")) \
                 .withColumn("File_Month", lit(File_Month).cast("int")) 

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

if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':reportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})

# COMMAND ----------

# DBTITLE 1,Loading the trusted table to SQL Database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':reportName})