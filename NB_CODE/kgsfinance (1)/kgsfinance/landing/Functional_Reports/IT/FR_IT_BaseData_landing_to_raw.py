# Databricks notebook source
# This NB handles Base data of IT Report
#Read files from landing and write into RAW curr(overwrite) & RAW history(append) tables
# Check no. of records in source vs delta tables

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print("filePath   :"+ filePath)
print("tableName  :"+tableName)
print("processName:"+ processName)
print("ReportName :"+ ReportName)


# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Import Statements
import string
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import * 
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",finance_landing_path_url+filePath)
currentDf = spark.read.format("csv").option("header","True").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
display(currentDf)   


# COMMAND ----------

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Dropping the unwanted columns
col_list=['1','2','3','Check','MI_Grouping_Check','Entity_Name_Check','Jounal_Name_Check','f','dd']
print(col_list)
currentDf=currentDf.drop(*col_list)
display(currentDf)


# COMMAND ----------

columnLen = len(currentDf.columns)
print("no of columns:",columnLen)
#Renaming the repeating columns
currentDf=colNameRepeating(currentDf,columnLen)
#triming all the values
currentDf=leadtrailremove(currentDf)
display(currentDf)


# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

print("Row ",currentDf.count())
currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")
print("Row ",currentDf.count())


# COMMAND ----------

# DBTITLE 1,Deriving  month, Calendar_year, financial_year and CurrentTime 

month_list=["OCT","NOV","DEC"]

currentDf=currentDf\
.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
.withColumn("Month",upper(substring(currentDf['period'],1,3)))\
.withColumn("Calendar_Year",substring(currentDf['period'],-4,4).cast('int'))\
.withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("Month"),'MMM'),'MM')))\
.withColumn("Financial_Year",when(col('Month').isin(month_list),substring(currentDf['period'],5,8).cast('int')+1).otherwise(substring(currentDf['period'],5,8).cast('int')))

display(currentDf)



# COMMAND ----------

# DBTITLE 1,Removing the space or unwanted character at the beginning and the end the column names

for i in range(columnLen):
    currentDf=currentDf.withColumnRenamed(currentDf.columns[i], colNameTrim(currentDf.columns[i],TrimValue="_"))   
    

# COMMAND ----------

#Final list of columns
final_col_list=currentDf.columns
print(final_col_list)

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Load Current Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_"+ReportName+"_"+processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Current table name & path 
saveTableName = "kgsfinancedb.raw_curr_"+ReportName+"_"+processName + "_"+ tableName
print("Table Created : ", saveTableName)
print("path          :",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Comparing the source count with current table count
if(spark._jsparkSession.catalog().tableExists(saveTableName)):
    tableDf = spark.sql("select * from "+saveTableName)

    tableDf_row = tableDf.count()
    print("Row ",tableDf_row)

    tableDf_col = len(tableDf.columns)
    print("Column ",tableDf_col)

    if((row == tableDf_row) & (column == tableDf_col)):
        print("Row and Column Count is Matching!!")
    else:
        print("Row Count is NOT Matching!!")
        
    
else:
    print("Table does not exists")

# COMMAND ----------

# DBTITLE 1,Loading Raw History Table
 currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",finance_raw_hist_savepath_url+ReportName+"/"+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsfinancedb.raw_hist_"+ReportName+"_"+ processName + "_" + tableName)