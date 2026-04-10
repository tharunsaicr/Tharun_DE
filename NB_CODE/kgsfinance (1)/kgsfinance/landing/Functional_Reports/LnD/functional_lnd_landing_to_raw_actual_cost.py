# Databricks notebook source
#Read files from landing and write into RAW curr(overwrite) & history(append) tables

# Have a SQL table or JSON config file to store details of which column to be handled or key value pairs to be used for dynamic processing
# Check no. of records in source vs delta tables

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit ,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
from pyspark.sql import functions as f
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

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

print(filePath)
print(tableName)
print(processName)
print(ReportName)

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

finance_landing_path_url+filePath

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",finance_landing_path_url+filePath)
currentDf =   spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes","true").load(finance_landing_path_url+filePath)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))


#triming all the values
currentDf=leadtrailremove(currentDf)
display(currentDf)
print(currentDf.count())

# COMMAND ----------

# Typecast every column to String  
currentDf=colcaststring(currentDf,currentDf.columns)
display(currentDf)

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")
print(currentDf.count())

# COMMAND ----------

#dropping unwanted columns like 1,2,3
collist=['1','2','3','f']
currentDf=currentDf.drop(*collist)

columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

# COMMAND ----------

# DBTITLE 1,Calling UDF to overcome repeated column from source  - Renaming the Repeating Columns
currentDf=colNameRepeating(currentDf,columnLen)
display(currentDf)

# COMMAND ----------

# DBTITLE 1,Dropping the unwanted columns if any
drop_col_df=currentDf.select(currentDf.colRegex("`_c_.+`"))
col_list=drop_col_df.columns

print(col_list)
currentDf=currentDf.drop(*col_list)
display(currentDf)


# COMMAND ----------

# DBTITLE 1,Removing the space or unwanted character at the beginning and the end the column names
columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

for i in range(columnLen):
    currentDf=currentDf.withColumnRenamed(currentDf.columns[i], colNameTrim(currentDf.columns[i],TrimValue="_"))   
    
display(currentDf)       

# COMMAND ----------

# DBTITLE 1,Deriving  month, Calendar_year, financial_year and CurrentTime 
month_list=["OCT","NOV","DEC"]


currentDf=currentDf.withColumnRenamed("000","amount_in_dollar_000")\
.withColumnRenamed("millions","amount_in_dollar_million")\
.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
.withColumn("Month",upper(substring(currentDf['period'],1,3)))\
.withColumn("Calendar_Year",substring(currentDf['period'],-4,4))\
.withColumn("Financial_Year",when(col('Month').isin(month_list),substring(currentDf['period'],5,8).cast('int')+1).otherwise(substring(currentDf['period'],5,8).cast('int')))


display(currentDf)


# COMMAND ----------

currentDf=currentDf.withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("Month"),'MMM'),'MM')))

# COMMAND ----------

#Final list of columns
final_col_list=currentDf.columns
print(final_col_list)

# COMMAND ----------

# Add Validation to count Number of records inserted into table matches with the original file

# COMMAND ----------

#compare the count from configuration table - process name, file name , header/column count

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName

# COMMAND ----------

"kgsfinancedb.raw_curr_"+ReportName+"_"+ processName + "_" +tableName

# COMMAND ----------

# DBTITLE 1,Load data to Raw Current
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

saveTableName = "kgsfinancedb.raw_curr_"+ReportName+"_"+ processName + "_" +tableName
print(saveTableName)

# COMMAND ----------

print("Table Created : ", saveTableName)
print("path",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

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
        fail
    
else:
    print("Table does not exists")
    fail

# COMMAND ----------

# DBTITLE 1,Load into Raw History Table
currentDf.write \
.mode("append") \
.format("delta") \
.option("mergeschema","true")\
.option("path",finance_raw_hist_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy")\
.saveAsTable("kgsfinancedb.raw_hist_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

#dbutils.notebook.run("/kgsfinance/raw/rawstg_to_rawcurr_clean_data",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

