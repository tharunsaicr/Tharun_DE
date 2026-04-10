# Databricks notebook source
# This NB handles Cost actuals
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
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Statements
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
from datetime import datetime
import re,string
import pytz

import pyspark.sql.functions as F
currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",finance_landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))
display(currentDf)   



# COMMAND ----------

#Renaming the repeating columns
currentDf=colNameRepeating(currentDf,columnLen)
#triming all the values
currentDf=leadtrailremove(currentDf)
#converting all columns to string datatype
currentDf=colcaststring(currentDf, currentDf.columns)


# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

print("Row ",currentDf.count())
currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")
print("Row ",currentDf.count())


# COMMAND ----------

# DBTITLE 1,Dropping the unwanted columns
col_list=['','1_Check___MI_Mapping','2_Opearting_Unit_check__per_MI_Mapping','_1','_2','Bonus___1','Markup__','Bonus','Gratuity___LE','Total_Incl_Bonus___Gratuity','Total_Incl_Markup','Remarks_1','Revenue','GEO_Final','BU_Wise','MI_Mapping_1','Check','MI_Gourping_Basis_Account_Code','MI_Gourping_Basis_Account_Code','MI_Grouping_Check','Entity_Name_as_per_OU','Entity_Name_Check','Description_Length','Jounal_Name_Check','Jounal_Description_Check','aa','aaa','All_Vendors','Core_Non_Core'] 
print(col_list)
currentDf=currentDf.drop(*col_list)
#display(currentDf)


# COMMAND ----------

# DBTITLE 1,Removing the space or unwanted character at the beginning and the end the column names
#pass the unwanted character in the TrimValue
columnLen = len(currentDf.columns)
print("no of columns:",columnLen)

for i in range(columnLen):
    currentDf=currentDf.withColumnRenamed(currentDf.columns[i], colNameTrim(currentDf.columns[i],TrimValue="_"))   
    
#display(currentDf)        

# COMMAND ----------

# DBTITLE 1,Deriving  month, Calendar_year, financial_year & CurrentTime and renaming the column
currentdatetime= datetime.now()
month_list=["oct","nov","dec"]

currentDf=currentDf\
.withColumnRenamed("000","amount_in_dollar_000")\
.withColumnRenamed("millions","amount_in_dollar_million")\
.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
.withColumn("Month",upper(substring(currentDf['period'],1,3)))\
.withColumn("Calendar_Year",substring(currentDf['period'],-4,4).cast('int'))\
.withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("Month"),'MMM'),'MM')))\
.withColumn("Financial_Year",when(lower(substring(currentDf['period'],1,3)).isin(month_list),substring(currentDf['period'],5,8).cast('int')+1).otherwise(substring(currentDf['period'],5,8).cast('int')))

display(currentDf)


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

# DBTITLE 1,Current table name & path created
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
        fail
    
else:
    print("Table does not exists")
    fail

# COMMAND ----------

# DBTITLE 1,Loading Raw History Table
 currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",finance_raw_hist_savepath_url+ReportName+"/"+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsfinancedb.raw_hist_"+ReportName+"_"+ processName + "_" + tableName)