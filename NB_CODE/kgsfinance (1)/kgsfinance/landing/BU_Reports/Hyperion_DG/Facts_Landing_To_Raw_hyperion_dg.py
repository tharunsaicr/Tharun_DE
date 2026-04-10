# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

print("filePath   :"+ filePath)
print("tableName  :"+tableName)
print("processName:"+ processName)
print("ReportName :"+ ReportName)

# COMMAND ----------

from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
from datetime import datetime
import re,string
import pytz

import pyspark.sql.functions as F
currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/common_components

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/Unpivot_hyperion_dg

# COMMAND ----------

# DBTITLE 1,Import Statements
from pyspark.sql.functions import trim, col
from pyspark.sql.functions import * 
from pyspark.sql.functions import col,when
from pyspark.sql.functions import regexp_replace

# COMMAND ----------

# DBTITLE 1,Read source data into dataframe

print("Current File Path : ",finance_landing_path_url+filePath)
df_curr = spark.read.format("csv").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

currColumnLen = len(df_curr.columns)
print("no of columns in curr file:",currColumnLen)

if "opex" in tableName:
    df_curr=df_curr.withColumn("Employee_Type",lit(None)) \
                   .withColumn("Role",lit(None))

if "projects" in tableName:
    df_curr=df_curr.withColumn("Employee_Type",lit(None))

df_curr=df_curr.withColumnRenamed("Operating Unit","Operating_Unit") \
               .withColumnRenamed("Employee Type","Employee_Type")
# df_curr=unpivotdf_bu(df_curr)
# print("new Row count ",df_curr.count())
# print("new Column count ",str(len(df_curr.columns)))


from pyspark.sql.functions import col, expr, explode, arrays_zip
 
# Define the columns that should not be unpivoted
columns_to_exclude = ["BusinessCategory", "Entity", "Employee_Type", "Role", "Operating_Unit", "Geo", "Location", "CostCenter", "Version", "Years", "Scenario", "Currency", "Period", "Dated_On", "Month", "Calendar_Year", "Financial_Year"]  # Add your columns here
 

display(df_curr)
 
# Unpivot the columns excluding the columns to be excluded
unpivoted_df = df_curr.selectExpr("BusinessCategory", "Entity", "Employee_Type", "Role", "Operating_Unit", "Geo", "Location", "CostCenter", "Version", "Years", "Scenario", "Currency", "Period"
,    
    "explode(map(" + ", ".join(["'{}', `{}`".format(col_name, col_name) for col_name in df_curr.columns if col_name not in columns_to_exclude]) + ")) as (ACCOUNT, DATA)"
)
 
# Show the resulting DataFrame

display(unpivoted_df)




# COMMAND ----------

# df_curr= df_curr.withColumn("ACCOUNT",
#                    when(col("ACCOUNT") == "Personnel Cost   Others Additional", "Personal Cost - Others Additional")
#                    .when(col("ACCOUNT") == "Personnel Cost   Others", "Personnel Cost - Others")
#                    .when(col("ACCOUNT") == "Professional Fees  Loaned Staff", "Professional Fees- Loaned Staffs")
#                    .when(col("ACCOUNT") == "Professional Fees  Loaned Staff Additional", "Professional Fees- Loaned Staff Additional")
#                    .when(col("ACCOUNT") == "Professional Fees   Secondee", "Professional Fees - Secondee")
#                    .when(col("ACCOUNT") == "Revenue Generating Utilization  ", "Revenue Generating Utilization %")
#                    .when(col("ACCOUNT") == "Staff Welfare   Offsite", "Staff Welfare - Offsite")
#                    .when(col("ACCOUNT") == "Personnel Cost   Others", "Staff_Welfare___Offsite_Additional")
#                    .when(col("ACCOUNT") == "Professional Fees  Loaned Staff", "Professional Fees- Loaned Staffs")
#                    .when(col("ACCOUNT") == "Professional Fees  Loaned Staff Additional", "Professional Fees- Loaned Staff Additional")
#                    .when(col("ACCOUNT") == "Professional Fees   Secondee", "Professional Fees - Secondee")
#                    .when(col("ACCOUNT") == "Revenue Generating Utilization  ", "Revenue Generating Utilization %")
#                    .otherwise(col("account")))

# COMMAND ----------

# DBTITLE 1,Format headers data
#rename column special characters with '_'
for col in unpivoted_df.columns:
    unpivoted_df=unpivoted_df.withColumnRenamed(col,replacechar(col))

currColumnLen = len(unpivoted_df.columns)
#trim column names 
for i in range(currColumnLen):
    unpivoted_df=unpivoted_df.withColumnRenamed(unpivoted_df.columns[i], colNameTrim(unpivoted_df.columns[i],TrimValue="_")) 
display(unpivoted_df)

#convert all columns to string type 
from pyspark.sql.functions import * 
df_curr = unpivoted_df.select([col(x).cast("String") for x in unpivoted_df.columns])

# COMMAND ----------

df_curr=df_curr.filter((~col("DATA").like("%#MISSING%")) & (~col("DATA").like("0")))
display(df_curr)

# COMMAND ----------

# DBTITLE 1,Deriving CurrentTime, Financial year, Calendar Year, Month
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp


month_list=["OCT","NOV","DEC"]

#adding housekeeping fiels
df_curr=df_curr\
.withColumn("Dated_On", lit(currentdatetime))\
.withColumn("Month",upper(df_curr['period']))\
.withColumn("Financial_Year",concat(lit("20"),substring(df_curr['Years'],3,2)).cast('int'))\
.withColumn("Calendar_Year",when(col("Month").isin(month_list),concat(lit("20"),substring(df_curr['Years'],3,2)).cast('int')-1).otherwise(concat(lit("20"),substring(df_curr['Years'],3,2)).cast('int')))\
.withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("Month"),'MMM'),'MM')))

display(df_curr)

# COMMAND ----------

# Replace empty value with None and drop null rows
df_curr=df_curr.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df_curr.columns])
df_curr = df_curr.dropna("all")

display(df_curr)


# COMMAND ----------

unchagned_columns=["Scenario","Dated_On","Month","Financial_Year","Calendar_Year","Month_Key"]
df_curr=df_curr.select(*[col(c).alias(c.upper()) if c not in unchagned_columns else col(c) for c in df_curr.columns])
display(df_curr)

# COMMAND ----------

# extracting number of rows from the curr Dataframe
row_curr = df_curr.count()
print("Row ",row_curr)

# extracting number of columns from the curr Dataframe
column_curr = len(df_curr.columns)
print("Column ",column_curr)

# COMMAND ----------

#raw table name in azure sql db
saveTableName = "kgsfinancedb.raw_curr_"+ReportName+"_"+processName + "_"+ tableName
print(saveTableName)

# COMMAND ----------

# DBTITLE 1,Load Raw Current table
#loading file in overwrite mode
df_curr.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_" + ReportName + "_" + processName + "_" + tableName)



# COMMAND ----------

print("Table Created : ", saveTableName)
print("path          :",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Check if row and column counts are matching with source
if(spark._jsparkSession.catalog().tableExists(saveTableName)):
    tableDf = spark.sql("select * from "+saveTableName)

    tableDf_row = tableDf.count()
    print("Row ",tableDf_row)

    tableDf_col = len(tableDf.columns)
    print("Column ",tableDf_col)

    if((row_curr == tableDf_row) & (column_curr == tableDf_col)):
        print("Row and Column Count is Matching!!")
    else:
        print("Row Count is NOT Matching!!")
        fail
    
else:
    print("Table does not exists")
    fail

# COMMAND ----------

# DBTITLE 1,Load Raw History table
currentDf = spark.sql("select * from kgsfinancedb.raw_curr_"+ReportName+ "_"+processName + "_"+ tableName)
display(currentDf)

currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",finance_raw_hist_savepath_url+ReportName+"/"+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsfinancedb.raw_hist_"+ReportName+"_"+ processName + "_" + tableName)

# COMMAND ----------

