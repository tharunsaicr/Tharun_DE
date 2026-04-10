# Databricks notebook source
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

# MAGIC %run
# MAGIC /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsfinance/common_utilities/common_components

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/Unpivot

# COMMAND ----------

print("File Path : ",finance_landing_path_url+filePath)

df = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

columnLen = len(df.columns)
print("no of columns:",columnLen)

display(df)
df.printSchema()

# COMMAND ----------

#replace special characters in column names with '_'
for col in df.columns:
    df=df.withColumnRenamed(col,replacechar(col))

#trim column names and remove leading and trailing underscores    
for i in range(columnLen):
    df=df.withColumnRenamed(df.columns[i], colNameTrim(df.columns[i],TrimValue="_")) 
    
    
display(df)

# COMMAND ----------

#convert all columns to string type 
from pyspark.sql.functions import trim, col
df = df.select([col(x).cast("String") for x in df.columns])
df.printSchema()

# COMMAND ----------

df = unpivotdf(df)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp

currentdatetime= datetime.now()
month_list=["OCT","NOV","DEC"]

#adding housekeeping fiels
df=df.withColumn("Dated_On", lit(currentdatetime))\
.withColumn("Period",upper(substring(df['Month'],0,3)))\
.withColumn("Financial_Year",when(col("Period").isin(month_list),concat(lit("20"),substring(df['Month'],5,2)).cast('int')+1).otherwise(concat(lit("20"),substring(df['Month'],5,2)).cast('int')))\
.withColumn('Month_Key',concat(lit("20"),substring(df['Month'],5,2),from_unixtime(unix_timestamp(substring(df['Month'],0,3),'MMM'),'MM')))

display(df)

# COMMAND ----------

#current table name in azure sql
saveTableName = "kgsfinancedb.raw_curr_"+ReportName+"_"+processName + "_"+ tableName
print(saveTableName)

# COMMAND ----------

df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_" + ReportName + "_" + processName + "_" +tableName)

# COMMAND ----------

print("Table Created : ", saveTableName)
print("path          :",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)