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

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Import Statements
from pyspark.sql.functions import trim, col
from pyspark.sql.functions import * 
from pyspark.sql.functions import col,when

# COMMAND ----------

# DBTITLE 1,Read source data into dataframe

print("Current File Path : ",finance_landing_path_url+filePath)
df_curr = spark.read.format("csv").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

currColumnLen = len(df_curr.columns)
print("no of columns in curr file:",currColumnLen)
display(df_curr)

# COMMAND ----------

# DBTITLE 1,Format headers data
#rename column special characters with '_'
for col in df_curr.columns:
    df_curr=df_curr.withColumnRenamed(col,replacechar(col))
    
#trim column names 
for i in range(currColumnLen):
    df_curr=df_curr.withColumnRenamed(df_curr.columns[i], colNameTrim(df_curr.columns[i],TrimValue="_")) 
display(df_curr)

#convert all columns to string type 
from pyspark.sql.functions import * 
df_curr = df_curr.select([col(x).cast("String") for x in df_curr.columns])

# COMMAND ----------

#change col type

df_curr = df_curr.withColumn("Updated_On", df_curr.Updated_On.cast("date"))

df_curr.printSchema()

# COMMAND ----------

# DBTITLE 1,Deriving CurrentTime, Financial year, Calendar Year, Month
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp

currentdatetime= datetime.now()
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

# DBTITLE 1,'Scenario' field transformation
#change Scenario field value to 'Actual' for all rows
df_curr = df_curr.withColumn("Scenario",lit("Actual"))
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