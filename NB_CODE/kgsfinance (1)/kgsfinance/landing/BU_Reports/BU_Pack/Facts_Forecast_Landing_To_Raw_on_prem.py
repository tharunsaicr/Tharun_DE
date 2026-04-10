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

# DBTITLE 1,Read source data into dataframe
print("File Path : ",finance_landing_path_url+filePath)
df = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

columnLen = len(df.columns)
print("no of columns:",columnLen)

display(df)

# COMMAND ----------

# DBTITLE 1,Format headers data
#rename column special characters with '_'
for col in df.columns:
    df=df.withColumnRenamed(col,replacechar(col))
    
#trim column names
for i in range(columnLen):
    df=df.withColumnRenamed(df.columns[i], colNameTrim(df.columns[i],TrimValue="_")) 
    
#convert all columns to string type 
from pyspark.sql.functions import trim, col
df = df.select([col(x).cast("String") for x in df.columns])
display(df)

# COMMAND ----------

#change col type

df = df.withColumn("Updated_On", df.Updated_On.cast("date"))

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Deriving CurrentTime, Financial year, Calendar Year, Month
from pyspark.sql.functions import * 
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp

currentdatetime= datetime.now()
month_list=["OCT","NOV","DEC"]

df=df\
.withColumn("Dated_On", lit(currentdatetime))\
.withColumn("Month",upper(df['period']))\
.withColumn("Financial_Year",concat(lit("20"),substring(df['Years'],3,2)).cast('int'))\
.withColumn("Calendar_Year",when(col("Month").isin(month_list),concat(lit("20"),substring(df['Years'],3,2)).cast('int')-1).otherwise(concat(lit("20"),substring(df['Years'],3,2)).cast('int')))\
.withColumn('Month_Key',concat(col("Calendar_Year"),from_unixtime(unix_timestamp(col("Month"),'MMM'),'MM')))


display(df)


# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

df=df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])
df = df.dropna("all")

display(df)


# COMMAND ----------

# DBTITLE 1,Scenario field transformations
#when Scenario = 'Actual' then change to 'Scenario'
if (tableName in ['forecast_opex_data_extract', 'forecast_projects_data_extract', 'forecast_hcplan_data_extract']):
    df = df.withColumn("Scenario",
        when((col("Scenario")=="Actual"), "Outlook1_Actual")
        .when((col("Scenario")=="Actual_Entity_Adjustment"), "Outlook1_Actual")
        .when((col("Scenario")=="Actual_Bonus_Adjustment"), "Outlook1_Actual")
        .otherwise(col("Scenario")))
display(df)

# COMMAND ----------

#when Scenario = 'Actual' then change to 'Scenario'
if (tableName in ['outlook_opex_data_extract', 'outlook_projects_data_extract', 'outlook_hcplan_data_extract']):
    df = df.withColumn("Scenario",
        when((col("Scenario")=="Actual"), "RunRate_Actual")
        .when((col("Scenario")=="Actual_Entity_Adjustment"), "RunRate_Actual")
        .when((col("Scenario")=="Actual_Bonus_Adjustment"), "RunRate_Actual")
        .when((col("Scenario")=="Actual_BU_Adjustment"), "RunRate_Actual")
        .otherwise(col("Scenario")))
display(df)

# COMMAND ----------

# extracting number of rows from the Dataframe
row = df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(df.columns)
print("Column ",column)

# COMMAND ----------

#raw table name in azure sql db
saveTableName = "kgsfinancedb.raw_curr_"+ReportName+"_"+processName + "_"+ tableName
print(saveTableName)

# COMMAND ----------

# DBTITLE 1,Load Raw Current Table 
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

# COMMAND ----------

# DBTITLE 1,Check if row and column counts are matching with source
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

# DBTITLE 1,Load Raw History Table
currentDf = spark.sql("select * from kgsfinancedb.raw_curr_"+ReportName+ "_"+processName + "_"+ tableName)
display(currentDf)

currentDf.write \
 .mode("append") \
 .format("delta") \
 .option("mergeschema","true") \
 .option("path",finance_raw_hist_savepath_url+ReportName+"/"+processName+"/"+tableName) \
 .option("compression","snappy") \
 .saveAsTable("kgsfinancedb.raw_hist_" + ReportName+"_" + processName + "_" + tableName)