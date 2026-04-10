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

# DBTITLE 1,Call connection configuration module
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components NB
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Read data in df
print("File Path : ",finance_landing_path_url+filePath)
df = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(finance_landing_path_url+filePath)

columnLen = len(df.columns)
print("no of columns:",columnLen)

display(df)

# COMMAND ----------

# DBTITLE 1,Format headers data
#replace special char in col names with '_'
for col in df.columns:
    df=df.withColumnRenamed(col,replacechar(col))

#trim col names and remove leading and trailing underscores     
for i in range(columnLen):
    df=df.withColumnRenamed(df.columns[i], colNameTrim(df.columns[i],TrimValue="_")) 

display(df)

# COMMAND ----------

#trim text in column 'Account'
df = df.withColumn("Account", f.trim(f.col("Account")))
display(df)

# COMMAND ----------

#drop duplicates based on column 'Account'
df = df.dropDuplicates(['Account'])
display(df)

# COMMAND ----------

#convert all columns to string type 
from pyspark.sql.functions import trim, col
df = df.select([col(x).cast("String") for x in df.columns])
df.printSchema()

# COMMAND ----------

# extracting number of rows from the Dataframe
row = df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(df.columns)
print("Column ",column)

# COMMAND ----------

#current table name in azure sql db
saveTableName = "kgsfinancedb.raw_curr_"+ReportName+"_"+processName + "_"+ tableName
print(saveTableName)

# COMMAND ----------

# DBTITLE 1,Write data to raw layer
df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_"+ ReportName + "_" + processName + "_" +tableName)

# COMMAND ----------

print("Table Created : ", saveTableName)
print("path          :",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Validate row and column count
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