# Databricks notebook source
# Dimensions used
# dim_EE_and_IandD_cost

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

print(filePath)
print(tableName)
print(processName)
print(reportName)

# COMMAND ----------

# DBTITLE 1,Call connection configuration module
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call Unpivot module
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/Unpivot_BU_pack

# COMMAND ----------

# DBTITLE 1,Import functions
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id,row_number
from pyspark.sql.functions import trim, col

# COMMAND ----------

# DBTITLE 1,read data in df
print("File Path : ",finance_landing_path_url+filePath)
df = spark.read.format("csv").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").option("InferSchema","true").load(finance_landing_path_url+filePath)

columnLen = len(df.columns)
print("no of columns:",columnLen)

display(df)

# COMMAND ----------

# DBTITLE 1,Renaming the column because it'll produce ambiguity
# df = df.withColumnRenamed("% Var. Vs FY23 Plan","Percent_Var_FY23_Plan")
import re
cols=[re.sub(r'(^_|_$)','',f.replace("%","percent")) for f in df.columns]
df = df.toDF(*cols)
df.display()

# COMMAND ----------

# DBTITLE 1,Replacing special chars in column names 
for col in df.columns:
    df=df.withColumnRenamed(col,replacechar(col))
    
for i in range(columnLen):
    df=df.withColumnRenamed(df.columns[i], colNameTrim(df.columns[i],TrimValue="_")) 

for i in range(columnLen):

    df=df.withColumnRenamed(df.columns[i], colNameTrim(df.columns[i],TrimValue="\r"))
    
    
display(df)

# COMMAND ----------

#finding percentage columns

percent = ["percent"]

percent_cols = [x for x in df.columns if any(x in y or y in x for y in percent)]

percent_cols

# COMMAND ----------

#multiply percent columns by 100 to receive correct percent values

from pyspark.sql.functions import trim, col

for col_name in df.columns:

    if col_name in percent_cols:

        df = df.withColumn(col_name,(col(col_name) * 100).cast("Integer"))

# COMMAND ----------

#convert all columns to string type 
from pyspark.sql.functions import trim, col
df = df.select([col(x).cast("String") for x in df.columns])
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Unpivot
#unpivot columns other than particulars
other_cols = ['Particulars']
df = unpivotdf_bu(df,other_cols)

display(df)

# COMMAND ----------

#raw table name in azure sql db
saveTableName = "kgsfinancedb.raw_curr_"+reportName+ "_"  +processName + "_"+ tableName
print(saveTableName)

# COMMAND ----------

# extracting number of rows from the Dataframe
row = df.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(df.columns)
print("Column ",column)

# COMMAND ----------

# DBTITLE 1,Write to raw layer
df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_raw_curr_savepath_url+reportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.raw_curr_"+ reportName + "_"+ processName + "_" +tableName)

# COMMAND ----------

print("Table Created : ", saveTableName)
print("path",finance_raw_curr_savepath_url)

# COMMAND ----------

# DBTITLE 1,Validate row and col count
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