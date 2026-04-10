# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

# COMMAND ----------

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col, lit,lower,upper,format_number,to_timestamp
from datetime import datetime
import pytz
import string

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

print("File Path : ",landing_path_url+filePath)

if (((processName.lower() == 'it') & (tableName.lower() == 'daily_exit_report'))):
    currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("skipRows", 1).option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)

elif (((processName.lower() == 'risk') & (tableName.lower() == 'applications'))):
    currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("skipRows", 2).option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)
    
elif (((processName.lower() == 'csr') & (tableName.lower() in ['krcpl_gl','kgspl_gl','indirect_exp_kgs','indirect_exp_krc','indirect_expense_kgdc','kgdcpl_gl']))):
    currentDf = spark.read.format("csv").option("inferschema","false").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)
  
else:
    currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)


# COMMAND ----------

# DBTITLE 1,Rename Repeating Column Name
if (((processName.lower() == 'impact') & (tableName.lower() == 'air_travel_data')) | ((processName.lower() == 'transport') & (tableName.lower() == 'roster_details'))):
    columnLen = len(currentDf.columns)
    print("no of columns:",columnLen)  
    currentDf=colNameRepeating(currentDf,columnLen)    

# COMMAND ----------

# DBTITLE 1,Trim and Convert Column name to Upper Case
wave1List = ['bgv','employee_engagement','global_mobility','headcount','headcount_monthly','jml','lnd','talent_acquisition']

if processName not in wave1List:
    for col in currentDf.columns:
        currentDf=currentDf.withColumnRenamed(col,toUpper((trimchar(col))))

# COMMAND ----------

# DBTITLE 1,Ignore ASCII in Column Name
wave1List = ['bgv','employee_engagement','global_mobility','headcount','headcount_monthly','jml','lnd','talent_acquisition']

if processName not in wave1List:
    for col in currentDf.columns:
        currentDf=currentDf.withColumnRenamed(col,ascii_ignore(col))


# COMMAND ----------

# DBTITLE 1,Format Headers Data
for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

# COMMAND ----------

# Double was getting converted with Scientific Notation while converting to String
typeCastCheckDf = spark.sql("select * from kgsonedatadb.config_data_type_cast")

# double Column List
doublecheckDf = typeCastCheckDf.select("Column_Name").where((lower(typeCastCheckDf.Process_Name) == processName.lower()) & (lower(typeCastCheckDf.Delta_Table_Name) == tableName.lower()) & (upper(typeCastCheckDf.Type_Cast_To) == ('DOUBLE')))

doubleList = doublecheckDf.rdd.flatMap(lambda x: x).collect()

print(doubleList)



# percent Column List
percentCheckDf = typeCastCheckDf.select("Column_Name").where((typeCastCheckDf.Process_Name == processName) & (typeCastCheckDf.Delta_Table_Name == tableName) & (upper(typeCastCheckDf.Type_Cast_To) == ('PERCENT')))

percentList = percentCheckDf.rdd.flatMap(lambda x: x).collect()


# percentList=["PERCENTAGE_OF_COMMUTING_BY_WALK"]
print(percentList)
for colName,data_type in currentDf.dtypes:
    if (colName in doubleList) & (data_type == 'double'):
        currentDf = currentDf.withColumn(colName,when(currentDf[colName].cast(DoubleType()).isNotNull(), format_number(currentDf[colName], 2)).otherwise(currentDf[colName]))
    if (colName in percentList):
        currentDf = currentDf.withColumn(colName,currentDf[colName]*100)


# COMMAND ----------

# Typecast every column to String
currentDf=colcaststring(currentDf,currentDf.columns)


# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

if (((processName.lower() == 'risk') & (tableName.lower() == 'applications'))):
    currentDf = currentDf.filter(col("ROW_LABELS")!="Grand Total")

# COMMAND ----------

# DBTITLE 1,Added for Legal
if (processName.lower() == 'legal'):
    currentDf=leadtrailremove(currentDf)

# COMMAND ----------

currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
currentDf=currentDf.withColumn("File_Date", lit(fileDate))

if (tableName == 'qualification_dump'):
    currentDf = currentDf.withColumnRenamed("EMPLOYEE_NUMBER","EMP_ID")
    currentDf = currentDf.withColumnRenamed("NAME","FULL_NAME")
    currentDf = currentDf.withColumnRenamed("GRADUATION","GRAD")
    currentDf = currentDf.withColumnRenamed("OTHER","OTHERS")
#     # currentDf=currentDf.withColumn("File_Date", currentDf[PS_FileDate])
#     currentDf=currentDf.withColumn("File_Date", col("PS_FileDate"))

# COMMAND ----------

junk_blank_columns = [column for column in currentDf.columns if column.lower().startswith("_c1") or column.lower().startswith("_c2") or column.lower().startswith("_c3") or column.lower().startswith("_c4") or column.lower().startswith("_c5") or column.lower().startswith("_c6") or column.lower().startswith("_c7") or column.lower().startswith("_c8") or column.lower().startswith("_c9") or column.lower().startswith("_c0")]

print(junk_blank_columns)

currentDf = currentDf.drop(*junk_blank_columns)

# COMMAND ----------

# DBTITLE 1,Added for Risk - IPF Table to fix Column Headers
#Using Colregex to get the IPF 2021 Assignment Date column name and making it one common format 'IPF_Assignment_Date'
if (((processName.lower() == 'risk') & (tableName.lower() == 'ipf'))):
    ipf_assignment=currentDf.select(currentDf.colRegex("`(IPF)+?.+(ASSIGNMENT)+?.+`")).columns
    ipf_completion=currentDf.select(currentDf.colRegex("`(IPF)+?.+(COMPLETION)+?.+`")).columns
    ipf_deadline=currentDf.select(currentDf.colRegex("`(DEADLINE_DATE)+?.+(ASSIGNMENT)+?.+`")).columns
    new_col1=ipf_assignment[0]
    new_col2=ipf_completion[0]
    new_col3=ipf_deadline[0]
    print(new_col1," ",new_col2," ",new_col3)
    currentDf=currentDf.withColumnRenamed(new_col1,'IPF_ASSIGNMENT_DATE')
    currentDf=currentDf.withColumnRenamed(new_col2,'IPF_COMPLETION_DATE')
    currentDf=currentDf.withColumnRenamed(new_col3,'DEADLINE_DATE')

# COMMAND ----------

databaseName = 'kgsonedatadb'
if spark._jsparkSession.catalog().databaseExists(databaseName):
    print("Database "+ databaseName +" exist")
else:
    spark.sql("create database "+ databaseName )
    print("Created the database "+ databaseName +" as it does not exist")

# COMMAND ----------

# DBTITLE 1,Load Raw Staging Data
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.raw_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load into Raw History Table
histTableName = "kgsonedatadb.raw_hist_"+ processName + "_" + tableName

if spark._jsparkSession.catalog().tableExists(histTableName):
    print("Table "+ histTableName +" exist")
    currentDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true")  \
    .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)


else:
    print("Table "+ histTableName +"  does not exist")
    currentDf.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema","true")  \
    .option("path",raw_hist_savepath_url+processName+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb.raw_hist_"+ processName + "_" + tableName)

# COMMAND ----------

# DBTITLE 1,Clean Up Script to remove duplicates, check for bad Record and Type Cast columns
layerName = 'raw'

dbutils.notebook.run("/kgsonedata/raw/Data_Cleanup",6000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})