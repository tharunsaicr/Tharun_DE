# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "FilePath", defaultValue = "")
filePath = dbutils.widgets.get("FilePath")

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

#Commented and hardcoded on 1/20/2023
# dbutils.widgets.text(name = "ProcessName", defaultValue = "")
# processName = dbutils.widgets.get("ProcessName")
processName = 'config'

print(filePath)
print(tableName)
print(processName)

# COMMAND ----------

fileDate = filePath.split('/')[-1].split('.')[0].split('_')[-1]
print(fileDate)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit,trim,lower,to_timestamp
import pyspark.sql.functions as f
import pytz
from pyspark.sql.functions import *
from pyspark.sql.window import Window

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

print("File Path : ",landing_path_url+filePath)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
print("File Path : ",landing_path_url+filePath)
currentDf = spark.read.format("csv").option("inferschema","true").option("header","true").option("delimiter",",").option("escape","\"").option("multiLine","true").option("escapeQuotes", "true").load(landing_path_url+filePath)


for col in currentDf.columns:
    currentDf=currentDf.withColumnRenamed(col,replacechar(col))

currentDf = currentDf.dropDuplicates()

# currentDf=currentDf.withColumn("File_Date", lit(fileDate))

# COMMAND ----------

if 'File_Date' in currentDf.columns:
    print("File_Date already exists")
    display(currentDf.select("File_Date").distinct())
else:
    print("Adding File_Date")
    currentDf=currentDf.withColumn("File_Date", lit(fileDate))
    display(currentDf)

# COMMAND ----------

if ((tableName == 'dim_global_function') | (tableName == 'cc_bu_sl') | (tableName == 'cost_center_business_unit')):
    currentDf=currentDf.withColumn("Final_BU", when((lower(trim(f.col("BU"))) == 'corporate functions') | (lower(trim(f.col("BU"))) == 'cf'),lit('CF'))\
        .when((lower(trim(f.col("BU"))) == 'kgs capability hubs') | (lower(trim(f.col("BU"))) == 'cap-hubs') | (lower(trim(f.col("BU"))) == 'ch') |(lower(trim(f.col("BU"))) == 'rak') ,lit('Cap-Hubs'))\
        .when((lower(trim(f.col("BU"))) == 'consulting') | (lower(trim(f.col("BU"))) == 'mc'),lit('Consulting'))\
        .when((lower(trim(f.col("BU"))) == 'da') | (lower(trim(f.col("BU"))) == 'das') | (lower(trim(f.col("BU"))) == 'da&s')  ,lit('DAS'))\
        .when(lower(trim(f.col("BU"))) == 'digital nexus',lit('Digital Nexus'))\
        .when(lower(trim(f.col("BU"))) == 'gdc',lit('GDC'))\
        .when(lower(trim(f.col("BU"))) == 'krc',lit('KRC'))\
        .when(lower(trim(f.col("BU"))) == 'ms',lit('MS'))\
        .when((lower(trim(f.col("BU"))) == 'risk services') | (lower(trim(f.col("BU"))) == 'rs') | (lower(trim(f.col("BU"))) == 'ras') |(lower(trim(f.col("BU"))) == 'rc'),lit('RS'))\
        .when((lower(trim(f.col("BU"))) == 'tax'),lit('Tax'))\
        .otherwise(f.col("BU")))


# display(currentDf["File_Date"].cast(StringType()))
currentDf = currentDf.withColumn("File_Date",currentDf["File_Date"].cast(StringType()))

# COMMAND ----------

if (tableName == 'dim_training_category'):
    currentDf=currentDf.withColumn('ItemTitle',unidecode_udf('ItemTitle'))
    currentDf=currentDf.withColumn('ItemTitle',regexp_replace('ItemTitle', '\n', ' '))      
    currentDf=currentDf.withColumn('row_num',row_number().over(Window.partitionBy('ItemTitle').orderBy('ItemTitle')))
    currentDf=currentDf.select('*').filter(currentDf.row_num ==1).drop('row_num')    

# COMMAND ----------

currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

# COMMAND ----------

# DBTITLE 1,Load Data to Current
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",config_path_url+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb."+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load data to history
histTableName = "kgsonedatadb."+ processName + "_hist_" + tableName

if spark._jsparkSession.catalog().tableExists(histTableName):
    print("Table "+ histTableName +" exist")

    currentDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema", "True") \
    .option("path",config_hist_path_url+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb."+ processName + "_hist_" +tableName)


else:
    print("Table "+ histTableName +"  does not exist")
    
    currentDf.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema","true")  \
    .option("path",config_hist_path_url+"/"+tableName) \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb."+ processName + "_hist_" +tableName)

# COMMAND ----------

if tableName == 'cc_bu_sl':

    currentDf = spark.sql("select distinct Cost_centre,Final_BU as BU,File_Date, Final_BU,Dated_On from kgsonedatadb.config_cc_bu_sl")

    currentDf.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "True") \
    .option("path",config_path_url+"/"+"cost_center_business_unit") \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb."+ processName + "_" +"cost_center_business_unit")

    currentDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema", "True") \
    .option("path",config_hist_path_url+"/"+"cost_center_business_unit") \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb."+ processName + "_hist_" +"cost_center_business_unit")

# COMMAND ----------

# DBTITLE 1,Delta to SQL Load
if (tableName == 'cc_bu_sl' or tableName =='admin_cc_bu_mapping' or tableName =='admin_location_mapping'):
    dbutils.notebook.run("/kgsonedata/trusted/Delta_to_SQL_with_Select",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

