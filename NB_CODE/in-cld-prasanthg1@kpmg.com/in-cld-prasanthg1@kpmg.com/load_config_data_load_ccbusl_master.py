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

if ((tableName == 'dim_global_function') | (tableName == 'cc_bu_sl') | (tableName == 'cc_bu_sl_master') | (tableName == 'cost_center_business_unit')):
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
    
    currentDf = spark.sql('select distinct Cost_centre,Client_Geography,BU,Service_Line,Service_Network,File_Date,Dated_On,Month_Key,Final_BU from kgsonedatadb.config_cc_bu_sl_master')

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
if tableName == 'cc_bu_sl':
    dbutils.notebook.run("/kgsonedata/trusted/Delta_to_SQL_with_Select",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

# DBTITLE 1,Write to SQL
print(processName)
print(tableName)

if processName == 'config':
    config_table = processName+"_"+tableName
    config_hist_table = processName+"_hist_"+tableName
    print(config_table)
    print(config_hist_table)
else:
    table = "trusted_"+processName+"_"+tableName
    hist_table = "trusted_hist_"+processName+"_"+tableName
    print(table)
    print(hist_table)

jdbcHostname = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseHostName")
jdbcDatabase = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseName")
jdbcPort = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databasePort")

username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNameUserName")
password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNamePassword")

# Using service principal

# username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="applicationId")
# password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="appregkgs1datasecret")
  
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {  
  "user" : username,  
  "password" : password,  
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"  
}

driver = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect(
f'DRIVER={driver};SERVER={jdbcHostname};DATABASE={jdbcDatabase};UID={username};PWD={password}'
)
cursor = conn.cursor()

if processName == "config":

    if tableName == "cc_bu_sl_master":
        sampleDF=spark.sql('select `Function`,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,BU,Service_Line,Service_Network,File_Date,Dated_On, cast(Month_Key as string) from kgsonedatadb.'+config_table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(config_hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",config_hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ config_hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",config_hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ config_hist_table)
        
    else:
        sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+config_table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(config_hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",config_hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ config_hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",config_hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ config_hist_table)
        