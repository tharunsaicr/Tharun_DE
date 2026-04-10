# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower,to_timestamp,max
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse
import string
from pyspark.sql.functions import *

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
currentDf = currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1, Trusted stg
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

# delta_table=spark.table("kgsonedatadb.trusted_gdc_absconding")

# COMMAND ----------

    if(spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_absconding")& spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_active_hc") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_attrition") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_attrition_pipeline") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_long_leave") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_fts") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_secondee") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_transfers") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_contractors") ):
        print("exists")
    else:
        print('not exists')

# COMMAND ----------

if tableName  in ["absconding","active_hc","attrition","attrition_pipeline","long_leave","fts","secondee","transfers","contractors"]:
    if(spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_absconding")& spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_active_hc") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_attrition") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_attrition_pipeline") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_long_leave") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_fts") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_secondee") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_transfers") & spark._jsparkSession.catalog().tableExists("kgsonedatadb.trusted_hist_gdc_contractors") ):
        #List of Delta table names including schema
        table_names = ["kgsonedatadb.trusted_hist_gdc_absconding", "kgsonedatadb.trusted_hist_gdc_active_hc", "kgsonedatadb.trusted_hist_gdc_attrition",
                "kgsonedatadb.trusted_hist_gdc_attrition_pipeline", "kgsonedatadb.trusted_hist_gdc_long_leave", "kgsonedatadb.trusted_hist_gdc_fts","kgsonedatadb.trusted_hist_gdc_secondee","kgsonedatadb.trusted_hist_gdc_transfers","kgsonedatadb.trusted_hist_gdc_contractors"]
    
        # List to store maximum dates from each table
        max_dates = []
        
        # Loop through each table to find the maximum date
        for table_name in table_names:
            delta_table = spark.table(table_name)
            max_date = delta_table.agg({"File_Date": "max"}).collect()[0][0]
            max_dates.append(max_date)
        print(max_dates,tableName)
        
        # # Check if all maximum dates match
        if all(date == max_dates[0] for date in max_dates[1:]):
            # If the dates match, call another notebook        
            dbutils.notebook.run("/kgsonedata/raw/GDC/Onboarding Master",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

if tableName not in ["absconding","active_hc","attrition","attrition_pipeline","long_leave","fts","secondee","transfers","vdi"]:
    dbutils.notebook.run("/kgsonedata/raw/GDC/Onboarding Master",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

