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

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)
display(currentDf)

# COMMAND ----------

if (processName.lower() == 'admin') & (tableName.lower() == 'qlikview_report'):

    #adding invoice date column to admin qlikview report table
    currentDf=currentDf.withColumn("INVOICE_DATE", lit(""))

    # Filter only KGS Entities
    Entity_List = ['KPMG Global Services Management Private Limited','KPMG Global Services Private Limited.','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited']

    Entity_List = [entity.lower() for entity in Entity_List]

    # Filter  required entities
    currentDf = currentDf.filter(lower(currentDf.ENTITY_NAME).isin(Entity_List))
    

# COMMAND ----------

from pyspark.sql.functions import substring, substring_index
from pyspark.sql.functions import trim
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

if (processName.lower() == 'admin') & (tableName.lower() == 'transaction_report'):

    #Drop duplicates - keep first in entry based on Emp name and payroll number
    window_spec = Window.partitionBy('NAME___REG_NUM', 'PAYROLL_NUM').orderBy(col("TRANSACTION_TIME").asc())
    currentDf = currentDf.withColumn("id", row_number().over(window_spec))
    currentDf = currentDf.filter(currentDf.id == 1)

    #trim location in trans report to remove letters before comma
    currentDf=currentDf.withColumn("Location_sub", trim(substring_index(col('Location'), ',', -1)))

    #load KGS location data from location mapping
    locationDf = spark.sql("select upper(Device_Location) as Device_Location, upper(Office_Location) as Office_Location, Location as final_location from kgsonedatadb.config_admin_location_mapping where Office_Location like 'KGS%' or Office_Location in ('KGS Bangalore One', 'KGS Bangalore GTP')")

    #trim location from trans report to 13 chars and drop duplicates
    locationDf=locationDf.withColumn("Device_Location", trim(substring(col('Device_Location'),1, 13)))
    locationDf = locationDf.dropDuplicates(['Device_Location', 'final_location'])

    currentDf = currentDf.join(locationDf, trim(substring(upper(currentDf.Location_sub),1,13)) == trim(substring(upper(locationDf.Device_Location),1,13)), "left")
    # currentDf = currentDf.withColumn("Location", currentDf.Office_Location)

    currentDf = currentDf.drop(*('Device_Location'))
    currentDf = currentDf.filter(currentDf.Office_Location.isNotNull())
display(currentDf)

# COMMAND ----------

display(currentDf)

# COMMAND ----------

if (processName.lower() == 'admin') & (tableName.lower() in ['transport_data_pune', 'transport_data_kolkata', 'transport_data_bangalore', 'transport_data_kochi', 'transport_data_gurgaon', 'transport_data_hyderabad','no_shows_pune', 'no_shows_kolkata', 'no_shows_,kochi', 'no_shows_bangalore', 'no_shows_gurgaon','no_shows_hyderabad']):
    currentDf = currentDf.withColumn("DATE", currentDf.DATE.cast("Date"))


# COMMAND ----------

# DBTITLE 1,Add BU2 to employee, cwk and loaned data
if (processName.lower() == 'admin') & (tableName.lower() in ['employee_data', 'cwk_data', 'loaned_data']): 
    buDf = spark.sql("select Cost_centre, BU2 from kgsonedatadb.config_admin_cc_bu_mapping")
    buDf = buDf.dropDuplicates()
    currentDf = currentDf.join(buDf, upper(currentDf.DEPARTMENT)== upper(buDf.Cost_centre), "left")
    currentDf = currentDf.drop('Cost_centre')


# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata'))
currentDf = currentDf.withColumn("Dated_On",lit(currentdatetime))



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