# Databricks notebook source
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

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower,to_timestamp,concat,regexp_extract,substring
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
currentDf = currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

# COMMAND ----------

currentDf=currentDf.withColumn("KGS_ADDRESS",when((lower(col("KGS_LOCATION_ADDRESS")).contains("rmz ecoworld")),"bangalore 1")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("dlf epitome")),"Epitome GGN")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("building no. 5. tower - c, dlf cyber city, phase - 2")),"GGN Building 5")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("dlf cyber city")),"GGN Building 10")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("global technology park")),"GTP Bangalore")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("hyderabad")),"Hyderabad")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("brigade world trade centre")),"Kochi")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("nesco")),"Nesco Mumbai")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("international tech park")),"Pune")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("godrej water side")),"Kolkata")\
                               .when((lower(col("KGS_LOCATION_ADDRESS")).contains("noida")),"Noida")\
                               .otherwise("NA"))
                            
display(currentDf)

# COMMAND ----------

currentDf=currentDf.withColumn("NATURE_OF_BUSINESS",trim(currentDf["NATURE_OF_BUSINESS"]))

# COMMAND ----------

Admin_list=['Housekeeping Services',
'O&M Electrical',
'Administration.Third Party',
'Security Services',
'Facility Services',
'SECURITY GUARDING - Premises',
'SECURITY GUARDING - Escort',
'OUTSOURCING & MANPOWER',
'Electrician',
'Facility Management',
'Integrated Facility Management Services',
'GC Service Work',
'Supply & Installation of Carpet Tiles and Chairs',
'System integrator',
'Retail (Supply and installation of Modular furniture and Chairs)',
'Supply of Modular Office Furniture',
'Dismantling work',
]

# COMMAND ----------

Business_list=['Staff rendering varied professional services.',
'Loaned staff from KPMG India rendering varied professional services.',
'Server Support - MySeat Application, IT, Software Providers.'
]

# COMMAND ----------

HR_List=['HR Services - Resourcing.']

# COMMAND ----------

IT_List=['IT Services', 
'Facility Management Services',
'DLP Monitoring',
'Hardware Support for Laptops',
'Audio Video Support',
'Network Cabling Engg',
'Printer Operator',
'Server Support',
]

# COMMAND ----------

currentDf=currentDf.withColumn("CATEGORY",when((col("NATURE_OF_BUSINESS").isin(Admin_list)),"Admin")\
                               .when((col("NATURE_OF_BUSINESS").isin(Business_list)),"Business")\
                               .when((col("NATURE_OF_BUSINESS").isin(HR_List)),"HR")\
                               .when((col("NATURE_OF_BUSINESS").isin(IT_List)),"IT")\
                               .otherwise("NA"))

# COMMAND ----------

## Capgemini vendor should be HR as told by Process Team
currentDf=currentDf.withColumn("CATEGORY",when((lower(col("CONTRACTOR_S_NAME")).contains("capgemini")),"HR")\
                                         .otherwise(col("CATEGORY")))

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