# Databricks notebook source
# This NB  extract 2 tables from the source mapping files and 2 tables from HC fact tables
# Names in PBI : Employee Type & Payroll/Contractor, in delta table: Employee_Type & Designation_Mapping


# COMMAND ----------

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "File_Year", defaultValue = "")
File_Year = dbutils.widgets.get("File_Year")

dbutils.widgets.text(name = "File_Month", defaultValue = "")
File_Month = dbutils.widgets.get("File_Month")

print(tableName)
print(processName)
print(ReportName)
print(File_Year)
print(File_Month)

# COMMAND ----------

import re,string
import pytz
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import * 
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,lit,when,concat,trim,substring,lower,upper,from_unixtime,unix_timestamp,to_timestamp
currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Format Headers Data from Fact tables

Designation_Mapping=spark.sql('''select distinct Designation_Mapping from kgsfinancedb.trusted_curr_fr_contractor_hc_actual 
UNION 
select distinct Designation_Mapping from kgsfinancedb.trusted_curr_fr_contractor_hc_plan
UNION
select distinct Designation_Mapping from kgsfinancedb.trusted_curr_fr_contractor_hc_forecast''')
display(Designation_Mapping)

Employee_Type = spark.sql('''select distinct Employee_Type from kgsfinancedb.trusted_curr_fr_contractor_hc_plan
UNION 
select distinct Employee_Type from kgsfinancedb.trusted_curr_fr_contractor_hc_actual 
UNION
select distinct Employee_Type from kgsfinancedb.trusted_curr_fr_contractor_hc_forecast''')
display(Employee_Type)

# COMMAND ----------

# DBTITLE 1,Adding Dated_on column
currentdatetime= datetime.now()
Designation_Mapping=Designation_Mapping.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
.withColumn("File_Month", lit(File_Month))\
.withColumn("File_Year", lit(File_Year))  
Employee_Type=Employee_Type.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))\
.withColumn("File_Month", lit(File_Month))\
.withColumn("File_Year", lit(File_Year))  


# COMMAND ----------

# DBTITLE 1,Load current data in trusted layer (Designation_Mapping)
#contractor Designation_Mapping current table (Over write mode)
Designation_Mapping.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName+"_Designation_Mapping") \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+processName +"_"+tableName+ "_Designation_Mapping")

# COMMAND ----------

# DBTITLE 1,Current & Hist table names and  path
saveTableName = "kgsfinancedb.trusted_curr_"+ ReportName + "_" +processName + "_"+ tableName+"_Designation_Mapping"
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName+"_Designation_Mapping")
curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName + "_" +processName + "_"+ tableName+"_Designation_Mapping"
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist data(Designation_Mapping)
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName+"_Designation_Mapping",'processName':processName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName+"_Designation_Mapping",'ProcessName':processName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Loading Delta trusted tables to SQL database (Designation_Mapping)
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName+"_designation_mapping",'ProcessName':processName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Load current data in trusted layer
#contractor Employee_Type current table (Over write mode)
Employee_Type.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName+"_Employee_Type") \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+processName + "_" +tableName+"_Employee_Type")

# COMMAND ----------

# DBTITLE 1,Current & Hist table names and  path
saveTableName = "kgsfinancedb.trusted_curr_"+ ReportName + "_" +processName + "_"+ tableName+"_Employee_Type"
print(saveTableName)
print("path : ",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName+"_Employee_Type")
curr_table_name=saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName + "_" +processName + "_"+ tableName+"_Employee_Type"
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load Trusted Hist Data
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/dim_trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName+"_Employee_Type",'processName':processName,'ReportName':ReportName,'File_Year':File_Year,'File_Month':File_Month})
else:
    print("Creating  Hist table on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName+"_Employee_Type",'ProcessName':processName,'ReportName':ReportName})

# COMMAND ----------

# DBTITLE 1,Loading Delta trusted tables to SQL database
dbutils.notebook.run("/kgsfinance/trusted/Delta_to_SQL_with_Select",6000,{'DeltaTableName':tableName+"_employee_type",'ProcessName':processName,'ReportName':ReportName})

# COMMAND ----------

