# Databricks notebook source
processName = "employee_engagement"
tableName = "encore_output"

# COMMAND ----------

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# Headcount Employee_dump data from delta table trusted_stg_headcount_employee_dump 

df_emp_dump = spark.sql("select * from kgsonedatadb.trusted_hist_headcount_employee_dump where File_date = '20231003'")

df_thanks_dump = spark.sql("select * from kgsonedatadb.trusted_employee_engagement_thanks_dump where upper(Reward_Status) in ('APPROVED','APPROVAL NOT REQUIRED')")

df_thanks_dump.count()

# COMMAND ----------

import pyspark
from pyspark.sql.functions import lit, col, split, date_format,concat,substring
import pyspark.sql.functions as f



df_empdump_thanksdump_join = df_thanks_dump.join(df_emp_dump, df_thanks_dump.Nominee_s_Employee_ID == df_emp_dump.Employee_Number, "left").select(df_thanks_dump["*"],df_emp_dump["*"])

df_empdump_thanksdump_join = df_empdump_thanksdump_join.filter(col('Entity') == lit("KGS"))

df_empdump_thanksdump_join.count()



# COMMAND ----------

#Bring BU by joining CC_BU with ED
df_config_cc_bu = spark.sql("select distinct Cost_centre,BU from kgsonedatadb.config_cost_center_business_unit")

df_empdump_thanksdump_join = df_empdump_thanksdump_join.join(df_config_cc_bu,df_empdump_thanksdump_join.Cost_centre == df_config_cc_bu.Cost_centre,"left").select(df_empdump_thanksdump_join["*"],df_config_cc_bu["BU"])

# df_empdump_thanksdump_join = df_empdump_thanksdump_join.withColumnRenamed("BU","Final_Function");

df_empdump_thanksdump_join = df_empdump_thanksdump_join.withColumnRenamed("Reward_Name","Category").withColumnRenamed("Employee_Number","Emp_No").withColumnRenamed("Full_Name","Final_Name").withColumnRenamed("Position","Designation").withColumnRenamed("Email_Address","Email_ID");



# COMMAND ----------

df_empdump_thanksdump_join=df_empdump_thanksdump_join.withColumn("date",when(f.upper(col('Reward_Status')) == 'APPROVED',date_format(to_date("Reward_Date",'yyyy-MM-dd'),"dd/MM/yyyy")).otherwise(date_format(to_date("Nomination_Date",'yyyy-MM-dd'),"dd/MM/yyyy"))).withColumn('Year',f.year(f.to_timestamp('date','dd/MM/yyyy'))).withColumn('Quarter',f.quarter(f.to_timestamp('date','dd/MM/yyyy'))).withColumn('Month',f.month(f.to_timestamp('date','dd/MM/yyyy'))).withColumn("Level", lit("null"));

df_empdump_thanksdump_join.count()

df_empdump_thanksdump_join = df_empdump_thanksdump_join.withColumn("quarter",when(col('quarter') == 4,1).otherwise(col('quarter')+1))

df_empdump_thanksdump_join.count()

df_empdump_thanksdump_join = df_empdump_thanksdump_join.withColumn("Year",when(col('month').isin(10,11,12),concat(lit("FY"),substring(col('Year')+1,3,2))).otherwise(concat(lit("FY"),substring(col('Year'),3,2))))

df_empdump_thanksdump_join.count()

# COMMAND ----------

# BU Not Available for below costCenter:

# Forensic-Inv - NA
# Nexus-Digital Adoption & Change - fixed
# Capability Hubs-Research-GCM Platinum Support - fixed
# MC-Advisory Solutions Enablement - fixed
# RC Core-Modeling Tech - fixed
# MC-PMO-Connected - fixed
# MC-IT Advisory-Testing QA - fixed
# Finance but we have CF-Finance - NA
# KGS Core ESG-Management - fixed
# M&A Consulting - NA
# Audit-Corporates Listed and Regulated - fixed

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col

currentDf= df_empdump_thanksdump_join.select( "Emp_No", "Final_Name","Company_Name" , "Designation","BU","Category","Reward_Date","Nomination_Date", "Reward_Status" ,"Nominee_s_Unit", "Quarter", "Year","Email_ID","Level","Gender","Gratuity_Date","Budget_From","Nominator_s_Employee_ID")

# COMMAND ----------

currentDf=currentDf.withColumn("File_Date", lit(FileDate))

# COMMAND ----------

currentDf.count()

# COMMAND ----------

# currentDf.write \
# .mode("overwrite") \
# .format("delta") \
# .option("overwriteSchema","true") \
# .option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
# .option("compression","snappy") \
# .saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_employee_engagement_encore_output where File_Date = '20230930'

# COMMAND ----------

Date 