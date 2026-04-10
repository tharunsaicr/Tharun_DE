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

import pyspark
from pyspark.sql.functions import lit, col, split, date_format,concat,substring,lower,to_timestamp,date_format,col,coalesce
import pyspark.sql.functions as f

# Headcount Employee_dump data from delta table trusted_stg_headcount_employee_dump 

# df_emp_dump = spark.sql("select * from kgsonedatadb.trusted_headcount_employee_dump")

df_emp_dump = spark.sql("select Employee_Number,Full_Name,Company_Name,Position,Cost_centre,Email_Address,Job_Name,Gender,Gratuity_Date,Entity  from (select rank() over(partition by Employee_Number order by file_date desc, dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump)ed_hist where rank=1").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))
print(df_emp_dump.count())

# COMMAND ----------

display(df_emp_dump.select('Position','Job_Name').distinct())

# COMMAND ----------

# Reading CWK data and join with ED
df_contingent = spark.sql("select Candidate_Id as Employee_Number,Full_Name,Company as Company_Name,Position,Cost_centre,Official_Email_ID as Email_Address,Job as Job_Name,Gender,'CWK' as Gratuity_Date,'KGS' as Entity  from (select rank() over(partition by Candidate_Id order by file_date desc, dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_contingent)contingent_hist where rank=1").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))
print(df_contingent.count())

df_emp_dump = df_emp_dump.union(df_contingent)
print(df_emp_dump.count())

# COMMAND ----------

display(df_contingent.select('Position','Job_Name').distinct())

# COMMAND ----------

from pyspark.sql.functions import split
df_contingent_new = df_contingent.select(split(df_contingent.Position, '\\.').getItem(2))
display(df_contingent_new)

# COMMAND ----------

#Read Levelwise dimension table and join with main data to get Level column
df_level_wise = spark.read.table("kgsonedatadb.config_dim_level_wise").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1));
df_level_wise=df_level_wise.drop("Dated_On","File_Date")
df_level_wise=df_level_wise.withColumn('Mapping',when(col('Mapping').contains('Senior Associate Director') | col('Mapping').contains('Associate Director') | col('Mapping').contains('Director +'),'AD and Above').otherwise(col('Mapping')))

df_emp_dump = df_emp_dump.join(df_level_wise, (lower(df_emp_dump.Position) == lower(df_level_wise.Position)) & (lower(df_emp_dump.Job_Name) == lower(df_level_wise.Job_Name)),'left').select(df_emp_dump["*"],df_level_wise["Mapping"])

print(df_emp_dump.count())

# COMMAND ----------

# display(df_level_wise.select('Position','Job_Name','Mapping').distinct())

# COMMAND ----------

# display(df_emp_dump.select('Position','Job_Name','Mapping').where(col('Mapping').isNull()).distinct())

# COMMAND ----------

#Read thanks_dump and join with ED and Contingent
# df_thanks_dump = spark.sql("select * from kgsonedatadb.trusted_employee_engagement_thanks_dump where upper(Reward_Status) in ('APPROVED','APPROVAL NOT REQUIRED')")

df_thanks_dump = spark.sql("select * from (select rank() over(partition by Nominee_s_Employee_ID,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+FileDate+"','yyyyMMdd'"+"))) thanksdump_hist where rank = 1 and upper(Reward_Status) in ('APPROVED','APPROVAL NOT REQUIRED')")

print(df_thanks_dump.count())

df_empdump_thanksdump_join = df_thanks_dump.join(df_emp_dump, df_thanks_dump.Nominee_s_Employee_ID == df_emp_dump.Employee_Number, "left").select(df_thanks_dump["*"],df_emp_dump["*"])
print(df_thanks_dump.count())

df_empdump_thanksdump_join = df_empdump_thanksdump_join.filter(col('Entity') == lit("KGS"))

# COMMAND ----------

display(df_empdump_thanksdump_join.select('Position','Job_Name','Mapping').where(col('Mapping').isNull()).distinct())

df_empdump_thanksdump_join=df_empdump_thanksdump_join.withColumn("Mapping",when(lower(coalesce(df_empdump_thanksdump_join.Job_Name,lit('NA'))).contains('contractor'),lit('Contractor')).otherwise(col('Mapping')))

# COMMAND ----------

display(df_empdump_thanksdump_join.select('Position','Job_Name','Mapping').where(col('Mapping').isNull()).distinct())
display(df_empdump_thanksdump_join.select('Position','Job_Name','Mapping').distinct())

# COMMAND ----------

df_empdump_thanksdump_join=df_empdump_thanksdump_join.withColumn("Position",when(lower(coalesce(df_empdump_thanksdump_join.Mapping,lit('NA'))).contains('contractor'),lit(split(df_empdump_thanksdump_join.Position, '\\.').getItem(2))).otherwise(col('Position')))

# COMMAND ----------

#Bring BU by joining CC_BU with ED
df_config_cc_bu = spark.sql("select distinct Cost_centre,Final_BU as BU from kgsonedatadb.config_hist_cc_bu_sl")

df_empdump_thanksdump_join = df_empdump_thanksdump_join.join(df_config_cc_bu,df_empdump_thanksdump_join.Cost_centre == df_config_cc_bu.Cost_centre,"left").select(df_empdump_thanksdump_join["*"],df_config_cc_bu["BU"])

# df_empdump_thanksdump_join = df_empdump_thanksdump_join.withColumnRenamed("BU","Final_Function");

# COMMAND ----------

df_empdump_thanksdump_join = df_empdump_thanksdump_join.withColumnRenamed("Reward_Name","Category").withColumnRenamed("Employee_Number","Emp_No").withColumnRenamed("Full_Name","Final_Name").withColumnRenamed("Position","Designation").withColumnRenamed("Email_Address","Email_ID").withColumnRenamed("Mapping","Level");

df_empdump_thanksdump_join=df_empdump_thanksdump_join.withColumn("date",when(f.upper(col('Reward_Status')) == 'APPROVED',date_format(to_date("Reward_Date",'yyyy-MM-dd'),"dd/MM/yyyy")).otherwise(date_format(to_date("Nomination_Date",'yyyy-MM-dd'),"dd/MM/yyyy"))).withColumn('Year',f.year(f.to_timestamp('date','dd/MM/yyyy'))).withColumn('Quarter',f.quarter(f.to_timestamp('date','dd/MM/yyyy'))).withColumn('Month',f.month(f.to_timestamp('date','dd/MM/yyyy')));

df_empdump_thanksdump_join = df_empdump_thanksdump_join.withColumn("quarter",when(col('quarter') == 4,1).otherwise(col('quarter')+1))

df_empdump_thanksdump_join = df_empdump_thanksdump_join.withColumn("Year",when(col('month').isin(10,11,12),concat(lit("FY"),substring(col('Year')+1,3,2))).otherwise(concat(lit("FY"),substring(col('Year'),3,2))))

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

currentDf= df_empdump_thanksdump_join.select( "Emp_No", "Final_Name","Company_Name" , "Designation","BU","Category","Reward_Date","Nomination_Date", "Reward_Status" ,"Nominee_s_Unit", "Quarter", "Year","Email_ID","Level","Gender","Gratuity_Date","Budget_From","Nominator_s_Employee_ID")

# COMMAND ----------

currentDf=currentDf.withColumn("File_Date", lit(FileDate))
currentDf=currentDf.dropDuplicates()
print(currentDf.count())

# COMMAND ----------

currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})