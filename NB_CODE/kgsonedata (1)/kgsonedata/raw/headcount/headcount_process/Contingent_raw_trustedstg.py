# Databricks notebook source
dbutils.widgets.text(name = "CurrentMonth", defaultValue = "")
currentMonth = dbutils.widgets.get("CurrentMonth")
currentMonthLeft = dbutils.widgets.get("CurrentMonth") + " Left"

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
currentCutOff = dbutils.widgets.get("CurrentCutOffDate")

dbutils.widgets.text(name = "LastCutOffDate", defaultValue = "")
lastCutoff = dbutils.widgets.get("LastCutOffDate")

tableName = "contingent"
processName = "headcount"

print(currentMonthLeft)
print(currentCutOff)
print(lastCutoff)
print(tableName)
print(processName)

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse


# COMMAND ----------

# %sql

# --There are some special character in hist_headcount_contingent_worker_resigned need to remove that

# select * from 
# -- hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent
# -- hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent_worker
# hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned
# where candidate_id like '%586222%' --and File_Date = '20220720'

# COMMAND ----------

currentCW = spark.sql("select * from hive_metastore.kgsonedatadb.raw_curr_headcount_contingent")

# display(currentCW)

previousCW = spark.sql("select * from (select rank() over(partition by candidate_id,file_date order by dated_on desc) as rank, * from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent where file_date = (select max(file_date) from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+currentCutOff+"'"+"))) C_hist where rank = 1")
previousCW = previousCW.drop("rank")

# display(previousCW)

previousHCCW = spark.sql("select * from (select rank() over(partition by candidate_id,file_date order by dated_on desc) as rank, * from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent_worker where file_date = (select max(file_date) from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent_worker where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+currentCutOff+"'"+"))) HCCW_hist where rank = 1")
previousHCCW = previousHCCW.drop("rank")

# display(previousHCCW)

previousHCLoanedContingentResigned = spark.sql("select * from (select rank() over(partition by candidate_id,file_date order by dated_on desc) as rank, * from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned where file_date = (select max(file_date) from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+currentCutOff+"'"+"))) HCLoanedContingentResigned_hist where rank = 1")
previousHCLoanedContingentResigned = previousHCLoanedContingentResigned.drop("rank")

# display(previousHCLoanedContingentResigned)

# COMMAND ----------

# DBTITLE 1,Filter records where Candidate_Id is notNull
currentCW = currentCW.filter(col("Candidate_Id").isNotNull())
display(currentCW)

# COMMAND ----------

# DBTITLE 1,Drop Duplicates based on Candidate_Id
currentCW = currentCW.dropDuplicates(["Candidate_Id"])
display(currentCW.select(count(currentCW.Candidate_Id)))

# COMMAND ----------

currentCW = currentCW.drop("Status")
previousCW = previousCW.withColumnRenamed('Status','LastMonthStatus')
previousHCCW = previousHCCW.withColumnRenamed('Candidate_Id','HCCWCandidate_ID')
previousHCLoanedContingentResigned = previousHCLoanedContingentResigned.withColumnRenamed('Candidate_Id','HCLoanedResignedCandidate_ID')

# COMMAND ----------

# 9/19/2022
# currentCW = (currentCW.withColumn("UpdatedDate_of_Joining", changeDateFormat(col("Date_of_Joining"))))

# display(currentCW)


# COMMAND ----------

# DBTITLE 1,Add Data from HC Contingent Worker and  Loaned Resign Left
joinedDf = currentCW.join(previousCW,currentCW.Candidate_Id == previousCW.Candidate_Id,"left").join(previousHCCW,currentCW.Candidate_Id == previousHCCW.HCCWCandidate_ID,"left").join(previousHCLoanedContingentResigned,currentCW.Candidate_Id == previousHCLoanedContingentResigned.HCLoanedResignedCandidate_ID,"left").select(currentCW["*"],previousCW["LastMonthStatus"], previousHCCW["HCCWCandidate_ID"],previousHCLoanedContingentResigned["HCLoanedResignedCandidate_ID"])


display(joinedDf)

# COMMAND ----------

joinedDf = joinedDf.withColumn("Status",col("LastMonthStatus"))
display(joinedDf)

# COMMAND ----------

# 9/19/2022 6:01 pm

# # Old Termination
# finalDf = joinedDf.withColumn("Status",when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "OLD TERMINATION") | ((upper(col("LastMonthStatus")).like("%LEFT%")) & (col("HCLoanedResignedCandidate_ID").isNotNull())))),"Old Termination")\
#                               .when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "HC") | (upper(col("LastMonthStatus")) == "NEW JOINERS")) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((upper(col("LastMonthStatus")) == "NEW JOINERS") | (upper(col("LastMonthStatus")) == "HC")) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") ) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .otherwise(col("Status")))


# # Current Month Left
# finalDf = joinedDf.withColumn("Status",when(((col("Contingent_Worker_Status") == "Terminated") & (upper(col("LastMonthStatus")).like("%LEFT%")) & (col("HCCWCandidate_ID").isNotNull())),currentMonthLeft)\
#                               .when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "HC") | (upper(col("LastMonthStatus")) == "NEW JOINERS")) & (col("HCLoanedResignedCandidate_ID").isNull())),currentMonthLeft)\
#                               .otherwise(col("Status")))

# # New Joiners + Left
# finalDf = joinedDf.withColumn("Status",when((col("Contingent_Worker_Status") == "Terminated") & (((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") )) & (col("HCCWCandidate_ID").isNull()) & (col("HCCWCandidate_ID").isNull()),"New Joiners + Left")\
#                              .otherwise(col("Status")))

# # HC
# finalDf = joinedDf.withColumn("Status",when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((upper(col("LastMonthStatus")) == "NEW JOINERS") | (upper(col("LastMonthStatus")) == "HC")) & (col("HCCWCandidate_ID").isNotNull())),"HC")\
#                              .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") ) & (col("HCCWCandidate_ID").isNotNull())),"HC")\
#                              .otherwise(col("Status")))


# # Removed RPO
# finalDf = joinedDf.withColumn("Status",when(((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "")) & (col("Job")== "085.Contractor-RPO") & (col("Company") == "KPMG Global Services Management Private Limited"),"Removed RPO")\
#                               .otherwise(col("Status")))
# # New Joiners
# finalDf = joinedDf.withColumn("Status",when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "")) & (col("Date_of_Joining").between(lastCutoff,currentCutOff))) |((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == ""))) & ((col("HCCWCandidate_ID").isNull())&(col("HCLoanedResignedCandidate_ID").isNull()))), "New Joiners")\
#                               .otherwise(col("Status")))                           

# COMMAND ----------

# # 9/19/2022 

# # Old Termination
# finalDf = joinedDf.withColumn("Status",when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "OLD TERMINATION") | ((upper(col("LastMonthStatus")).like("%LEFT%")) & (col("HCLoanedResignedCandidate_ID").isNotNull())))),"Old Termination")\
#                               .when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "HC") | (upper(col("LastMonthStatus")) == "NEW JOINERS")) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((upper(col("LastMonthStatus")) == "NEW JOINERS") | (upper(col("LastMonthStatus")) == "HC")) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") ) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .otherwise(col("Status")))


# # Current Month Left
# finalDf = joinedDf.withColumn("Status",when(((col("Contingent_Worker_Status") == "Terminated") & (upper(col("LastMonthStatus")).like("%LEFT%")) & (col("HCCWCandidate_ID").isNotNull())),currentMonthLeft)\
#                               .when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "HC") | (upper(col("LastMonthStatus")) == "NEW JOINERS")) & (col("HCLoanedResignedCandidate_ID").isNull())),currentMonthLeft)\
#                               .otherwise(col("Status")))

# # New Joiners + Left
# finalDf = joinedDf.withColumn("Status",when((col("Contingent_Worker_Status") == "Terminated") & (((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") )) & (col("HCCWCandidate_ID").isNull()) & (col("HCCWCandidate_ID").isNull()),"New Joiners + Left")\
#                              .otherwise(col("Status")))

# # HC
# finalDf = joinedDf.withColumn("Status",when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((upper(col("LastMonthStatus")) == "NEW JOINERS") | (upper(col("LastMonthStatus")) == "HC")) & (col("HCCWCandidate_ID").isNotNull())),"HC")\
#                              .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") ) & (col("HCCWCandidate_ID").isNotNull())),"HC")\
#                              .otherwise(col("Status")))


# # Removed RPO
# finalDf = joinedDf.withColumn("Status",when(((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "")) & (col("Job")== "085.Contractor-RPO") & (col("Company") == "KPMG Global Services Management Private Limited"),"Removed RPO")\
#                               .otherwise(col("Status")))
# # New Joiners
# finalDf = joinedDf.withColumn("Status",when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "")) & (col("UpdatedDate_of_Joining").between(lastCutoff,currentCutOff))) |((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == ""))) & ((col("HCCWCandidate_ID").isNull())&(col("HCLoanedResignedCandidate_ID").isNull()))), "New Joiners")\
#                               .otherwise(col("Status")))                           

# COMMAND ----------

finalDf = joinedDf.withColumn("Status",when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "OLD TERMINATION") | ((upper(col("LastMonthStatus")).like("%LEFT%")) & (col("HCLoanedResignedCandidate_ID").isNotNull())))),"Old Termination")\
                              .when(((col("Contingent_Worker_Status") == "Terminated") & (upper(col("LastMonthStatus")).like("%LEFT%")) & (col("HCCWCandidate_ID").isNotNull())),currentMonthLeft)\
                              .when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "HC") | (upper(col("LastMonthStatus")) == "NEW JOINERS")) & (col("HCLoanedResignedCandidate_ID").isNull())),currentMonthLeft)\
                              .when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "HC") | (upper(col("LastMonthStatus")) == "NEW JOINERS")) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
                              .when((col("Contingent_Worker_Status") == "Terminated") & (((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") )) & (col("HCCWCandidate_ID").isNull()) & (col("HCCWCandidate_ID").isNull()),"New Joiners + Left")\
                              .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((upper(col("LastMonthStatus")) == "NEW JOINERS") | (upper(col("LastMonthStatus")) == "HC")) & (col("HCCWCandidate_ID").isNotNull())),"HC")\
                              .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((upper(col("LastMonthStatus")) == "NEW JOINERS") | (upper(col("LastMonthStatus")) == "HC")) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
                              .when(((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "")) & (col("Job")== "085.Contractor-RPO") & (col("Company") == "KPMG Global Services Management Private Limited"),"Removed RPO")\
                              .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "")) & (col("Date_of_Joining").between(lastCutoff,currentCutOff))) |((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == ""))) & ((col("HCCWCandidate_ID").isNull())&(col("HCLoanedResignedCandidate_ID").isNull()))), "New Joiners")\
                              .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") | (upper(col("LastMonthStatus")).like("%LEFT%")) ) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
                              .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") ) & (col("HCCWCandidate_ID").isNotNull())),"HC")\
                              .otherwise(col("LastMonthStatus")))

# COMMAND ----------

# # Correct Output except for 2
# finalDf = joinedDf.withColumn("Status",when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "OLD TERMINATION") | ((upper(col("LastMonthStatus")).like("%LEFT%")) & (col("HCLoanedResignedCandidate_ID").isNotNull())))),"Old Termination")\
#                               .when(((col("Contingent_Worker_Status") == "Terminated") & (upper(col("LastMonthStatus")).like("%LEFT%")) & (col("HCCWCandidate_ID").isNotNull())),currentMonthLeft)\
#                               .when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "HC") | (upper(col("LastMonthStatus")) == "NEW JOINERS")) & (col("HCLoanedResignedCandidate_ID").isNull())),currentMonthLeft)\
#                               .when(((col("Contingent_Worker_Status") == "Terminated") & ((upper(col("LastMonthStatus")) == "HC") | (upper(col("LastMonthStatus")) == "NEW JOINERS")) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .when((col("Contingent_Worker_Status") == "Terminated") & (((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") )) & (col("HCCWCandidate_ID").isNull()) & (col("HCCWCandidate_ID").isNull()),"New Joiners + Left")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((upper(col("LastMonthStatus")) == "NEW JOINERS") | (upper(col("LastMonthStatus")) == "HC")) & (col("HCCWCandidate_ID").isNotNull())),"HC")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((upper(col("LastMonthStatus")) == "NEW JOINERS") | (upper(col("LastMonthStatus")) == "HC")) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .when(((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "")) & (col("Job")== "085.Contractor-RPO") & (col("Company") == "KPMG Global Services Management Private Limited"),"Removed RPO")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "")) & (col("UpdatedDate_of_Joining").between(lastCutoff,currentCutOff))) |((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == ""))) & ((col("HCCWCandidate_ID").isNull())&(col("HCLoanedResignedCandidate_ID").isNull()))), "New Joiners")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") ) & (col("HCLoanedResignedCandidate_ID").isNotNull())),"Old Termination")\
#                               .when((((col("Contingent_Worker_Status").isNull()) | (col("Contingent_Worker_Status") == "")) & ((col("LastMonthStatus").isNull()) | (col("LastMonthStatus") == "") ) & (col("HCCWCandidate_ID").isNotNull())),"HC")\
#                               .otherwise(col("LastMonthStatus")))

# COMMAND ----------

# display(finalDf.select("LastMonthStatus").distinct())
# display(finalDf.select("Status").distinct())

# COMMAND ----------

# display(finalDf.select('Candidate_Id','Full_Name','Contingent_Worker_Status','LastMonthStatus','HCCWCandidate_ID','HCLoanedResignedCandidate_ID','Status').filter(col('Candidate_Id').isin(607100,621706,625405,626802)))

# COMMAND ----------

# display(finalDf.select('Candidate_Id','Full_Name','Contingent_Worker_Status','LastMonthStatus','HCCWCandidate_ID','HCLoanedResignedCandidate_ID','Status').filter(finalDf.Status == "Jul Left"))

# display(finalDf.select("Business_Category").distinct())

# COMMAND ----------

finalDf = finalDf.drop("LastMonthStatus","HCCWCandidate_ID","HCLoanedResignedCandidate_ID","Business_Category")

# finalDf = finalDf.withColumnRenamed('Qualification_Type64','Qualification_Type')\
# .withColumnRenamed('Others_Qualification_Type65','Others_Qualification_Type')\
# .withColumnRenamed('End_Date66','End_Date')\
# .withColumnRenamed('Institute_Name67','Institute_Name')\
# .withColumnRenamed('Institute_Location68','Institute_Location')\
# .withColumnRenamed('Qualification_Type69','Qualification_Type')\
# .withColumnRenamed('Others_Qualification_Type70','Others_Qualification_Type')\
# .withColumnRenamed('End_Date72','End_Date')\
# .withColumnRenamed('Institute_Name73','Institute_Name')\
# .withColumnRenamed('Institute_Location74','Institute_Location')\

# COMMAND ----------

# Commented on 3/10/2023
# finalDf = finalDf.withColumn("PML_Employee_Number", col("PML_Employee_Number").cast(IntegerType()))
# finalDf = finalDf.withColumn("Buddy_Employee_Number", col("Buddy_Employee_Number").cast(IntegerType()))

display(finalDf)

# COMMAND ----------

# DBTITLE 1,Drop Duplicates
finalDf = finalDf.dropDuplicates()

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
finalDf = finalDf.withColumn("Dated_On",lit(currentdatetime))

finalDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# kgsonedatadb.trusted_stg_headcount_contingent

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})