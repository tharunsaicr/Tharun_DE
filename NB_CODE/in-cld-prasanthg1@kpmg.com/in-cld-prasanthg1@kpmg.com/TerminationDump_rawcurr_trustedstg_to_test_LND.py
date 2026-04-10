# Databricks notebook source
dbutils.widgets.text(name = "CurrentMonth", defaultValue = "")
currentMonthLeft = dbutils.widgets.get("CurrentMonth") + " Left"

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
currentCutOff = dbutils.widgets.get("CurrentCutOffDate")

dbutils.widgets.text(name = "LastCutOffDate", defaultValue = "")
lastCutoff = dbutils.widgets.get("LastCutOffDate")

processName = "headcount"
tableName = "termination_dump"

print(currentMonthLeft)
print(currentCutOff)
print(lastCutoff)
print(processName)
print(tableName)

# COMMAND ----------

# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date
from datetime import datetime
from pyspark.sql.types import *

# COMMAND ----------

currentmonthDF = spark.sql("select * from hive_metastore.kgsonedatadb.test_raw_curr_headcount_termination_dump")

lastmonthDF = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_termination_dump where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_termination_dump where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+currentCutOff+"'"+"))) td_hist where rank = 1")
lastmonthDF = lastmonthDF.drop("rank")

prevResignLeft = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_resigned_and_left where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_resigned_and_left where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+currentCutOff+"'"+"))) res_lft_hist where rank = 1")
prevResignLeft = prevResignLeft.drop("rank")

# COMMAND ----------

currentmonthDF = currentmonthDF.drop("Status","Entity")

# COMMAND ----------

lastmonthDF = lastmonthDF.withColumnRenamed("Status","LastMonthStatus")
prevResignLeft = prevResignLeft.withColumnRenamed("Employee_Number","Prev_Resign_Left_Employee_Number")

# COMMAND ----------

# DBTITLE 1,Trasnform Entity column: KGS & KI based on the company name
#Add the value of Entity based on Company Name

currentmonthDF = currentmonthDF.withColumn("Entity",when((col("Company_Name") == "KPMG Resource Centre Private Limited") | (col("Company_Name") == "KPMG Global Services Management Private Limited")  | (col("Company_Name") == "KPMG Global Services Private Limited") | (col("Company_Name") == "KPMG Global Serv Mgmt Pvt Ltd") | (col("Company_Name") == "KPMG Global Delivery Center Private Limited"), "KGS").otherwise("KI"))

# COMMAND ----------

# joinedDf = currentmonthDF.join(lastmonthDF,currentmonthDF.Employee_Number == lastmonthDF.Employee_Number,"left").select(currentmonthDF["*"], lastmonthDF["LastMonthStatus"])

joinedDf = currentmonthDF.join(lastmonthDF,currentmonthDF.Employee_Number == lastmonthDF.Employee_Number,"left").join(prevResignLeft,currentmonthDF.Employee_Number == prevResignLeft.Prev_Resign_Left_Employee_Number,"left").select(currentmonthDF["*"], lastmonthDF["LastMonthStatus"],prevResignLeft["Prev_Resign_Left_Employee_Number"])


# COMMAND ----------

joinedDf.select("LastMonthStatus").distinct().show()

# COMMAND ----------

# DBTITLE 1,Transform Status when Entity = "KI"
#	Entity = KI and status available in prev TD then as it is status, otherwise if Entity is KI and status is NA then status=KI

joinedDf = joinedDf.withColumn("Status",when((col("Entity") == "KI") & (col("LastMonthStatus").isNotNull()), col("LastMonthStatus")) \
                               .when((col("Entity") == "KI") & (col("LastMonthStatus").isNull()), "KI").otherwise(""))

# COMMAND ----------

# display(joinedDf.select('Entity','LastMonthStatus','Status').distinct().sort('Entity',ascending = [False]))

# COMMAND ----------

# 9/19/2022 4:48pm
#Covert TerminationDate to DateType and create column UpdateTerminationDate

# joinedDf= joinedDf.withColumn("UpdatedTermination_Date", changeDateFormat(col("Termination_Date")))
# display(joinedDf.select("UpdatedTermination_Date"))

# COMMAND ----------

# DBTITLE 1,Update Status when Entity = "KGS"
joinedDf= joinedDf.withColumn("Status", when((col("Entity") == "KGS") & (col("LastMonthStatus").isNotNull()), "Old Termination")\
                              .when((col("Entity") == "KGS") & (col("LastMonthStatus").isNull()) & (col("Prev_Resign_Left_Employee_Number").isNotNull()), "Old Termination")\
                              .when((col("Entity") == "KGS") & (col("LastMonthStatus").isNull()) & (col("Prev_Resign_Left_Employee_Number").isNull()) & ((col("Termination_Date") != "1900-01-01") & (col("Termination_Date") < lastCutoff)), "Old Termination")\
                              .when((col("Entity") == "KGS") & (col("LastMonthStatus").isNull()) & (col("Prev_Resign_Left_Employee_Number").isNull()) & (col("Termination_Date").between(lastCutoff,currentCutOff)), currentMonthLeft)\
                              .otherwise(col("Status")))

# COMMAND ----------

# display(joinedDf.select('Entity','LastMonthStatus','Status').filter(joinedDf.Entity == "KGS").distinct().sort('Entity',ascending = [False]))

# COMMAND ----------

# display(joinedDf)

# COMMAND ----------

# d.	Check in prev month Left and Resigned tab for manual cases -> if emplyoyee number from current TD is available in last month Final HC 'Resigned and left' also and status is 'current month left' (in current TD) then change to 'old termination'. 

# finalDf= joinedDf.withColumn("Status", when((joinedDf.Entity == "KGS") & (joinedDf.Prev_Resign_Left_Employee_Number.isNotNull()) & (joinedDf.Status == currentMonthLeft), "Old Termination").otherwise(col("Status")))

# COMMAND ----------

# display(finalDf.select('Entity','vlookup','Status').filter((col("Entity") == "KGS")))
joinedDf.select('Entity','LastMonthStatus','Status').distinct().sort('Entity',ascending = [False]).show()

# COMMAND ----------

finaldf = joinedDf.drop("LastMonthStatus","Prev_Resign_Left_Employee_Number")
# finaldf = finaldf.withColumnRenamed("Source_of_Hire_Details31","Source_of_Hire_Details").withColumnRenamed("Source_Of_Hire_Details32","Source_of_Hire_Details")

# COMMAND ----------

# DBTITLE 1,Drop duplicates
finaldf = finaldf.dropDuplicates()

# COMMAND ----------

# finaldf.select('Entity','Status').distinct().sort('Entity',ascending = [False]).show()

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
finaldf = finaldf.withColumn("Dated_On",lit(currentdatetime))

finaldf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True")\
.option("path",trusted_stg_savepath_url+processName+"/"+tableName+"_test") \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.test_trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
# dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})