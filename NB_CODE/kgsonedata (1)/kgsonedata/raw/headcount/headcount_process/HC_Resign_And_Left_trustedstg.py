# Databricks notebook source
import datetime

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
current_cutoff_date = dbutils.widgets.get("CurrentCutOffDate")

current_cutoff_date = datetime.datetime.strptime(current_cutoff_date, '%Y-%m-%d').date()

currentMonth = current_cutoff_date.strftime("%B")
currentMonthLeft = currentMonth + " Left"

tableName = "resigned_and_left"
processName = "headcount"

print(currentMonthLeft)
print(tableName)

convertedFileDate = current_cutoff_date
print(convertedFileDate)

# COMMAND ----------

# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,concat
from datetime import datetime
from pyspark.sql.types import *

# COMMAND ----------

# %sql
# select distinct status from kgsonedatadb.trusted_stg_headcount_termination_dump --where status like '%Left%'

# COMMAND ----------

# Commented and changed on 5/18/2023
# currentTD = spark.sql("select * from kgsonedatadb.trusted_stg_headcount_termination_dump where status like '%Left%'")
currentTD = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_termination_dump where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_termination_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+str(current_cutoff_date)+"'"+"))) td_hist where rank = 1 and lower(status) like '%left%' ")

# display(currentTD)

# Commented and changed on 5/18/2023
# currentED = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_stg_headcount_employee_dump where status like '%Left%'")

currentED = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+str(current_cutoff_date)+"'"+"))) ed_hist where rank = 1 and lower(status) like '%left%' ")

# display(currentED)

# previousResign = spark.sql("select * from hive_metastore.kgs1data.adhoc_last_cutoff_resigned_left limit 2")
# display(previousResign)


# COMMAND ----------

# DBTITLE 1,Derive BU for TD
costcenterBUdf = spark.sql("select * from hive_metastore.kgsonedatadb.config_cost_center_business_unit")
costcenterBUdf = costcenterBUdf.withColumnRenamed("Cost_centre","Mapping_Cost_centre").drop("Dated_On","File_Date")
# display(costcenterBUdf)

joinedTDDf = currentTD.join(costcenterBUdf,currentTD.Cost_Centre == costcenterBUdf.Mapping_Cost_centre,"left")
# display(joinedDf)

currentTD = joinedTDDf.drop("Mapping_Cost_centre")
# display(currentTD)


currentTD.createOrReplaceTempView("currentTD")

# COMMAND ----------

# DBTITLE 1,Derive BU for ED
costcenterBUdf = spark.sql("select * from hive_metastore.kgsonedatadb.config_cost_center_business_unit")
costcenterBUdf = costcenterBUdf.withColumnRenamed("Cost_centre","Mapping_Cost_centre").drop("Dated_On","File_Date")
# display(costcenterBUdf)

joinedEDDf = currentED.join(costcenterBUdf,currentED.Cost_centre == costcenterBUdf.Mapping_Cost_centre,"left")
# display(joinedDf)

currentED = joinedEDDf.drop("Mapping_Cost_centre")
# display(currentTD)

currentED.createOrReplaceTempView("currentED")

# COMMAND ----------

# 	Copy the “Current month left” employees from emp. dump
# 	Paste it in final HC “Resigned and Left” tab as per the headers
# 	Copy all the“Current month left” Employees from Termination Dump
# 	And paste it in Final HC “Resigned and Left” tab as per the headers


# COMMAND ----------

#Yet to add BU as part of ED and TD

# COMMAND ----------

# currentmonthTD = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Business_Category,User_Type,Client_Geography,Position,Location,Sub_Location,Job_Name,Date_First_Hired,Termination_Date,Gender,Employee_Category,People_Group_Name,Company_Name,Email_Address,Status,BU from kgsonedatadb.trusted_stg_headcount_termination_dump")

currentmonthTD = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Business_Category,User_Type,Client_Geography,Position,Location,Sub_Location,Job_Name,Date_First_Hired,Termination_Date,Gender,Employee_Category,People_Group_Name,Company_Name,Email_Address,Status,BU,File_Date from currentTD")

currentmonthTD = currentmonthTD.filter((col("Status") == currentMonthLeft) | (col("Status").like("%Left%")))
# currentmonthTD = currentmonthTD.filter(col("Status") == currentMonthLeft)

# display(currentmonthTD)

currentmonthTD = currentmonthTD.drop("Status")

# COMMAND ----------

# currentMonthED = spark.sql("""

# select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Business_Category,User_Type,Client_Geography,Position,Location,Sub_Location,Job_Name,Date_First_Hired,Termination_Date,Gender,Employee_Category,People_Group_Name,Company_Name,Email_Address,Status,BU from hive_metastore.kgsonedatadb.raw_stg_headcount_employee_dump

# """)

currentMonthED = spark.sql("""

select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Business_Category,User_Type,Client_Geography,Position,Location,Sub_Location,Job_Name,Date_First_Hired,Termination_Date,Gender,Employee_Category,People_Group_Name,Company_Name,Email_Address,Status,BU,File_Date from currentED

""")

currentMonthED = currentMonthED.filter((col("Status") == currentMonthLeft) | (col("Status").like("%Left%")))
# currentMonthED = currentMonthED.filter(col("Status") == currentMonthLeft)

# display(currentMonthED)

currentMonthED = currentMonthED.drop("Status")

# COMMAND ----------

finalDf = currentMonthED.union(currentmonthTD)


# COMMAND ----------

finalDf = finalDf.withColumn("Date_First_Hired", finalDf["Date_First_Hired"].cast(DateType()))\
.withColumn("Termination_Date", finalDf["Termination_Date"].cast(DateType()))

# display(finalDf)

# COMMAND ----------

# DBTITLE 1,drop duplicates
finalDf = finalDf.dropDuplicates()

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
finalDf = finalDf.withColumn("Dated_On",lit(currentdatetime))

finalDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True")\
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/trusted/maker_checker_validation",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

