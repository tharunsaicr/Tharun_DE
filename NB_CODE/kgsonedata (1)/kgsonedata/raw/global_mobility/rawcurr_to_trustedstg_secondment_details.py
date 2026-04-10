# Databricks notebook source
#dbutils.widgets.removeAll ()
processName = "global_mobility"
tableName = "secondment_details"

# COMMAND ----------

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

#currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

# COMMAND ----------

from pyspark.sql.functions import lit, col

#Retrieving global mobility secondment_tracker data from delta table raw_curr_global_mobility_secondment_tracker

df_secondment_tracker = spark.sql("select Employee_No,Assignment_Start_Date,Assignment_End_Date,Home_Country,Host_Country,Travel_Model,Inbound___Outbound,Secondment_Status,Remarks,Personal_Email_ID,Host_Email_ID,India_Email_ID from kgsonedatadb.trusted_global_mobility_secondment_tracker")
df_secondment_tracker = df_secondment_tracker.withColumn("End_Date_for_FTS",lit("null"))

# display(df_secondment_tracker)

df_secondment_tracker =df_secondment_tracker.withColumnRenamed("Assignment_Start_Date","Start_Date").withColumnRenamed("Assignment_End_Date","End_Date").withColumnRenamed("Remarks","Comments_Mobility").withColumnRenamed("Travel_Model","Secondment_Type_Long_or_Short").withColumnRenamed("Secondment_Status","Active_or_Concluded")

#Retrieving Headcount Employee_dump data from delta table trusted_stg_headcount_employee_dump 
df_emp_dump = spark.sql("select * from kgsonedatadb.trusted_headcount_employee_dump where Employee_Category in ('Secondee-Outward-With Pay','Secondee-Inward-With Pay','Secondee-Inward-Without Pay','Secondee-Outward-Without Pay')")

# display(df_emp_dump)

df_emp_dump =df_emp_dump.withColumnRenamed("Job_Name","Job").withColumnRenamed("People_Group_Name","People_Group").withColumnRenamed("Company_Name","Company").withColumnRenamed("Email_Address","Email").withColumnRenamed("End_Date","End_Date1")

#joining secondment_tracker and Employee_dump the data
# df_join = df_secondment_tracker.join(df_emp_dump, df_secondment_tracker.Employee_No== df_emp_dump.Employee_Number, 'left')
df_join = df_emp_dump.join(df_secondment_tracker,  df_emp_dump.Employee_Number == df_secondment_tracker.Employee_No, 'left')


display(df_join)

# COMMAND ----------

# df = df_join.select("Employee_No","Full_Name", "Function", "Employee_Subfunction","Employee_Subfunction_1","Organization_Name","Cost_centre","Business_Category","Operating_Unit","User_Type","Client_Geography","Location","Sub_Location","Position","Job","People_Group","Employee_Category","Date_First_Hired","End_Date_for_FTS","Gender","Company","Supervisor_Name","Performance_Manager","email","Start_Date","End_Date","Home_Country","Host_Country","Secondment_Type_Long_or_Short","Inbound___Outbound","Active_or_concluded","Comments_Mobility")

# list = ["Secondee-Outward-With Pay","Secondee-Inward-With Pay","Secondee-Inward-Without Pay","Secondee-Outward-Without Pay"]

# df = df.where( ( col("Employee_Category").isin (list)))

#df = df.filter(col("Employee_Category").contains("Secondee-Outward-With Pay","Secondee-Inward-With Pay","Secondee-Inward-Without Pay","Secondee-Outward-Without Pay"))


# Added 3 more columns "India_Email_ID","Personal_Email_ID","Host_Email_ID" on request from Ritish and Team

df = df_join.select("Employee_Number","Full_Name", "Function", "Employee_Subfunction","Employee_Subfunction_1","Organization_Name","Cost_centre","Business_Category","Operating_Unit","User_Type","Client_Geography","Location","Sub_Location","Position","Job","People_Group","Employee_Category","Date_First_Hired","End_Date_for_FTS","Gender","Company","Supervisor_Name","Performance_Manager","Email","Personal_Email_ID","Host_Email_ID","India_Email_ID","Start_Date","End_Date","Home_Country","Host_Country","Secondment_Type_Long_or_Short","Inbound___Outbound","Active_or_concluded","Comments_Mobility")
df =df.withColumnRenamed("Employee_No","Employee_Number")

display(df)


# COMMAND ----------

df=df.withColumn("File_Date", lit(FileDate))

# COMMAND ----------

# DBTITLE 1, Trusted stg
df.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})