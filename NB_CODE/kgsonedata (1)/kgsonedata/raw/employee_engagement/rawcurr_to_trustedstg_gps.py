# Databricks notebook source
processName = "employee_engagement"
tableName = "gps"

# COMMAND ----------

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

fileYear = FileDate[:4]
fileMonth = FileDate[4:6]

monthList = ["10","11","12"]

if fileMonth in monthList:
    # rating_column = "RATING_OCT_"+(int(fileYear)-1)+"_TO_SEP_"+fileYear+"_"
    FY = "FY"+str(int(fileYear)+1)[2:4]
else:
    # rating_column = "RATING_OCT_"+str(int(fileYear)-2)+"_TO_SEP_"+str(int(fileYear)-1)+"_"
    FY = "FY"+fileYear[2:4]

print(FY)

# COMMAND ----------

import datetime

fileDay = FileDate[6:]
convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay
print(convertedFileDate)

convertedFileDate = datetime.datetime.strptime(convertedFileDate, '%Y-%m-%d').date()


# COMMAND ----------

from datetime import date
from datetime import datetime

fileDateTimestamp = datetime.combine(convertedFileDate, datetime.min.time())

print(fileDateTimestamp)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import lit, col, split, floor, when,round
from pyspark.sql import functions as F

def current_local_date():
    return F.from_utc_timestamp(F.current_timestamp(), 'Europe/Riga').cast('date')

#Retrieving Compensation YEC data from delta table raw_curr_compensation_yec
# df_comp_yec = spark.read.sql("select * from kgsonedatadb.trusted_compensation_yec")

df_comp_yec = spark.sql("select * from (select rank() over(partition by EMPLOYEE_NUMBER,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_compensation_yec where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_compensation_yec where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) yec_hist where rank = 1")

df_comp_yec = df_comp_yec.withColumnRenamed("Employee_Number","YEC_Employee_Number")

#Retrieving Headcount Employee_dump data from delta table trusted_stg_headcount_employee_dump 
# df_emp_dump = spark.sql("select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'")

df_emp_dump = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) ed_hist where rank = 1")

df_emp_dump = df_emp_dump.withColumnRenamed("FULL_NAME","Full_Name1").withColumnRenamed("EMP_CATEGORY","Emp_Category")

df_emp_dump = df_emp_dump.withColumn("Gratuity_Date",changeDateFormat(col("Gratuity_Date")))

#Join CC-BU mapping to get BU
df_config_cc_bu = spark.sql("select distinct Cost_centre,BU from kgsonedatadb.config_cost_center_business_unit")


df_emp_dump = df_emp_dump.join(df_config_cc_bu,df_emp_dump.Cost_centre == df_config_cc_bu.Cost_centre,"left").select(df_emp_dump["*"],df_config_cc_bu["BU"])

list1 = [1,2]

#changed on 4/14/2023 - to add select statement which was missed before
df_join_emp = df_emp_dump.join(df_comp_yec, df_comp_yec.YEC_Employee_Number ==  df_emp_dump.Employee_Number, 'left').select(df_emp_dump["*"],df_comp_yec["*"])

df_ratings = spark.sql("select emp_rating.Employee_Number as Rating_Employee_Number, emp_rating.FY as Rating_FY, emp_rating.Rating as Emp_Rating from (select rank() over(partition by Employee_Number,FY order by File_Date desc, Dated_On desc) as Rank_Rating,* from kgsonedatadb.trusted_hist_compensation_employee_ratings where Employee_Number is not null and FY = '"+FY+"') emp_rating where emp_rating.Rank_Rating = 1")

df_join_emp = df_join_emp.join(df_ratings, df_join_emp.Employee_Number==df_ratings.Rating_Employee_Number,'left').select(df_join_emp['*'],df_ratings["Emp_Rating"])


df_add_col = df_join_emp\
.withColumn("Global_Job_Level", col("Job_Name"))\
.withColumn("Global_Function", col("Company_Name"))\
.withColumn('Age', (F.months_between(lit(convertedFileDate), F.col('Date_of_Birth')) / 12).cast('int'))\
.withColumn("Full_time_or_Part_time", col("Employee_Category"))\
.withColumn("Gender", col("Gender"))\
.withColumn("Tenure_Length_of_Service", round((F.datediff(lit(convertedFileDate), F.col('Gratuity_Date')) / 365),2))\
.withColumn("Is_Client_Facing", col("People_Group_Name"))\
.withColumn("Cost_Centre", col("Cost_centre"))\
.withColumn("DOJ", col("Date_First_Hired").cast(StringType()))\
.withColumn("Is_a_High_Performer",when(col("Emp_Rating").isin(list1), lit("Yes") ).otherwise(lit("No")))\
.withColumn("Recently_Promoted_last_24_months", lit("null"))


df_add_col = df_add_col.distinct()


# COMMAND ----------

from pyspark.sql.functions import lit, col, split, floor, when

df_add_col = df_add_col.withColumn("Employment_Type", when(df_add_col.Person_Type_Flag == "EMPLOYEE","Permanent")
                                 .when(df_add_col.Person_Type_Flag == "CONTRACTUAL EMPLOYEE","Contractor")
                                 .otherwise(df_add_col.Person_Type_Flag))

# COMMAND ----------

# DBTITLE 1,Is_a_Performance_Manager and Is_a_People_Leader
performanceMangerList = df_add_col.select("PM_Employee_Number").distinct().rdd.flatMap(lambda x: x).collect()

pmlList = df_add_col.select("PML_Number").distinct().rdd.flatMap(lambda x: x).collect()

df_add_col = df_add_col.withColumn("Is_a_Performance_Manager",when(df_add_col.Employee_Number.isin(performanceMangerList), lit("Yes") ).otherwise(lit("No")))

df_add_col = df_add_col.withColumn("Is_a_People_Leader",when(df_add_col.Employee_Number.isin(pmlList), lit("Yes") ).otherwise(lit("No")))

# COMMAND ----------

# DBTITLE 1,Has_Taken_Parental_Leave_in_the_Past_3_Years
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as F
# df_leave=spark.sql("select Employee_Number, Leave_Start_Date, Leave_Type from kgsonedatadb.trusted_headcount_leave_report")

# df_leave = spark.sql("select Employee_Number, Leave_Start_Date, Leave_Type from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_leave_report where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) ed_hist where rank = 1")

df_leave=spark.sql("select * from (select rank() over(partition by employee_number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_leave_report where upper(approval_status) = 'APPROVED' and upper(cancel_status) is null and to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+") hist_leave_report where rank = 1")

df_add_col=df_add_col.join(df_leave,df_add_col.Employee_Number==df_leave.Employee_Number,'left').select(df_add_col["*"],df_leave["Leave_Start_Date"],df_leave["Leave_Type"])



df=df_add_col.withColumn("curr_timestamp",lit(fileDateTimestamp)).withColumn("curr_year",date_format(col('curr_timestamp'),'y'))\
.withColumn("leave_year",date_format(col('Leave_Start_Date'),'y'))\
.withColumn('prev_date',F.date_add(F.date_trunc('yyyy', 'curr_year'), -1))\
.withColumn("prev_year",date_format(col('prev_date'),'y'))\
.withColumn('prevv_date',F.date_add(F.date_trunc('yyyy', 'prev_year'), -1))\
.withColumn("prevv_year",date_format(col('prevv_date'),'y'))


prevList1 = df.select("curr_year","prev_year","prevv_year").distinct().rdd.flatMap(lambda x: x).collect()

list = ["KGS Paternity Leave","KGS Maternity Leave","KGS Maternity Miscarriage Leave","KI Maternity Leave"]

df = df.withColumn('Has_Taken_Parental_Leave_in_the_Past_3_Years',
    F.when(df.leave_year.isin(prevList1)  & df.Leave_Type.isin(list), df.Leave_Start_Date).otherwise(lit("NA")))

df = df.drop('curr_timestamp','curr_year','prev_year','prevv_year')

# COMMAND ----------

# display(df.select("Employee_Number","Full_Name1", "Email_Address", "Global_Job_Level", "Global_Function", "Employment_Type", "Age", "Full_time_or_Part_time", "Gender","DOJ", "Gratuity_Date","Tenure_Length_of_Service", "Is_a_High_Performer",  "Is_a_Performance_Manager", "Is_a_People_Leader","Is_Client_Facing","Has_Taken_Parental_Leave_in_the_Past_3_Years","Recently_Promoted_last_24_months","Cost_Centre","BU").distinct())

# COMMAND ----------

currentDf= df.select("Employee_Number","Full_Name1", "Email_Address", "Global_Job_Level", "Global_Function", "Employment_Type", "Age", "Full_time_or_Part_time", "Gender","DOJ","Gratuity_Date" ,"Tenure_Length_of_Service", "Is_a_High_Performer",  "Is_a_Performance_Manager", "Is_a_People_Leader","Is_Client_Facing","Has_Taken_Parental_Leave_in_the_Past_3_Years","Recently_Promoted_last_24_months","Cost_Centre","BU").distinct()

currentDf = currentDf.withColumnRenamed("Full_Name1","Full_Name")
currentDf = currentDf.dropDuplicates()
	

# COMMAND ----------

currentDf=currentDf.withColumn("File_Date", lit(FileDate))

# COMMAND ----------

display(currentDf)

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