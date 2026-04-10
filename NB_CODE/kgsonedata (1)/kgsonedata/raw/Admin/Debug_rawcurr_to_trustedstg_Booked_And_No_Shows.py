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

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower,concat,row_number,expr,regexp_replace
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse
from pyspark.sql.window import Window

# COMMAND ----------

# Booked and No Show
# We need to check for Daily Attendance Report,Visited w_o Bookings as well
# 1) Remove duplicates from Daily Attendance Report_WE271023 based on EMP ID and Date.
# 2) Get all the data after clean up from  Daily Attendance Report and set Status ='With Booking' if present in Daily Attendance 
# 3) Get data from Visited w_o Bookings put filter for locations('Bangalore','Gurugram','Kochi','Mumbai','Noida','Pune') append to the existing data and set Status ='W/o Booking' 
# 4) Based on Department join with Mapping file and get 'BU'
# Check for holiday list if there is a Mandatory holiday for any location remove their data from  Booked and No Show
# If Department is missing, get that from Admin HRMS/Monthly Headcount/CardSwipe Report and then find corresponding BU

# COMMAND ----------

# dailyAttendanceReportDf = spark.sql("select * from kgsonedatadb.trusted_hist_admin_daily_attendance_report where File_Date = "+ fileDate)
dailyAttendanceReportDf = spark.sql("select * from kgsonedatadb.trusted_hist_admin_daily_attendance_report where Employee_Id in ('110868','101526','33006') and File_Date = "+ fileDate)

# visitedWOBookingDf = spark.sql("select * from kgsonedatadb.trusted_hist_admin_visited_without_bookings where File_Date = "+ fileDate)

visitedWOBookingDf = spark.sql("select * from kgsonedatadb.trusted_hist_admin_visited_without_bookings where Employee_Id in ('110868','101526','33006') and File_Date = "+ fileDate)


buDf = spark.sql("select Cost_centre, BU2 as BU from kgsonedatadb.config_admin_cc_bu_mapping")
buDf = buDf.dropDuplicates()
buDf = Append_String_to_ColumnName(buDf,'Mapping')

cardSwipeDf = spark.sql("select distinct PAYROLL_NUM,TRANSACTION_TIME,DEPARTMENT from kgsonedatadb.trusted_hist_admin_card_swipe_master_data  where File_Date = "+ fileDate)
cardSwipeDf = cardSwipeDf.dropDuplicates()
cardSwipeDf = Append_String_to_ColumnName(cardSwipeDf,'CardSwipe')

# COMMAND ----------

display(dailyAttendanceReportDf.filter(col('Location') =='Bangalore'))

# COMMAND ----------

display(dailyAttendanceReportDf.select('Location').distinct())

# COMMAND ----------

display(visitedWOBookingDf.select('Location').distinct())

# COMMAND ----------

# DBTITLE 1,Drop Duplicates in CardSwipe
window_spec = Window.partitionBy("CardSwipe_PAYROLL_NUM","CardSwipe_TRANSACTION_TIME").orderBy(col("CardSwipe_DEPARTMENT").desc())

cardSwipeDf =cardSwipeDf.withColumn("CardSwipe_Row_Number", row_number().over(window_spec))

cardSwipeDf = cardSwipeDf.filter(col('CardSwipe_Row_Number') == 1)

# COMMAND ----------

# Check for duplicates
cardSwipeDf \
    .groupby(['CardSwipe_PAYROLL_NUM']) \
    .count() \
    .where('count > 1') \
    .sort('count', ascending=False) \
    .show()

# COMMAND ----------

# DBTITLE 1,Admin HRMS Data
employeeDataDf = spark.sql("select EMP_ID,NAME,EMAIL_ID,DEPARTMENT,BU2 from kgsonedatadb.trusted_hist_admin_employee_data where File_Date = "+ fileDate)
employeeDataDf = employeeDataDf.withColumn("Source",lit('Employee_Data'))

loanedDataDf = spark.sql("select EMP_ID,NAME,EMAIL_ID,DEPARTMENT,BU2 from kgsonedatadb.trusted_hist_admin_loaned_data where File_Date = "+ fileDate)
loanedDataDf = loanedDataDf.withColumn("Source",lit('Loaned_Data'))

cwkDataDf = spark.sql("select EMP_ID,NAME,EMAIL_ID,DEPARTMENT,BU2 from kgsonedatadb.trusted_hist_admin_cwk_data where File_Date = "+ fileDate)
cwkDataDf = cwkDataDf.withColumn("Source",lit('CWK_Data'))

# COMMAND ----------

# DBTITLE 1,Remove Duplicates from HRMS
hrmsUnionDataDf = employeeDataDf.union(loanedDataDf).union(cwkDataDf)

# If duplicates are there then give priority to Employee_Data then Loaned_Data then CWK_Data
custom_order = {"Employee_Data":1,"Loaned_Data":2,"CWK_Data":3}

window_spec = Window.partitionBy("EMP_ID","Name").orderBy(\
    when(hrmsUnionDataDf["Source"] =="Employee_Data",custom_order["Employee_Data"])\
    .when(hrmsUnionDataDf["Source"] =="Loaned_Data",custom_order["Loaned_Data"])\
    .when(hrmsUnionDataDf["Source"] =="CWK_Data",custom_order["CWK_Data"])\
    .otherwise(float('inf')))

hrmsUnionDataDf =hrmsUnionDataDf.withColumn("Row_Number", row_number().over(window_spec))

hrmsUnionDataDf = hrmsUnionDataDf.filter(col('Row_Number') == 1)

# Changing column name to HRMS_<ColumnName>
hrmsUnionDataDf = Append_String_to_ColumnName(hrmsUnionDataDf,'HRMS')

# COMMAND ----------

# DBTITLE 1,Monthly Headcount Report
# Sabbatical and Maternity are already included in Employee_Details
monthlyEmployeeDf = spark.sql("select Employee_Number,Full_Name,Cost_centre,'Employee_details' as Source from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where to_date(File_Date,'yyyyMMdd') <= to_date('"+fileDate+"','yyyyMMdd'))")

monthlyResignedDf = spark.sql("select Employee_Number,Full_Name,Cost_centre,'Resigned_and_Left' as Source from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where to_date(File_Date,'yyyyMMdd') <= to_date('"+fileDate+"','yyyyMMdd'))")

monthlyContingentWorker = spark.sql("select Candidate_Id as Employee_Number,Full_Name,Cost_centre,'Contingent_Worker' as Source  from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker where to_date(File_Date,'yyyyMMdd') <= to_date('"+fileDate+"','yyyyMMdd'))")
monthlyContingentWorker = monthlyContingentWorker.withColumn("Employee_Number",regexp_replace(col("Employee_Number"), "[^a-zA-Z0-9]", ""))

monthlyAcademicTrainee = spark.sql("select Candidate_Id as Employee_Number,Full_Name,Cost_centre,'Academin_Trainee' as Source from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee where to_date(File_Date,'yyyyMMdd') <= to_date('"+fileDate+"','yyyyMMdd'))")
monthlyAcademicTrainee = monthlyAcademicTrainee.withColumn("Employee_Number",regexp_replace(col("Employee_Number"), "[^a-zA-Z0-9]", ""))

monthlyLoanedDF = spark.sql("select Employee_Number,Employee_Name as Full_Name,Cost_centre,'Loned_Staff_from_KI' as Source  from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki where to_date(File_Date,'yyyyMMdd') <= to_date('"+fileDate+"','yyyyMMdd'))")

monthlyContingentWorkerResignedDf = spark.sql("select Candidate_Id as Employee_Number,Full_Name,Cost_centre,'Contingent_Worker_Resigned' as Source  from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned where to_date(File_Date,'yyyyMMdd') <= to_date('"+fileDate+"','yyyyMMdd'))")
monthlyContingentWorkerResignedDf = monthlyContingentWorkerResignedDf.withColumn("Employee_Number",regexp_replace(col("Employee_Number"), "[^a-zA-Z0-9]", ""))

monthlyLoanedResignedDf = spark.sql("select Employee_Number,Employee_Name as Full_Name,Cost_Center as Cost_centre,'Loned_Staff_Resigned' as BU  from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned where to_date(File_Date,'yyyyMMdd') <= to_date('"+fileDate+"','yyyyMMdd'))")

monthlySecondeeOutwardDf = spark.sql("select Employee_Number,Full_Name,Cost_centre,'Secondee_Outward' as Source  from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward where to_date(File_Date,'yyyyMMdd') <= to_date('"+fileDate+"','yyyyMMdd'))")

# COMMAND ----------

# DBTITLE 1,FAcility - Noida and Building Gurugram --> location to be updated as Noida
# In Daily Attendance Report received from the system please update the location to Gurugram, where Facility is NOAN and Location is Noida.

dailyAttendanceReportDf = dailyAttendanceReportDf.withColumn('LOCATION',when((upper(col('LOCATION')) == 'GURUGRAM') & (upper(col('FACILITY')) == 'NOIDA'), lit('Noida'))\
.otherwise(col('Location')))

# COMMAND ----------

dailyAttendanceReportDf = dailyAttendanceReportDf.withColumnRenamed('STATUS','STATUS2')

# Remove duplicates based on Employee Id and From Date
dailyAttendanceReportDf = dailyAttendanceReportDf.dropDuplicates(['EMPLOYEE_ID','FROM_DATE'])

dailyAttendanceReportDf = dailyAttendanceReportDf.withColumn('STATUS', lit('With Booking'))

dailyAttendanceReportDf = dailyAttendanceReportDf.select('LOCATION','DEPARTMENT','EMPLOYEE_ID','EMPLOYEE_NAME','STATUS2','FROM_DATE','STATUS'
)


# COMMAND ----------

dailyAttendanceReportDf = dailyAttendanceReportDf.withColumn('Attendance_Key',concat(dailyAttendanceReportDf.EMPLOYEE_ID,dailyAttendanceReportDf.FROM_DATE))

cardSwipeDf = cardSwipeDf.withColumn('CardSwipe_Key',concat(cardSwipeDf.CardSwipe_PAYROLL_NUM,cardSwipeDf.CardSwipe_TRANSACTION_TIME))

# COMMAND ----------

# DBTITLE 1,Check Daily Attendance data against Cardswipe
dailyAttendanceReportDf = dailyAttendanceReportDf.join(cardSwipeDf,dailyAttendanceReportDf.Attendance_Key == cardSwipeDf.CardSwipe_Key,'left').select(dailyAttendanceReportDf['*'],cardSwipeDf['CardSwipe_Key'])

# COMMAND ----------

# We have renamed STATUS to STATUS2 in above step
dailyAttendanceReportDf = dailyAttendanceReportDf.withColumn('STATUS2',\
    when((col('CardSwipe_Key').isNotNull()) & (col('STATUS2') == 'VISITED'),lit('VISITED'))\
    .when((col('CardSwipe_Key').isNotNull()) & (col('STATUS2') == 'NO SHOW'),lit('VISITED'))\
    .when((col('CardSwipe_Key').isNull()) & (col('STATUS2') == 'VISITED'),lit('VISITED'))\
    .when((col('CardSwipe_Key').isNull()) & (col('STATUS2') == 'NO SHOW'),lit('No SHOW'))\
    .otherwise(col('STATUS2')))

# COMMAND ----------

dailyAttendanceReportDf = dailyAttendanceReportDf.select('LOCATION','DEPARTMENT','EMPLOYEE_ID','EMPLOYEE_NAME','STATUS2','FROM_DATE','STATUS'
)

# COMMAND ----------

locationList = ['BANGALORE','GURUGRAM','KOCHI','MUMBAI','NOIDA','PUNE','KOLKATA','HYDERABAD']

visitedWOBookingDf = visitedWOBookingDf.filter(upper(col('LOCATION')).isin(locationList))

visitedWOBookingDf = visitedWOBookingDf\
    .withColumn('STATUS2', lit('Visited'))\
    .withColumn('STATUS', lit('W/o Booking'))

visitedWOBookingDf = visitedWOBookingDf.withColumnRenamed('VISITED_DATE','FROM_DATE')

visitedWOBookingDf = visitedWOBookingDf.select('LOCATION','DEPARTMENT','EMPLOYEE_ID','EMPLOYEE_NAME','STATUS2','FROM_DATE','STATUS'
)


# COMMAND ----------

# DBTITLE 1,Combine Attendance and Visited_w_o_Booking
currentDf = dailyAttendanceReportDf.union(visitedWOBookingDf)

# COMMAND ----------

currentDf = currentDf.withColumn('Original_DEPARTMENT',col('DEPARTMENT'))
currentDf = currentDf.withColumn('DEPARTMENT',lit(None))
currentDf = currentDf.withColumn('BU',lit(None))

# COMMAND ----------

# DBTITLE 1,Get Deparment from Admin HRMS
# Trying to get Department/BU from Admin HRMS

currentDf = currentDf.join(hrmsUnionDataDf, (currentDf.EMPLOYEE_ID == hrmsUnionDataDf.HRMS_EMP_ID) & (currentDf.EMPLOYEE_NAME == hrmsUnionDataDf.HRMS_NAME),"left")

# COMMAND ----------

display(currentDf)

# COMMAND ----------

currentDf = currentDf.withColumn('DEPARTMENT',when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '-') | (trim(currentDf.DEPARTMENT) == '')) & (currentDf.HRMS_DEPARTMENT.isNotNull()) & (currentDf.HRMS_DEPARTMENT != '-'), col('HRMS_DEPARTMENT')).otherwise(col('DEPARTMENT')))

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Check in Headcount Monthly report for Department(if any left)
unionHeadcountMonthlyDf = monthlyEmployeeDf.union(monthlyResignedDf).union(monthlyContingentWorker).union(monthlyAcademicTrainee).union(monthlyLoanedDF).union(monthlyContingentWorkerResignedDf).union(monthlyLoanedResignedDf).union(monthlySecondeeOutwardDf)

# Changing column name to HRMS_<ColumnName>
unionHeadcountMonthlyDf = Append_String_to_ColumnName(unionHeadcountMonthlyDf,'Headcount')

unionHeadcountMonthlyDf = unionHeadcountMonthlyDf.dropDuplicates(["Headcount_Employee_Number","Headcount_Full_Name"])

# COMMAND ----------

# display(unionHeadcountMonthlyDf.filter(col('Headcount_Employee_Number').isin('145407')))

# COMMAND ----------

# Get Deparment from Headcount Report
currentDf = currentDf.join(unionHeadcountMonthlyDf, (currentDf.EMPLOYEE_ID == unionHeadcountMonthlyDf.Headcount_Employee_Number) & (currentDf.EMPLOYEE_NAME == unionHeadcountMonthlyDf.Headcount_Full_Name),"left").select(currentDf['*'],unionHeadcountMonthlyDf['Headcount_Cost_centre'])

# COMMAND ----------

display(currentDf)

# COMMAND ----------

currentDf = currentDf.withColumn('DEPARTMENT', \
    when((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '-') | (trim(currentDf.DEPARTMENT) == ''), col('Headcount_Cost_centre'))\
    .otherwise(col('DEPARTMENT')))

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Get Department from CardSwipe Report(if missed)
currentDf = currentDf.join(cardSwipeDf,currentDf.EMPLOYEE_ID == cardSwipeDf.CardSwipe_PAYROLL_NUM,'left').select(currentDf['*'],cardSwipeDf['CardSwipe_DEPARTMENT'])

# COMMAND ----------

currentDf = currentDf.withColumn('DEPARTMENT', \
    when((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '-') | (trim(currentDf.DEPARTMENT) == ''), col('CardSwipe_DEPARTMENT'))\
    .otherwise(col('DEPARTMENT')))

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,If not still Department is null get from Original input file 
currentDf = currentDf.withColumn('DEPARTMENT', \
    when((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '-') | (trim(currentDf.DEPARTMENT) == ''), col('Original_DEPARTMENT'))\
    .otherwise(col('DEPARTMENT')))

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Get BU from Mapping File
currentDf = currentDf.join(buDf, upper(currentDf.DEPARTMENT)== upper(buDf.Mapping_Cost_centre), "left").select(currentDf['*'],buDf['Mapping_BU'])

# COMMAND ----------

currentDf = currentDf.withColumn("BU", col('Mapping_BU'))

# COMMAND ----------

display(currentDf.filter(col('Location') =='Bangalore'))

# COMMAND ----------

# DBTITLE 1,Remove data for Location if Mandatory Holiday - Part 1
# Check for holiday list if there is a Mandatory holiday for any location remove their data from  Booked and No Show
holidayDf = spark.sql("select Occasion, Date as Holiday_Date, Location as Holiday_Location,BU as Holiday_BU from kgsonedatadb.trusted_admin_locationwise_holidaylist")

holidayDf = holidayDf.withColumn('Holiday_Location', when(upper(col('Holiday_Location')) == 'BENGALURU', lit('Bangalore')).otherwise(col('Holiday_Location')))

# display(holidayDf)

currentDf = currentDf.join(holidayDf, (upper(currentDf.LOCATION)== upper(holidayDf.Holiday_Location)) & (currentDf.FROM_DATE==holidayDf.Holiday_Date), "left")  

# COMMAND ----------

display(holidayDf.filter(col('Holiday_Date') == '2024-04-11'))

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Remove data for Location if Mandatory Holiday - Part 2
from pyspark.sql.functions import when, lit

currentDf = currentDf.withColumn(
"FILTER_SELECTOR",
when(currentDf.Holiday_BU == 'All', lit('yes'))
.otherwise(
    when((currentDf.Holiday_BU == 'GDC') & (currentDf.BU == 'Audit-GDC'), lit('yes'))
    .otherwise(
        when((currentDf.Holiday_BU == 'KGS & KRC') & (currentDf.BU != 'Audit-GDC'), lit('yes'))
        .otherwise(lit('No'))
    )
)
)

currentDf = currentDf.filter(col('FILTER_SELECTOR')=='No')

# COMMAND ----------

currentDf = currentDf.select('LOCATION','DEPARTMENT','BU','EMPLOYEE_ID','EMPLOYEE_NAME','STATUS2','FROM_DATE','STATUS','HRMS_DEPARTMENT','Headcount_Cost_centre','CardSwipe_DEPARTMENT','Original_DEPARTMENT')

# COMMAND ----------

display(currentDf.filter(col('Location') =='Bangalore'))

# COMMAND ----------

display(currentDf.select('Location','BU').distinct())

# COMMAND ----------

currentDf.filter(col('BU').isNull()).display()

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata'))
currentDf = currentDf.withColumn("Dated_On",lit(currentdatetime))
currentDf = currentDf.withColumn("FILE_DATE",lit(fileDate))


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

# COMMAND ----------

