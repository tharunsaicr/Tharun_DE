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

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower,concat,row_number,expr
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

# COMMAND ----------

dailyAttendanceReportDf = spark.sql("select * from kgsonedatadb.trusted_hist_admin_daily_attendance_report where File_Date = "+ fileDate)

visitedWOBookingDf = spark.sql("select * from kgsonedatadb.trusted_hist_admin_visited_without_bookings where File_Date = "+ fileDate)

buDf = spark.sql("select Cost_centre, BU2 as BU from kgsonedatadb.config_admin_cc_bu_mapping")
buDf = buDf.dropDuplicates()

cardSwipeDf = spark.sql("select distinct PAYROLL_NUM,TRANSACTION_TIME from kgsonedatadb.trusted_hist_admin_card_swipe_master_data  where File_Date = "+ fileDate)

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

hrmsUnionDataDf.count()

# COMMAND ----------

dailyAttendanceReportDf.count()

# COMMAND ----------

visitedWOBookingDf.count()

# COMMAND ----------

cardSwipeDf.count()

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

cardSwipeDf = cardSwipeDf.withColumn('CardSwipe_Key',concat(cardSwipeDf.PAYROLL_NUM,cardSwipeDf.TRANSACTION_TIME))

# COMMAND ----------

dailyAttendanceReportDf.count()

# COMMAND ----------

# DBTITLE 1,Check Attendance data against Cardswipe
dailyAttendanceReportDf = dailyAttendanceReportDf.join(cardSwipeDf,dailyAttendanceReportDf.Attendance_Key == cardSwipeDf.CardSwipe_Key,'left')

# COMMAND ----------

dailyAttendanceReportDf.count()

# COMMAND ----------

# If concat of EMployee_Id, From_Date (Transaction report) is there in  concat of Payroll, Transaction_Time (CardSwipe) then update as 'Visited' else 'No-Show'

dailyAttendanceReportDf = dailyAttendanceReportDf.withColumn('STATUS2', when(col('CardSwipe_Key').isNotNull(),lit('VISITED'))\
    .when(col('CardSwipe_Key').isNull(),lit('NO SHOW'))\
    .otherwise(col('STATUS2')))

# COMMAND ----------

dailyAttendanceReportDf = dailyAttendanceReportDf.select('LOCATION','DEPARTMENT','EMPLOYEE_ID','EMPLOYEE_NAME','STATUS2','FROM_DATE','STATUS'
)

# COMMAND ----------

locationList = ['BANGALORE','GURUGRAM','KOCHI','MUMBAI','NOIDA','PUNE','KOLKATA']

visitedWOBookingDf = visitedWOBookingDf.filter(upper(col('LOCATION')).isin(locationList))

visitedWOBookingDf = visitedWOBookingDf\
    .withColumn('STATUS2', lit('Visited'))\
    .withColumn('STATUS', lit('W/o Booking'))

visitedWOBookingDf = visitedWOBookingDf.withColumnRenamed('VISITED_DATE','FROM_DATE')

visitedWOBookingDf = visitedWOBookingDf.select('LOCATION','DEPARTMENT','EMPLOYEE_ID','EMPLOYEE_NAME','STATUS2','FROM_DATE','STATUS'
)


# COMMAND ----------

currentDf = dailyAttendanceReportDf.union(visitedWOBookingDf)

# COMMAND ----------

# Union of Daily Attendance and Visited_w_o_Bookings
currentDf.count()

# COMMAND ----------

currentDf = currentDf.join(buDf, upper(currentDf.DEPARTMENT)== upper(buDf.Cost_centre), "left")

# COMMAND ----------

currentDf.count()

# COMMAND ----------

# Trying to get Department/BU from HRMS

currentDf = currentDf.join(hrmsUnionDataDf, (currentDf.EMPLOYEE_ID == hrmsUnionDataDf.HRMS_EMP_ID) & (currentDf.EMPLOYEE_NAME == hrmsUnionDataDf.HRMS_NAME),"left")

currentDf = currentDf.withColumn('DEPARTMENT',when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '-')) & (currentDf.HRMS_DEPARTMENT.isNotNull()) & (currentDf.HRMS_DEPARTMENT != '-'), col('HRMS_DEPARTMENT')).otherwise(col('DEPARTMENT')))

currentDf = currentDf.withColumn('BU',when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '-')) & (currentDf.HRMS_BU2.isNotNull()) & (currentDf.HRMS_BU2 != '-'), col('HRMS_BU2')).otherwise(col('BU')))

currentDf = currentDf.select('LOCATION','DEPARTMENT','EMPLOYEE_ID','EMPLOYEE_NAME','STATUS2','FROM_DATE','STATUS','Cost_centre','BU')

# COMMAND ----------

currentDf.count()

# COMMAND ----------

# DBTITLE 1,Remove data for Location if Mandatory Holiday - Part 1
# Check for holiday list if there is a Mandatory holiday for any location remove their data from  Booked and No Show
holidayDf = spark.sql("select Occasion, Date as Holiday_Date, Location as Holiday_Location,BU as Holiday_BU from kgsonedatadb.trusted_admin_locationwise_holidaylist")

holidayDf = holidayDf.withColumn('Holiday_Location', when(upper(col('Holiday_Location')) == 'BENGALURU', lit('Bangalore')).otherwise(col('Holiday_Location')))

# display(holidayDf)

currentDf = currentDf.join(holidayDf, (upper(currentDf.LOCATION)== upper(holidayDf.Holiday_Location)) & (currentDf.FROM_DATE==holidayDf.Holiday_Date), "left")  

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

currentDf = currentDf.select('LOCATION','DEPARTMENT','BU','EMPLOYEE_ID','EMPLOYEE_NAME','STATUS2','FROM_DATE','STATUS')

# COMMAND ----------

display(currentDf)

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