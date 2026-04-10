# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace,lower
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse

# COMMAND ----------

# DBTITLE 1,Load Data from Parent tables
if (tableName == 'card_swipe_master_data'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_stg_admin_transaction_report")

if (tableName == 'xport_planned_vs_actuals_pune'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_transport_data_pune")
if (tableName == 'xport_planned_vs_actuals_bangalore'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_transport_data_bangalore")
if (tableName == 'xport_planned_vs_actuals_gurgaon'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_transport_data_gurgaon")
if (tableName == 'xport_planned_vs_actuals_kochi'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_transport_data_kochi")
if (tableName == 'xport_planned_vs_actuals_kolkata'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_transport_data_kolkata")
if (tableName == 'xport_planned_vs_actuals_hyderabad'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_transport_data_hyderabad")

if (tableName == 'xport_no_shows_pune'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_no_shows_pune")
if (tableName == 'xport_no_shows_bangalore'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_no_shows_bangalore")
if (tableName == 'xport_no_shows_kochi'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_no_shows_kochi")
if (tableName == 'xport_no_shows_kolkata'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_no_shows_kolkata")
if (tableName == 'xport_no_shows_gurgaon'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_no_shows_gurgaon")
if (tableName == 'xport_no_shows_hyderabad'):
    currentDf = spark.sql("select * from kgsonedatadb.trusted_admin_no_shows_hyderabad")  

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# DBTITLE 1,Data Cleanup

import pyspark.sql.functions as f
from pyspark.sql.functions import col

# display(currentDf)
if (tableName == 'card_swipe_master_data'):
    # print(tableName)

    #Cast transaction time to date and convert Job_title, Department to not null columns
    currentDf = currentDf.withColumn("TRANSACTION_TIME", currentDf.TRANSACTION_TIME.cast("Date"))
    currentDf = currentDf.withColumn("JOB_TITLE", when(currentDf.JOB_TITLE.isNull(), lit('')).otherwise(currentDf.JOB_TITLE))
    currentDf = currentDf.withColumn("DEPARTMENT", when(currentDf.DEPARTMENT.isNull(), lit('')).otherwise(currentDf.DEPARTMENT))
    

# COMMAND ----------

from pyspark.sql.functions import col

if (tableName == 'card_swipe_master_data'):
    currentDf = currentDf.filter(

        ~((upper(trim(currentDf.OUTCOME)) == 'Not in system') &
        (currentDf.NAME___REG_NUM.isNull() | (upper(trim(currentDf.NAME___REG_NUM)) == '')) &
        (trim(currentDf.PAYROLL_NUM).isNull() | (upper(trim(currentDf.PAYROLL_NUM)) == '')) &
        (trim(currentDf.JOB_TITLE).isNull() | (upper(trim(currentDf.JOB_TITLE)) == '')) &
        (trim(currentDf.DEPARTMENT).isNull() | (upper(trim(currentDf.DEPARTMENT)) == '')))

        &

        ~(
        (upper(trim(currentDf.DEPARTMENT)) == 'SECURITY') |
        (upper(trim(currentDf.DEPARTMENT)) == 'HOUSEKEEPING') |
        (upper(trim(currentDf.DEPARTMENT)).rlike('.*DVS.*$')) |
        (upper(trim(currentDf.DEPARTMENT)).rlike('.*MAIL.*$')) |
        (upper(trim(currentDf.DEPARTMENT)) == 'MAINTENANCE') |
        ((upper(trim(currentDf.DEPARTMENT)).rlike('.*ADMIN.*$')) & (upper(trim(currentDf.DEPARTMENT)).rlike('.*OTHER.*$'))) |
        ((upper(trim(currentDf.DEPARTMENT)).rlike('.*TRANSPORT.*$')) & (upper(trim(currentDf.DEPARTMENT)).rlike('.*OTHER.*$'))) |
        ((upper(trim(currentDf.DEPARTMENT)).rlike('.*IT.*$')) & (upper(trim(currentDf.DEPARTMENT)).rlike('.*OTHER.*$'))) |
        (upper(trim(currentDf.DEPARTMENT)) == 'CPT') |
        (upper(trim(currentDf.DEPARTMENT)) == 'FINANCE') |
        (upper(trim(currentDf.DEPARTMENT)) == 'TEMP') |
        (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*EMERGENCY CARD.*$')) |
        (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TEMPORARY ACCESS.*$')) |
        (upper(trim(currentDf.JOB_TITLE)) == 'SMART Q') 
        )


    )

    display(currentDf)

# COMMAND ----------

from pyspark.sql.functions import col

if (tableName == 'card_swipe_master_data'):
    currentDf = currentDf.filter(
        (
        trim(currentDf.PAYROLL_NUM).rlike("^[0-9]*$") |

        col("PAYROLL_NUM").startswith("INB") | 

        upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TAX.*$') |
        (trim(currentDf.PAYROLL_NUM).rlike('.*MS.*$') & (~trim(currentDf.PAYROLL_NUM).rlike('.*BMS.*$'))) |
        trim(currentDf.PAYROLL_NUM).rlike('.*MC.*$') |
        trim(currentDf.PAYROLL_NUM).rlike('.*GDC.*$') |
        trim(currentDf.PAYROLL_NUM).rlike('.*KRC.*$') |
        trim(currentDf.PAYROLL_NUM).rlike('.*CF-HR.*$') |
        trim(currentDf.PAYROLL_NUM).rlike('.*DA.*$') |
        # trim(currentDf.PAYROLL_NUM).rlike('.*DAS.*$') |
        trim(currentDf.PAYROLL_NUM).rlike('.*RC.*$') |
        (trim(currentDf.PAYROLL_NUM).rlike('.*IT.*$') & (~trim(currentDf.PAYROLL_NUM).rlike('.*SECURITY.*$')) & (~trim(currentDf.PAYROLL_NUM).rlike('.*EXIT.*$')) & (~trim(currentDf.PAYROLL_NUM).rlike('.*/BOY.*$')) & (~trim(currentDf.PAYROLL_NUM).rlike('.*FACILITY.*$')) & (~trim(currentDf.PAYROLL_NUM).rlike('.*PEREGRINE SECURITY.*$'))) |
        # (~(trim(currentDf.PAYROLL_NUM).rlike('.*SEC.*$') & (~trim(currentDf.PAYROLL_NUM).rlike('.*SECONDEE.*$'))) )|
        # trim(currentDf.PAYROLL_NUM).rlike('.*ADMIN.*$') |
        ((trim(currentDf.PAYROLL_NUM).rlike('.*ADM.*$')) & (~(upper(trim(currentDf.PAYROLL_NUM)).rlike('.*ADM-.*$') & (trim(currentDf.DEPARTMENT) == 'ADMIN'))) & (~(upper(trim(currentDf.COMPANY)).rlike('.*CALIBER.*$') )) ) | 
        ((trim(currentDf.PAYROLL_NUM).rlike('.*TEMP.*$')) & (upper(trim(currentDf.E_MAIL_ID)).rlike('.*@KPMG.COM.*$')) ) 
        ) 
        |
        #PAYROLL NUM = SECURITY AND CONTAINS ANY BU NAME
        (
            (trim(currentDf.PAYROLL_NUM).rlike('.*SECURITY.*$')) &
            (
                upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TAX.*$') |
                trim(currentDf.PAYROLL_NUM).rlike('.*MS.*$') |
                trim(currentDf.PAYROLL_NUM).rlike('.*MC.*$') |
                trim(currentDf.PAYROLL_NUM).rlike('.*GDC.*$') |
                trim(currentDf.PAYROLL_NUM).rlike('.*KRC.*$') |
                trim(currentDf.PAYROLL_NUM).rlike('.*CF-HR.*$') |
                trim(currentDf.PAYROLL_NUM).rlike('.*DA.*$') |
                # trim(currentDf.PAYROLL_NUM).rlike('.*DAS.*$') |
                trim(currentDf.PAYROLL_NUM).rlike('.*RC.*$') |
                # trim(currentDf.PAYROLL_NUM).rlike('.*ADMIN.*$') |
                trim(currentDf.PAYROLL_NUM).rlike('.*ADM.*$')
            )
        ) 
        |
        (
        #COMPANY EXCLUSIONS
        (
            ~(upper(trim(currentDf.COMPANY)).rlike('.*PEREGRINE SECURITY.*$') |
            upper(trim(currentDf.COMPANY)).rlike('.*I S S FACILITY.*$') |
            upper(trim(currentDf.COMPANY)).rlike('.*ELITE SECURITY.*$') |
            upper(trim(currentDf.COMPANY)).rlike('.*COMPASS INDIA.*$') |
            (upper(trim(currentDf.COMPANY)).rlike('.*CALIBER.*$') & (~upper(trim(currentDf.DEPARTMENT)).rlike('.*TRANSPORT HD.*$')))
            )
        ) 
        &

        #DEPARTMENT EXCLUSIONS
        (
            ~(upper(trim(currentDf.DEPARTMENT)).rlike('.*G4S.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*ISS.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*GUARD.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*FOOD.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*GGN-FACILITY.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*GGN-FINANCE.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*MAIL ROOM.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*GGN-TRANSPORT.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*P/BOY.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*TELECOM.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*SECURITY.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*ADMIN OTHER.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*HOUSEKEEPING.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*/SUP.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*TRAVEL DESK.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*DVS VENDOR.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*NOIDA-FACILITY.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*PGPL.*$') |
            upper(trim(currentDf.DEPARTMENT)).rlike('.*MAINTENANCE.*$') 
            )
        ) &

        #JOB_TITLE EXCLUSIONS
        (
            ~(upper(trim(currentDf.JOB_TITLE)).rlike('.*G4S.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*ISS.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*GUARD.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*FOOD.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*TEMP CARD.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*PANTRY BOY.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*BOY.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*MST.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*CM.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*HK.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*H/K.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*TELECOM.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*RECEPTIONIST.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*SMART Q.*$') |
            upper(trim(currentDf.JOB_TITLE)).rlike('.*SECURITY.*$') 
            )
        ) &

        #PAYROLL_NUM EXCLUSIONS
        (
            ~(upper(trim(currentDf.PAYROLL_NUM)).rlike('.*G4S.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*ISS.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*GUARD.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*FOOD.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*HE.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*OTHER-.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*INF.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*M&E.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*H/K.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*HK.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*/SUP.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*P/BOY.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*BOY.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*P/B.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*CC.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TEMPORARY ACCESS..*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*CARD.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*EMERGENCY.*$') |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TEMP.*$') |
            (trim(currentDf.PAYROLL_NUM).rlike('.*SEC.*$') & (~trim(currentDf.PAYROLL_NUM).rlike('.*SECONDEE.*$')))|
            (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*T-.*$') & (trim(currentDf.JOB_TITLE) == '') &  (trim(currentDf.DEPARTMENT) == '')) |
            upper(trim(currentDf.PAYROLL_NUM)).rlike('.*SECURITY.*$') 
            )
        ) &

        #payroll = ADM- AND DEPT = ADMIN : EXCLUDE
        (
            ~(upper(trim(currentDf.PAYROLL_NUM)).rlike('.*ADM-.*$') & (trim(currentDf.DEPARTMENT) == 'ADMIN'))
        ) &

        #payroll = THIRD PARTY AND DEPT = ADMINISTRATION AND JOB_TITLE = EXECUTIVE : EXCLUDE
        (
            ~(((upper(trim(currentDf.PAYROLL_NUM)).rlike('.*THIRD PARTY.*$')) | (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*THIRD-PARTY.*$'))) & (upper(trim(currentDf.JOB_TITLE)) == 'EXECUTIVE') & (trim(currentDf.DEPARTMENT) == 'ADMINISTRATION'))
        ) &

        #payroll = THIRD PARTY AND DEPT = '' : EXCLUDE
        (
            ~(((upper(trim(currentDf.PAYROLL_NUM)).rlike('.*THIRD PARTY.*$')) | (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*THIRD-PARTY.*$'))| (upper(trim(currentDf.JOB_TITLE)).rlike('.*THIRD PARTY.*$')))& ((trim(currentDf.DEPARTMENT) == '') | (upper(trim(currentDf.DEPARTMENT)).rlike('.*ADMIN OTHER.*$'))))
        ) &

        #payroll = NEW JOINER AND DEPT = '' : EXCLUDE
        (
            ~((upper(trim(currentDf.PAYROLL_NUM)).rlike('.*NEW JOINER.*$') | upper(trim(currentDf.PAYROLL_NUM)).rlike('.*NJ.*$') | upper(trim(currentDf.PAYROLL_NUM)).rlike('.*N/J.*$') | upper(trim(currentDf.PAYROLL_NUM)).rlike('.*NEW JOINEE.*$')) & (trim(currentDf.DEPARTMENT) == ''))
        ) &

        #payroll = '' AND DEPT = '' AND JOB TITLE = '' AND NAME = '' : EXCLUDE
        (
            ~((trim(currentDf.PAYROLL_NUM) == '') & (trim(currentDf.DEPARTMENT) == '') & (trim(currentDf.JOB_TITLE) == '') & (trim(currentDf.NAME___REG_NUM) == ''))
        )

        )

    )
    display(currentDf)

# COMMAND ----------

if (tableName == 'card_swipe_master_data'):
    currentDf = currentDf.drop('LOCATION')
    currentDf = currentDf.withColumnRenamed('final_location', 'LOCATION')


# COMMAND ----------

# display(currentDf.filter(currentDf.DEPARTMENT == ''))

# COMMAND ----------

# from pyspark.sql.functions import substring, substring_index
 
# if (tableName == 'card_swipe_master_data'):
#     #get Job title from employee dump where its not available in trans report
#     jobTitleDf = spark.sql("select distinct Employee_Number, Position from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date = (select max(File_Date) from kgsonedatadb.trusted_hist_headcount_employee_dump where dated_on = (select max(dated_on) from kgsonedatadb.trusted_hist_headcount_employee_dump))")
    
#     #trim Job_Name in Employee Dump to remove letters before dot
#     jobTitleDf=jobTitleDf.withColumn("Position", trim(substring_index(col('Position'), '.', -1)))
    
#     currentDf = currentDf.join(jobTitleDf, trim(upper(currentDf.PAYROLL_NUM)) == trim(upper(jobTitleDf.Employee_Number)), "left")
    
#     currentDf = currentDf.withColumn('JOB_TITLE', when(trim(col('JOB_TITLE'))=='', col('Position')).otherwise(col('JOB_TITLE')))
#     display(currentDf)
    

# COMMAND ----------

# DBTITLE 1,Add BU2 from Admin_CC_BU_Mapping
if tableName != 'card_swipe_master_data':
    buDf = spark.sql("select Cost_centre, BU2 from kgsonedatadb.config_admin_cc_bu_mapping")
    buDf = buDf.dropDuplicates()
    currentDf = currentDf.join(buDf, upper(currentDf.DEPARTMENT)== upper(buDf.Cost_centre), "left")
    currentDf = currentDf.drop('Cost_centre')


# COMMAND ----------

#get BU2 from employee data where it is NULL
if tableName == 'card_swipe_master_data':
    
    employeeDf = spark.sql("select EMP_ID, NAME, EMAIL_ID, DEPARTMENT AS EMP_DEPARTMENT, BU2 as emp_BU, DESIGNATION____GRADE from kgsonedatadb.trusted_hist_admin_employee_data  where File_Date = "+ fileDate)
    employeeDf = employeeDf.dropDuplicates()

    currentDf = currentDf.join(employeeDf, ((upper(currentDf.PAYROLL_NUM)== upper(employeeDf.EMP_ID)) ) , "left")

    # currentDf = currentDf.withColumn('BU2', when(((currentDf.BU2.isNull()) | (currentDf.BU2 == '-')), col('emp_BU')).otherwise(col('BU2')))
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('EMP_DEPARTMENT')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('DESIGNATION____GRADE')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', col('EMP_DEPARTMENT'))
    currentDf = currentDf.withColumn('LEVEL', col('DESIGNATION____GRADE'))

    
    currentDf = currentDf.drop(*('EMP_ID', 'NAME', 'EMAIL_ID', 'EMP_DEPARTMENT', 'emp_BU','DESIGNATION____GRADE'))

display(currentDf)


# COMMAND ----------

#get BU2 from cwk data where it is NULL
if tableName == 'card_swipe_master_data':
    
    cwkDf = spark.sql("select EMP_ID, NAME, EMAIL_ID, DEPARTMENT AS EMP_DEPARTMENT, BU2 as emp_BU, DESIGNATION___GRADE from kgsonedatadb.trusted_admin_cwk_data where File_Date = "+ fileDate)
    cwkDf = cwkDf.dropDuplicates()

    currentDf = currentDf.join(cwkDf, ((upper(currentDf.PAYROLL_NUM)== upper(cwkDf.EMP_ID))) , "left")

    # currentDf = currentDf.withColumn('BU2', when(((currentDf.BU2.isNull()) | (currentDf.BU2 == '-')), col('emp_BU')).otherwise(col('BU2')))
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('EMP_DEPARTMENT')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('DESIGNATION___GRADE')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('EMP_DEPARTMENT')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('DESIGNATION___GRADE')).otherwise(col('LEVEL')))
    
    currentDf = currentDf.drop(*('EMP_ID', 'NAME', 'EMAIL_ID', 'EMP_DEPARTMENT', 'emp_BU','DESIGNATION___GRADE'))

display(currentDf)


# COMMAND ----------

#### Loaned data from HRMS Report is not reliable as it is not updated regularly, hence don't use this

# #get BU2 from loaned data where it is NULL
# if tableName == 'card_swipe_master_data':
    
#     loanedDf = spark.sql("select EMP_ID, NAME, EMAIL_ID, DEPARTMENT AS EMP_DEPARTMENT, BU2 as emp_BU,DESIGNATION____GRADE from kgsonedatadb.trusted_admin_loaned_data")
#     loanedDf = loanedDf.dropDuplicates()

#     currentDf = currentDf.join(loanedDf, ((upper(currentDf.PAYROLL_NUM)== upper(loanedDf.EMP_ID))) , "left")

#     # currentDf = currentDf.withColumn('BU2', when(((currentDf.BU2.isNull()) | (currentDf.BU2 == '-')), col('emp_BU')).otherwise(col('BU2')))
#     currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('EMP_DEPARTMENT')).otherwise(col('DEPARTMENT')))
# currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '-')), col('DESIGNATION____GRADE')).otherwise(col('JOB_TITLE')))
#     currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '-')), col('EMP_DEPARTMENT')).otherwise(col('Cost_Code')))
    
#     currentDf = currentDf.drop(*('EMP_ID', 'NAME', 'EMAIL_ID', 'EMP_DEPARTMENT', 'emp_BU','DESIGNATION____GRADE'))

# display(currentDf)


# COMMAND ----------

display(currentDf)

# COMMAND ----------

if tableName == 'card_swipe_master_data':

    #fetch department from monthly headcount report where it is not found in HRMS Report as well

    #EMPLOYEE DATA
    monthlyEmployeeDf = spark.sql("select Employee_Number,Full_Name,Cost_centre,'Employee_details' as Source,Position from kgsonedatadb.trusted_headcount_monthly_employee_details")
    monthlyEmployeeDf.dropDuplicates()
    currentDf = currentDf.join(monthlyEmployeeDf, ((upper(currentDf.PAYROLL_NUM)== upper(monthlyEmployeeDf.Employee_Number))) , "left")
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('Cost_centre')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('Position')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('Cost_centre')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('Position')).otherwise(col('LEVEL')))

    currentDf = currentDf.drop(*('Employee_Number', 'Full_Name', 'Cost_centre', 'Source','Position'))

    # display(currentDf)

    #RESIGNED DATA
    monthlyResignedDf = spark.sql("select Employee_Number,Full_Name,Cost_centre,'Resigned_and_Left' as Source, Position  from kgsonedatadb.trusted_headcount_monthly_resigned_and_left")
    monthlyResignedDf.dropDuplicates()
    currentDf = currentDf.join(monthlyResignedDf, ((upper(currentDf.PAYROLL_NUM)== upper(monthlyResignedDf.Employee_Number)) ) , "left")
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('Cost_centre')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('Position')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('Cost_centre')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('Position')).otherwise(col('LEVEL')))

    currentDf = currentDf.drop(*('Employee_Number', 'Full_Name', 'Cost_centre', 'Source','Position'))

    #CONTINGENT WORKER
    monthlyContingentWorker = spark.sql("select Candidate_Id as Employee_Number,Full_Name,Cost_centre,'Contingent_Worker' as Source, Position from kgsonedatadb.trusted_headcount_monthly_contingent_worker")
    monthlyContingentWorker = monthlyContingentWorker.withColumn("Employee_Number",regexp_replace(col("Employee_Number"), "[^a-zA-Z0-9]", ""))
    monthlyContingentWorker.dropDuplicates()
    currentDf = currentDf.join(monthlyContingentWorker, ((upper(currentDf.PAYROLL_NUM)== upper(monthlyContingentWorker.Employee_Number)) ) , "left")
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('Cost_centre')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('Position')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('Cost_centre')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('Position')).otherwise(col('LEVEL')))

    currentDf = currentDf.drop(*('Employee_Number', 'Full_Name', 'Cost_centre', 'Source','Position'))

    #ACADEMIC TRAINEE DATA
    monthlyAcademicTrainee = spark.sql("select Candidate_Id as Employee_Number,Full_Name,Cost_centre,'Academin_Trainee' as Source, Position from kgsonedatadb.trusted_headcount_monthly_academic_trainee")
    monthlyAcademicTrainee = monthlyAcademicTrainee.withColumn("Employee_Number",regexp_replace(col("Employee_Number"), "[^a-zA-Z0-9]", ""))
    monthlyAcademicTrainee.dropDuplicates()
    currentDf = currentDf.join(monthlyAcademicTrainee, ((upper(currentDf.PAYROLL_NUM)== upper(monthlyAcademicTrainee.Employee_Number)) ) , "left")
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('Cost_centre')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('Position')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('Cost_centre')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('Position')).otherwise(col('LEVEL')))

    currentDf = currentDf.drop(*('Employee_Number', 'Full_Name', 'Cost_centre', 'Source','Position'))

    #LOANED DATA
    monthlyLoanedDF = spark.sql("select Employee_Number,Employee_Name as Full_Name,Cost_centre,'Loned_Staff_from_KI' as Source, Position_Name from kgsonedatadb.trusted_headcount_loaned_staff_from_ki")
    monthlyLoanedDF.dropDuplicates()
    currentDf = currentDf.join(monthlyLoanedDF, ((upper(currentDf.PAYROLL_NUM)== upper(monthlyLoanedDF.Employee_Number)) ) , "left")
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('Cost_centre')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('Position_Name')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('Cost_centre')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('Position_Name')).otherwise(col('LEVEL')))

    currentDf = currentDf.drop(*('Employee_Number', 'Full_Name', 'Cost_centre', 'Source','Position_Name'))

    #CONTINGENT WORKER RESIGNED
    monthlyContingentWorkerResignedDf = spark.sql("select Candidate_Id as Employee_Number,Full_Name,Cost_centre,'Contingent_Worker_Resigned' as Source, Position from kgsonedatadb.trusted_headcount_monthly_contingent_worker_resigned")
    monthlyContingentWorkerResignedDf = monthlyContingentWorkerResignedDf.withColumn("Employee_Number",regexp_replace(col("Employee_Number"), "[^a-zA-Z0-9]", ""))
    monthlyContingentWorkerResignedDf.dropDuplicates()
    currentDf = currentDf.join(monthlyContingentWorkerResignedDf, ((upper(currentDf.PAYROLL_NUM)== upper(monthlyContingentWorkerResignedDf.Employee_Number)) ) , "left")
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull())  | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('Cost_centre')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('Position')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('Cost_centre')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('Position')).otherwise(col('LEVEL')))

    currentDf = currentDf.drop(*('Employee_Number', 'Full_Name', 'Cost_centre', 'Source','Position'))

    #LOANED RESIGNED
    monthlyLoanedResignedDf = spark.sql("select Employee_Number,Employee_Name as Full_Name,Cost_Center as Cost_centre,'Loned_Staff_Resigned' as Source, Position_Name from kgsonedatadb.trusted_headcount_monthly_loaned_staff_resigned")
    monthlyLoanedResignedDf.dropDuplicates()
    currentDf = currentDf.join(monthlyLoanedResignedDf, ((upper(currentDf.PAYROLL_NUM)== upper(monthlyLoanedResignedDf.Employee_Number)) ) , "left")
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('Cost_centre')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('Position_Name')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('Cost_centre')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('Position_Name')).otherwise(col('LEVEL')))

    currentDf = currentDf.drop(*('Employee_Number', 'Full_Name', 'Cost_centre', 'Source','Position_Name'))

    #SECONDEE OUTWARD
    monthlySecondeeOutwardDf = spark.sql("select Employee_Number,Full_Name,Cost_centre,'Secondee_Outward' as Source, Position from kgsonedatadb.trusted_headcount_monthly_secondee_outward")
    monthlySecondeeOutwardDf.dropDuplicates()
    currentDf = currentDf.join(monthlySecondeeOutwardDf, ((upper(currentDf.PAYROLL_NUM)== upper(monthlySecondeeOutwardDf.Employee_Number)) ) , "left")
    currentDf = currentDf.withColumn('DEPARTMENT', when(((currentDf.DEPARTMENT.isNull()) | (currentDf.DEPARTMENT == '') | (currentDf.DEPARTMENT == '-')), col('Cost_centre')).otherwise(col('DEPARTMENT')))
    # currentDf = currentDf.withColumn('JOB_TITLE', when(((currentDf.JOB_TITLE.isNull()) | (currentDf.JOB_TITLE == '') | (currentDf.JOB_TITLE == '-')), col('Position')).otherwise(col('JOB_TITLE')))
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (currentDf.Cost_Code == '') | (currentDf.Cost_Code == '-')), col('Cost_centre')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('Position')).otherwise(col('LEVEL')))

    currentDf = currentDf.drop(*('Employee_Number', 'Full_Name', 'Cost_centre', 'Source','Position'))

    # buDf = spark.sql("select Cost_centre, BU2 as config_BU from kgsonedatadb.config_admin_cc_bu_mapping")
    # buDf = buDf.dropDuplicates()
    # currentDf = currentDf.join(buDf, upper(currentDf.DEPARTMENT)== upper(buDf.Cost_centre), "left")
    # currentDf = currentDf.withColumn('BU2', when(((currentDf.BU2.isNull()) | (currentDf.BU2 == '-')), col('config_BU')).otherwise(col('BU2')))
    # currentDf = currentDf.drop(*('Cost_centre', 'config_BU'))

    display(currentDf)





# COMMAND ----------

if tableName == 'card_swipe_master_data':
    #Entity type Flag : If employee found in HRMS or daily headcount then 'KGS' otherwise 'Non_KGS'
    currentDf = currentDf.withColumn('ENTITY_TYPE', when((currentDf.Cost_Code.isNull()), lit('Non-KGS')).otherwise(lit('KGS')))

# COMMAND ----------

if tableName == 'card_swipe_master_data':
    #get cost code from transaction report if not found in HRMS or monthly headcount
    currentDf = currentDf.withColumn('Cost_Code', when(((currentDf.Cost_Code.isNull()) | (trim(currentDf.Cost_Code) == '') | (currentDf.Cost_Code == '-')), col('DEPARTMENT')).otherwise(col('Cost_Code')))
    currentDf = currentDf.withColumn('LEVEL', when(((currentDf.LEVEL.isNull()) | (currentDf.LEVEL == '') | (currentDf.LEVEL == '-')), col('JOB_TITLE')).otherwise(col('LEVEL')))

    display(currentDf.filter(currentDf.Cost_Code.isNull()))

# COMMAND ----------

if tableName == 'card_swipe_master_data':
    #Get BU2 mapping from Admin CC BU mapping
    buDf = spark.sql("select Cost_centre, BU2 from kgsonedatadb.config_admin_cc_bu_mapping")
    buDf = buDf.dropDuplicates()
    currentDf = currentDf.join(buDf, upper(currentDf.Cost_Code)== upper(buDf.Cost_centre), "left")
    currentDf = currentDf.drop('Cost_centre')

    display(currentDf)


# COMMAND ----------

display(currentDf.filter(currentDf.BU2.isNull()))

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, trim
if tableName == 'card_swipe_master_data':
    
    currentDf = currentDf.withColumn('BU2', 
    when((currentDf.BU2.isNull()) & (upper(trim(currentDf.LOCATION)).rlike('.*BANG.*$')) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TPT.*$')), lit('Admin'))
    .when((currentDf.BU2.isNull()) & (upper(trim(currentDf.LOCATION)).rlike('.*BANG.*$')) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*IT.*$')), lit('IT'))
    .otherwise(col('BU2'))
    )
    display(currentDf)

# COMMAND ----------

#Payroll to BU mapping
if tableName == 'card_swipe_master_data':
    currentDf = currentDf.withColumn('BU2', 
    when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*ADM.*$'))) , lit('Admin'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*GDC.*$'))) , lit('Audit-GDC'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*KRC.*$'))) , lit('Audit-KRC'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*MC.*$'))) , lit('MC'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*MS.*$'))) , lit('MS'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*RC.*$'))) , lit('RC'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TAX.*$'))) , lit('Tax'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TCS.*$'))) , lit('IT'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TE-DL-SOFTWAREENGG.*$'))) , lit('MC'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*IT.*$'))) , lit('IT'))
    .when(((currentDf.BU2.isNull()) & (upper(trim(currentDf.PAYROLL_NUM)).rlike('.*TEAM COMPUTTER.*$'))) , lit('IT'))
    .otherwise(col('BU2'))
    )

    display(currentDf)

# COMMAND ----------

display(currentDf.filter(currentDf.BU2.isNull()))

# COMMAND ----------

# if (tableName == 'xport_planned_vs_actuals' or tableName == 'xport_no_shows'):
# Check for holiday list if there is a Mandatory holiday for any location remove their data from  Booked andNo Show
holidayDf = spark.sql("select Occasion, Date as Holiday_Date, Location as Holiday_Location,BU from kgsonedatadb.trusted_admin_locationwise_holidaylist")
holidayDf = holidayDf.withColumn('Holiday_Location', when(upper(col('Holiday_Location')) == 'BENGALURU', lit('Bangalore')).otherwise(col('Holiday_Location')))
holidayDf = holidayDf.withColumn('Holiday_Location', when(upper(col('Holiday_Location')) == 'GURUGRAM', lit('Gurgaon')).otherwise(col('Holiday_Location')))
display(holidayDf)
# if (tableName == 'xport_planned_vs_actuals' or tableName == 'xport_no_shows'):
#     currentDf = currentDf.join(holidayDf, (upper(currentDf.LOCATION)== upper(holidayDf.Holiday_Location)) &(currentDf.DATE==holidayDf.Holiday_Date), "left")    

if (tableName == 'card_swipe_master_data'):
    currentDf = currentDf.join(holidayDf, (upper(currentDf.LOCATION)== upper(holidayDf.Holiday_Location)) &(currentDf.TRANSACTION_TIME==holidayDf.Holiday_Date), "left") 
else:
    currentDf = currentDf.join(holidayDf, (upper(currentDf.LOCATION)== upper(holidayDf.Holiday_Location)) &(currentDf.DATE==holidayDf.Holiday_Date), "left")
# currentDf = currentDf.filter(col('Occasion').isNull())
    


# COMMAND ----------

from pyspark.sql.functions import when, lit

currentDf = currentDf.withColumn(
    "FILTER_SELECTOR",
    when(currentDf.BU == 'All', lit('yes'))
    .otherwise(
        when((currentDf.BU == 'GDC') & (currentDf.BU2 == 'Audit-GDC'), lit('yes'))
        .otherwise(
            when((currentDf.BU == 'KGS & KRC') & (currentDf.BU2 != 'Audit-GDC'), lit('yes'))
            .otherwise(lit('No'))
        )
    )
)

currentDf = currentDf.filter(col('FILTER_SELECTOR')=='No')

# COMMAND ----------

# DBTITLE 1,Select required columns
if (tableName == 'card_swipe_master_data'):
    currentDf = currentDf.select('LOCATION','DEPARTMENT','PAYROLL_NUM','NAME___REG_NUM','JOB_TITLE','TRANSACTION_TIME','BU2','Cost_Code','Office_Location','ENTITY_TYPE','FILE_DATE')
if (tableName in ['xport_planned_vs_actuals_pune', 'xport_planned_vs_actuals_bangalore', 'xport_planned_vs_actuals_kochi', 'xport_planned_vs_actuals_kolkata', 'xport_planned_vs_actuals_gurgaon']):
    currentDf = currentDf.select('LOCATION','DATE','DEPARTMENT','PLANNED','ACTUAL','NO_SHOWS','BU2','FILE_DATE')
if (tableName in ['xport_no_shows_pune', 'xport_no_shows_bangalore', 'xport_no_shows_kochi', 'xport_no_shows_kolkata', 'xport_no_shows_gurgaon']):
    currentDf = currentDf.select('LOCATION','DATE','EMPLOYEE_NUMBER','NAME','DEPARTMENT','BU2','FILE_DATE')


# COMMAND ----------

display(currentDf)

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
from datetime import datetime
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata'))
currentDf = currentDf.withColumn("Dated_On",lit(currentdatetime))



# COMMAND ----------

currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Delta to SQL Load
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000, {'DeltaTableName':tableName, 'ProcessName':processName})