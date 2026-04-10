# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

#print(tableName)
#print(processName)
#print(fileDate)

# COMMAND ----------

# MAGIC %run /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import lit, col, from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat
from datetime import datetime,timedelta
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType


trusted_stg_lnd_url =trusted_stg_savepath_url+"lnd/"
#print(trusted_stg_lnd_url)

# COMMAND ----------

mydate = datetime.now()
month=mydate.strftime("%b")

year=mydate.strftime("%Y") 
#print(month , year )

# COMMAND ----------

def rename_emp_columns(df):
    for c in df.columns:
        df=df.withColumnRenamed(c,"Emp_"+c)
    return df

# COMMAND ----------

def rename_lw_columns(df):
    for c in df.columns:
        df=df.withColumnRenamed(c,"lw_"+c)
    return df

# COMMAND ----------

def rename_emp_dump_columns(df):
    for c in df.columns:
        df=df.withColumnRenamed(c,"Emp_"+c)
    return df

# COMMAND ----------

def rename_term_dump_columns(df):
    for c in df.columns:
        df=df.withColumnRenamed(c,"TD_"+c)
    return df

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Completion_Date,Item_Revision_Date,to_date(Completion_Date,'dd-MMM-yy'),to_date(Item_Revision_Date,'dd-MMM-yy') from kgsonedatadb.raw_hist_lnd_glms_details where File_Date = '20221001' and Local_HR_Id in ('118732')

# COMMAND ----------

# df_raw_curr_lnd_glms = spark.sql("select *,to_date(Completion_Date,'dd-MMM-yy') from kgsonedatadb.raw_hist_lnd_glms_details where File_Date = '20230301' and Local_HR_Id in ('133781','133540')")
# df_raw_curr_lnd_glms = df_raw_curr_lnd_glms.drop("df_raw_curr_lnd_glms")
# df_raw_curr_lnd_glms = df_raw_curr_lnd_glms.withColumnRenamed("to_date(Completion_Date, dd-MMM-yy)","Completion_Date")

display(df_raw_curr_lnd_glms)

# COMMAND ----------

# DBTITLE 1,Load data from  GLMS , Employee dump and Termination Dump 
#Load raw current GLMS data and remove records having more than 1 record for the same Item_Title and  Local_HR_ID
# df_raw_curr_lnd_glms = spark.sql("select * from kgsonedatadb.raw_hist_lnd_glms_details where File_Date = '20230301' and Local_HR_Id in ('133781','133540')")

df_raw_curr_lnd_glms = spark.sql("select *,to_date(Completion_Date,'dd-MMM-yy'),to_date(Item_Revision_Date,'dd-MMM-yy')  from kgsonedatadb.raw_hist_lnd_glms_details where File_Date = '20221001' and Local_HR_Id in ('118732')")
df_raw_curr_lnd_glms = df_raw_curr_lnd_glms.drop("Completion_Date","Item_Revision_Date")
df_raw_curr_lnd_glms = df_raw_curr_lnd_glms.withColumnRenamed("to_date(Completion_Date, dd-MMM-yy)","Completion_Date")
df_raw_curr_lnd_glms = df_raw_curr_lnd_glms.withColumnRenamed("to_date(Item_Revision_Date, dd-MMM-yy)","Item_Revision_Date")

df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn("CPD_CPE_Hours_Awarded",df_raw_curr_lnd_glms.CPD_CPE_Hours_Awarded.cast(DoubleType()))
# print("before raw glms",df_raw_curr_lnd_glms.count())
# df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn('row_num',row_number().over(Window.partitionBy(df_raw_curr_lnd_glms.Local_HR_ID,df_raw_curr_lnd_glms.Item_Title,df_raw_curr_lnd_glms.Completion_Date,df_raw_curr_lnd_glms.Item_ID).orderBy(df_raw_curr_lnd_glms.Total_Hours.desc()))).filter(col('row_num')==1).drop('row_num')
# print("after raw glms",df_raw_curr_lnd_glms.count())


#Load Employee Dump
df_emp_dump=spark.sql("select * from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date = '20230228' and (upper(Entity) != 'KI') and (Position is not null and trim(Position) != '') and (Job_Name is not null and trim(Job_Name) != '') order by File_Date desc").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

# print("df_emp_dump ",df_emp_dump.count())

df_emp_dump=df_emp_dump.withColumn('row_num',row_number().over(Window.partitionBy(df_emp_dump.Employee_Number).orderBy(df_emp_dump.Employee_Number))).where((col('row_num')==1)).drop('row_num')

df_emp_dump=rename_emp_dump_columns(df_emp_dump)

#Load Termination Dump
df_termination_dump=spark.sql("select * from kgsonedatadb.trusted_hist_headcount_termination_dump where File_Date = '20230228' and  (upper(Entity) != 'KI') and (Position is not null and trim(Position) != '') and (Job_Name is not null and trim(Job_Name) != '') order by File_Date desc").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

df_termination_dump=df_termination_dump.withColumn('row_num',row_number().over(Window.partitionBy(df_termination_dump.Employee_Number).orderBy(df_termination_dump.Employee_Number))).filter(col('row_num')==1).drop('row_num')

# print("df_termination_dump ",df_termination_dump.count())
df_termination_dump=rename_term_dump_columns(df_termination_dump)

# COMMAND ----------

display(df_raw_curr_lnd_glms)
# display(df_emp_dump.filter(col('Emp_Employee_number').isin('133781','133540')))
# display(df_termination_dump.filter(col('TD_Employee_number').isin('133781','133540')))

# COMMAND ----------

# DBTITLE 1,Load Dimensional Tables - Level Wise, Training Category, CC BU, CC BU SL, Global Function
#Load Level wise data
df_level_wise = spark.read.table("kgsonedatadb.config_dim_level_wise").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1));
df_level_wise=df_level_wise.drop("Dated_On")
df_level_wise=rename_lw_columns(df_level_wise)

#Load training category data
df_training_category = spark.read.table("kgsonedatadb.config_dim_training_category");
df_training_category=df_training_category.drop('Dated_On','File_Date')
# print("before df_training_category",df_training_category.count())
df_training_category==df_training_category.withColumn('row_num',row_number().over(Window.partitionBy(df_training_category.ItemTitle).orderBy(df_training_category.ItemTitle))).filter(col('row_num')==1).drop('row_num')
# print("after df_training_category",df_training_category.count())
 

#Load config_cost_center_business_unit
df_config_cost_center_business_unit = spark.read.table("kgsonedatadb.config_cost_center_business_unit");
df_config_cost_center_business_unit=df_config_cost_center_business_unit.drop('Dated_On')
# print("df_config_cost_center_business_unit ",df_config_cost_center_business_unit.count())


#Load config_cc_bu_sl to get SL
df_config_cc_bu_sl = spark.read.table("kgsonedatadb.config_cc_bu_sl");
df_config_cc_bu_sl=df_config_cc_bu_sl.drop('Dated_On')
# print("df_config_cc_bu_sl ",df_config_cc_bu_sl.count())

#Load config_dim_global_function to get Global Function
df_config_dim_global_function = spark.read.table("kgsonedatadb.config_dim_global_function");
df_config_dim_global_function=df_config_dim_global_function.drop('Dated_On')
df_config_dim_global_function=df_config_dim_global_function.withColumnRenamed('Global_Function','GlobalFunction')
#print("df_config_dim_global_function ",df_config_dim_global_function.count())

# COMMAND ----------

# DBTITLE 1,Levelwise Mapping of Senior AD, Director +, AD to 'AD and Above'
df_level_wise=df_level_wise.withColumn('Mapping_Original',col('lw_Mapping'))

df_level_wise=df_level_wise.withColumn('lw_Mapping',when(col('lw_Mapping').contains('Senior Associate Director') | col('lw_Mapping').contains('Associate Director') | col('lw_Mapping').contains('Director +'),'AD and Above').otherwise(col('lw_Mapping')))

# COMMAND ----------

df_level_wise.select("Mapping_Original","lw_Mapping").display()

# COMMAND ----------

# DBTITLE 1,Filtering out system_program_entity records and Instructed
df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.filter(df_raw_curr_lnd_glms.Item_Type != 'SYSTEM_PROGRAM_ENTITY')

df_raw_curr_lnd_glms_instructor=df_raw_curr_lnd_glms.filter(df_raw_curr_lnd_glms.Completion_Status_Description == 'Instructed')

df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.filter(df_raw_curr_lnd_glms.Completion_Status_Description != 'Instructed')

# print("After dropping Instructed,SYSTEM_PROGRAM_ENTITY records GLMS Count",df_raw_curr_lnd_glms.count())

# COMMAND ----------

display(df_raw_curr_lnd_glms)

# COMMAND ----------

df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn('Is_Client_Facing?',when(lower(col('Is_Client_Facing?')).contains('y'),'Yes')\
    .when(lower(col('Is_Client_Facing?')).contains('n'),'No')\
    .otherwise(col('Is_Client_Facing?')))

# COMMAND ----------

display(df_raw_curr_lnd_glms)

# COMMAND ----------

# DBTITLE 1,Filter out bad data records considering completion date is in current month and year 
df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn("YYYMM_Completion_Date", date_format("Completion_Date", "yyyyMM"))

maxdm=df_raw_curr_lnd_glms.groupBy('YYYMM_Completion_Date').agg(count('YYYMM_Completion_Date').alias('datemonth'))
display(maxdm)

maxCount = maxdm.select(max(maxdm.datemonth)).rdd.flatMap(lambda x: x).collect()
maxCount=maxCount[0]
print('maxCount  ',maxCount)

monthYear = maxdm.select('YYYMM_Completion_Date').filter(maxdm.datemonth == maxCount).rdd.flatMap(lambda x: x).collect()
monthYear=monthYear[0]
print('monthYear  ',monthYear)

monthYear =202210

df_raw_curr_lnd_glms =df_raw_curr_lnd_glms.filter(df_raw_curr_lnd_glms.YYYMM_Completion_Date ==monthYear)
df_raw_curr_lnd_glms =df_raw_curr_lnd_glms.drop('YYYMM_Completion_Date')
#print('df_raw_curr_lnd_glms  ',df_raw_curr_lnd_glms.count())

# COMMAND ----------

display(df_raw_curr_lnd_glms)

# COMMAND ----------

# DBTITLE 1,Join GLMS Input data with Employee Dump and Termination Dump
#Join GLMS data with Employee Dump and Termination Dump

df_join_lnd_emp_sl=df_raw_curr_lnd_glms.alias('glms').join(df_emp_dump,df_raw_curr_lnd_glms.Local_HR_ID ==  df_emp_dump.Emp_Employee_Number, 'left').select('glms.*','Emp_Employee_Number','Emp_Full_Name','Emp_Function','Emp_Employee_Subfunction','Emp_Employee_Subfunction_1','Emp_Cost_centre','Emp_Business_Category','Emp_Client_Geography','Emp_Location','Emp_Position','Emp_Job_Name','Emp_Email_Address','Emp_File_Date')

df_join_lnd_emp_sl=df_join_lnd_emp_sl.alias('glms_EmpDump').join(df_termination_dump,df_join_lnd_emp_sl.Local_HR_ID ==  df_termination_dump.TD_Employee_Number, 'left').select('glms_EmpDump.*','TD_Employee_Number','TD_Full_Name','TD_Function','TD_Employee_Subfunction','TD_Employee_Subfunction_1','TD_Cost_Centre','TD_Business_Category','TD_Client_Geography','TD_Location','TD_Position','TD_Job_Name','TD_email_Address','TD_File_Date')

# print("df_join_lnd_emp_sl count",df_join_lnd_emp_sl.count())

# COMMAND ----------

display(df_join_lnd_emp_sl)

# COMMAND ----------

# For Position
df_join_lnd_emp_sl=df_join_lnd_emp_sl.withColumn('Position',when(col('Emp_Position').isNull() & col('TD_Position').isNotNull(),col('TD_Position'))\
    .when(col('Emp_Position').isNotNull() & col('TD_Position').isNull(),col('Emp_Position'))\
    .when(col('Emp_Position').isNotNull() & col('TD_Position').isNotNull(),col('Emp_Position'))\
    .otherwise(None))

# COMMAND ----------

#For Job Name
df_join_lnd_emp_sl=df_join_lnd_emp_sl.withColumn('Job_Name',when(col('Emp_Job_Name').isNull() & col('TD_Job_Name').isNotNull(),col('TD_Job_Name'))\
    .when(col('Emp_Job_Name').isNotNull() & col('TD_Job_Name').isNull(),col('Emp_Job_Name'))\
    .when(col('Emp_Job_Name').isNotNull() & col('TD_Job_Name').isNotNull(),col('Emp_Job_Name'))\
    .otherwise(None))

# COMMAND ----------

#For Location
df_join_lnd_emp_sl=df_join_lnd_emp_sl.withColumn('Location',when(col('Emp_Location').isNull() & col('TD_Location').isNotNull(),col('TD_Location'))\
    .when(col('Emp_Location').isNotNull() & col('TD_Location').isNull(),col('Emp_Location'))\
    .when(col('Emp_Location').isNotNull() & col('TD_Location').isNotNull(),col('Emp_Location'))\
    .otherwise(None))

# COMMAND ----------

# DBTITLE 1,Join with Level wise - Position & Job Name to get Mapping
#Position and Job Name Join with Dim Level Wise to get Mapping
df_join_lnd_emp_sl_lw = df_join_lnd_emp_sl.join(df_level_wise, (lower(df_join_lnd_emp_sl.Position) == lower(df_level_wise.lw_Position)) & (lower(df_join_lnd_emp_sl.Job_Name) == lower(df_level_wise.lw_Job_Name)),'left')
#print("df_join_lnd_emp_sl_lw",df_join_lnd_emp_sl_lw.count())

# COMMAND ----------

#df_join_lnd_emp_sl_lw.select('Position','Job_Name').filter(col('lw_Mapping').isNull()).distinct().display()

# df_join_lnd_emp_sl_lw.select('Local_HR_Id').filter(col('Position').isNull()).distinct().display()

# COMMAND ----------

#df_join_lnd_emp_sl.select('*').distinct().where(col("Emp_Employee_Number").isNull() & col("TD_Employee_Number").isNull()).display()

# COMMAND ----------

# DBTITLE 1,Join with training Category dimensional tables
#Join with Training category data to get Training category column
df_join_lnd_joined = df_join_lnd_emp_sl_lw.join(df_training_category, lower(df_join_lnd_emp_sl_lw.Item_Title) == lower(df_training_category.ItemTitle), 'left')
# print("df_join_lnd_joined",df_join_lnd_joined.count())
df_join_lnd_joined=df_join_lnd_joined.drop("ItemTitle")

# COMMAND ----------

# Leadership (Techno functional + Performance enablement)
df_join_lnd_joined=df_join_lnd_joined.withColumn('TrainingCategory',when(lower(col('TrainingCategory')).contains('performance enablement'),'Enabling')\
.when(col('lw_Mapping').contains('AD and Above'),'Leadership')\
.otherwise(col('TrainingCategory')))

# COMMAND ----------

#display(df_join_lnd_joined.select('Cost_Center','Emp_Cost_centre','TD_Cost_Centre','Emp_Client_Geography','TD_Client_Geography'))

# COMMAND ----------

# DBTITLE 1,Derive Cost Centre from Emp Dump | Termination Dump
df_join_lnd_joined=df_join_lnd_joined.withColumn('Cost_Centre',when(col('Emp_Employee_Number').isNotNull() & col('TD_Employee_Number').isNull(),col('Emp_Cost_centre'))\
.when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNull(),col('TD_Cost_Centre'))\
                                                 .when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNotNull(),col('Emp_Cost_Centre'))\
                                                 .otherwise(None))

# COMMAND ----------

managerEDDf = df_emp_dump.select("Emp_Email_Address",'Emp_Cost_Centre','Emp_Client_Geography')\
    .withColumnRenamed("Emp_Cost_Centre", "RM_Cost_Centre")\
    .withColumnRenamed("Emp_Email_Address", "RM_Email_Address")\
    .withColumnRenamed("Emp_Client_Geography", "RM_Client_Geography")

managerTDDf = df_termination_dump.select("TD_email_Address",'TD_Cost_Centre','TD_Client_Geography')\
    .withColumnRenamed("TD_Cost_Centre", "TRM_Cost_Centre")\
    .withColumnRenamed("TD_email_Address", "TRM_Email_Address")\
    .withColumnRenamed("TD_Client_Geography", "TRM_Client_Geography")

# COMMAND ----------

# DBTITLE 1,Derive Cost Centre for employees not in Emp/TD Dump from Reporting Manager Email Address
df_join_lnd_joined= df_join_lnd_joined.alias('df_join_lnd_joined')\
    .join(managerEDDf, (lower(df_join_lnd_joined.Manager_Email_Address) == lower(managerEDDf.RM_Email_Address)), 'left')
    #.join(managerTDDf,(lower(df_join_lnd_joined.Manager_Email_Address) == lower(managerTDDf.TRM_Email_Address)), 'left').select('TRM_Cost_Centre','TRM_Email_Address','TRM_Client_Geography')
#df_join_lnd_joined.count() #.select('df_join_lnd_joined.*','RM_Cost_Centre','RM_Email_Address','RM_Client_Geography')\

# COMMAND ----------

## Fill Cost Centre from RM CC
df_join_lnd_joined=df_join_lnd_joined.withColumn('Cost_Centre',when(col('Cost_Centre').isNull(),col('RM_Cost_Centre')).otherwise(col('Cost_Centre')))

# COMMAND ----------

#df_join_lnd_joined.filter(col("Cost_Centre").isNull()).display()

# COMMAND ----------

#df_join_lnd_joined.select("*").where(col('Cost_Centre').isNull() & col('RM_Email_Address').isNull()).display()

# COMMAND ----------

# DBTITLE 1,Derive Geo from Emp Dump | Termination Dump
df_join_lnd_joined=df_join_lnd_joined.withColumn('Geo',when(col('Emp_Employee_Number').isNotNull() & col('TD_Employee_Number').isNull(),col('Emp_Client_Geography'))\
.when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNull(),col('TD_Client_Geography'))\
                                                 .when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNotNull(),col('Emp_Client_Geography'))\
                                                 .otherwise(None))

# COMMAND ----------

#df_join_lnd_joined.filter(col("Geo").isNull()).display()

# COMMAND ----------

## Fill Geo from RM Geo
df_join_lnd_joined=df_join_lnd_joined.withColumn('Geo',when(col('Geo').isNull(),col('RM_Client_Geography')).otherwise(col('Geo')))

# COMMAND ----------

#display(df_join_lnd_joined.select('Cost_Centre','Geo').distinct())


# COMMAND ----------

# DBTITLE 1,Join with Cost center business unit table to get BU
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_cost_center_business_unit,lower(df_join_lnd_joined.Cost_Centre) == lower(df_config_cost_center_business_unit.Cost_centre),'left').select('df_join_lnd_joined.*','Final_BU')

df_join_lnd_joined=df_join_lnd_joined.withColumnRenamed('Final_BU','BU')
# df_join_lnd_joined.count()

# COMMAND ----------

# DBTITLE 1,Join with Dim Global Function table on BU to get global function of Employee
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_dim_global_function,lower(df_join_lnd_joined.BU)==lower(df_config_dim_global_function.Final_BU),'left').select('df_join_lnd_joined.*','GlobalFunction')
#df_join_lnd_joined.count()

# COMMAND ----------

#df_join_lnd_joined.select('Cost_Centre','BU','GlobalFunction','Emp_File_Date','TD_File_Date').filter(col('Cost_Centre').isNotNull() & col('BU').isNull()).distinct().display()

# COMMAND ----------

# DBTITLE 1,Join with CC BU SL to get SL
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_cc_bu_sl,(lower(df_join_lnd_joined.Cost_Centre) == lower(df_config_cc_bu_sl.Cost_centre)) & (lower(df_join_lnd_joined.Geo) == lower(df_config_cc_bu_sl.Client_Geography)) & (lower(df_join_lnd_joined.BU) == lower(df_config_cc_bu_sl.Final_BU)),'left').select('df_join_lnd_joined.*','Service_Line')
# df_join_lnd_joined.count()

# COMMAND ----------

#display(df_join_lnd_joined.select('Local_HR_ID','Emp_Employee_Number','TD_Employee_Number','Cost_Centre','Geo','BU','Service_Line').where(col("Service_Line").isNull() & col("Cost_Centre").isNull() & col("Geo").isNull() & col("BU").isNull()))

# COMMAND ----------

# DBTITLE 1,Filter out bad data records based on File date concept , not implementing this when completion date is in current month and year 
# if(date == '01'):
#     yestDate = (datetime.strptime(fileDate, '%Y%m%d') - timedelta(days=1) ).strftime('%Y%m%d')
#     yestDate = yestDate[:6] 
#     df_join_lnd_joined=df_join_lnd_joined.withColumn("YYYMM_Completion_Date", date_format("Completion_Date", "yyyyMM"))
#     df_join_lnd_joined =df_join_lnd_joined.filter(df_join_lnd_joined.YYYMM_Completion_Date ==yestDate)
#     df_join_lnd_joined=df_join_lnd_joined.drop('YYYMM_Completion_Date')
# else:
#     df_join_lnd_joined = df_join_lnd_joined.withColumn("YYYMM_Completion_Date", date_format("Completion_Date", "yyyyMM"))  # 20221001--202211  202210
#     df_join_lnd_joined =df_join_lnd_joined.filter(df_join_lnd_joined.YYYMM_Completion_Date ==monthYear)
#     df_join_lnd_joined=df_join_lnd_joined.drop('YYYMM_Completion_Date')


# print(df_join_lnd_joined.count())
#.withColumn("Period", from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMMyy"))\

# COMMAND ----------

df_lnd_transformed = df_join_lnd_joined\
.withColumn("Training_Category",when(upper(col("Item_Title")).contains("KVA"), "Enabling")\
            .when(lower(col("Item_Title")).contains("linkedin learning"), "Enabling")\
            .when(lower(col("Item_Title")).contains("linkedin"), "Enabling")\
            .when(lower(col("Item_Title")).contains("performance enablement"), "Enabling")\
            .otherwise(col("TrainingCategory")))\
            .withColumn("Period", from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM yyyy"))\
.withColumn("Quarter", when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Jan", 1)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Feb", 1)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Mar", 1)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Apr", 2)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "May", 2)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Jun", 2)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Jul", 3)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Aug", 3)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Sep", 3)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Oct", 4)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Nov", 4)\
            .when((from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMM")) == "Dec", 4)\
            .otherwise(lit("")))\
.withColumn("Level_Wise",col("lw_Mapping"))\
.withColumnRenamed("Is_Client_Facing?","Is_Client_Facing")\
.withColumn("CC", regexp_replace(col("Department"), "IN_", ""))\
.withColumn("Cost_Center", regexp_replace(col("Cost_Center"), "IN_", ""))\
.withColumn("Item_Type_Category",when(col("Item_Type") == "ILT","VILT").when(col("Item_Type") == "VILT","VILT").otherwise("Self Paced"))\
.withColumn("Local_Function", regexp_replace(col("Local_Function"), "IN_", ""))\
.withColumn("ORG_ID", regexp_replace(col("ORG_ID"), "KGS_", ""))\
.withColumn("Local_HR_ID",df_join_lnd_joined["Local_HR_ID"].cast(StringType()))\
.withColumn("Year",lit(year))\
.withColumn("Month",lit(month))\
.withColumn("GLMS_Dated_on",lit(mydate))\
.withColumn("File_Type",lit('GLMS'))

#df_lnd_transformed.count()

# COMMAND ----------

display(df_lnd_transformed)

# COMMAND ----------

## Add Code to Load Bad records in another database table
finalDf=df_lnd_transformed.withColumn('Emp_TD_Employee_Number',when(col('Emp_Employee_Number').isNull() & col('TD_Employee_Number').isNotNull(),col('TD_Employee_Number'))\
    .when(col('Emp_Employee_Number').isNotNull() & col('TD_Employee_Number').isNull(),col('Emp_Employee_Number'))\
    .when(col('Emp_Employee_Number').isNotNull() & col('TD_Employee_Number').isNotNull(),col('Emp_Employee_Number'))\
    .otherwise(None))\
    .withColumn('Emp_TD_Email_Address',when(col('Emp_Email_Address').isNull() & col('TD_email_Address').isNotNull(),col('TD_email_Address'))\
    .when(col('Emp_Email_Address').isNotNull() & col('TD_email_Address').isNull(),col('Emp_Email_Address'))\
    .when(col('Emp_Email_Address').isNotNull() & col('TD_email_Address').isNotNull(),col('Emp_Email_Address'))\
    .otherwise(None))

display(finalDf)
 
# badDf=finaldf.select('*').filter((col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull()) | (col('Email_Address__User_') =='kpmgindiaadmin@skillton.degreed.com') | col('Level_Wise').isNull() | col('Period').isNull() | col('Training_Category').isNull() | col('CPD_CPE_Hours_Awarded').isNull() | col('Cost_centre').isNull() | col('BU').isNull() | col('Local_Service_Line').isNull() | col('Service_Line').isNull() | col('Geo').isNull() | col('Location').isNull() | col('Item_Type_Category').isNull())  # Check 1 correct

badDf=finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull()) | (col('Email_Address__User_') =='kpmgindiaadmin@skillton.degreed.com'))

badDf=badDf.withColumn('Remarks',when(col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull(),lit('Employee not present in both the Dumps'))\
                                .when(col('Email_Address__User_') == ('kpmgindiaadmin@skillton.degreed.com'), lit('Email Id is Invalid kpmgindiaadmin@skillton.degreed.com'))\
                                .when(col('Level_Wise').isNull(),lit('Level Wise is Null'))\
                                .when(col('Training_Category').isNull(),lit('Training Category is Null'))\
                                .when(col('CPD_CPE_Hours_Awarded').isNull(),lit('CPD_CPE_Hours_Awarded is Null'))\
                                .when(col('Cost_centre').isNull(),lit('Cost_centre is Null'))\
                                .when(col('BU').isNull(),lit('BU is Null'))\
                                .when(col('Service_Line').isNull(),lit('Service_Line is Null'))\
                                .when(col('Local_Service_Line').isNull(),lit('Local_Service_Line is Null'))\
                                .when(col('Geo').isNull(),lit('Geo is Null'))\
                                .when(col('Location').isNull(),lit('Location is Null'))\
                                .when(col('Item_Type_Category').isNull(),lit('Item_Type_Category is Null'))\
                                .otherwise(None))


# COMMAND ----------

#Drop Unwanted Computed Columns
drop_cols=['Emp_Employee_Number','Emp_Full_Name','Emp_Function','Emp_Employee_Subfunction','Emp_Employee_Subfunction_1','Emp_Cost_centre','Emp_Business_Category','Emp_Client_Geography','Emp_Location','Emp_Position','Emp_Job_Name','Emp_Email_Address','TD_Employee_Number','TD_Full_Name','TD_Function','TD_Employee_Subfunction','TD_Employee_Subfunction_1','TD_Cost_Centre','TD_Business_Category','TD_Client_Geography','TD_Location','TD_Position','TD_Job_Name','TD_email_Address','TD_Location','TD_Position','RM_Email_Address','RM_Cost_Centre','RM_Client_Geography']

badDf=badDf.drop(*drop_cols)

# COMMAND ----------

#display(badDf.select('Local_HR_ID','Emp_TD_Employee_Number','Emp_TD_Email_Address').filter((col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull()) | (col('Email_Address__User_') =='kpmgindiaadmin@skillton.degreed.com')).distinct())

# COMMAND ----------

# display(badDf.select("Local_HR_ID","Position","Job_Name","Level_wise","Cost_Centre","BU","Service_Line","*"))


badDf.display()

# COMMAND ----------

badDfCount = badDf.count()

if int(badDfCount) > 0:
    badDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true") \
    .option("path",bad_filepath_url+processName+"/"+tableName+"_bad") \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb_badrecords.trusted_hist_"+ processName + "_" + tableName+"_bad")

# COMMAND ----------

# finalDf =  finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNotNull() & col('Emp_TD_Email_Address').isNotNull()) & (col('Email_Address__User_') !='kpmgindiaadmin@skillton.degreed.com') & col('Level_Wise').isNotNull() & col('Period').isNotNull() & col('Training_Category').isNotNull() & col('CPD_CPE_Hours_Awarded').isNotNull() & col('Cost_centre').isNotNull() & col('BU').isNotNull() & col('Local_Service_Line').isNotNull() & col('Service_Line').isNotNull() & col('Geo').isNotNull() & col('Location').isNotNull() & col('Item_Type_Category').isNotNull())

finalDf =  finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNotNull() & col('Emp_TD_Email_Address').isNotNull()) & (col('Email_Address__User_') !='kpmgindiaadmin@skillton.degreed.com'))
finalDf=finalDf.drop(*drop_cols)

# COMMAND ----------

#finalDf.count()

# COMMAND ----------

# DBTITLE 1,Load to Trusted Staging
finalDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path", trusted_stg_lnd_url+"glms_details") \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_lnd_glms_details")

# COMMAND ----------

# DBTITLE 1,Load data to final trusted table
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':'glms_details','ProcessName':'lnd'})

# COMMAND ----------

# DBTITLE 1,Load to final glms_kva table
dbutils.notebook.run("/kgsonedata/trusted/lnd/glms_kva_to_final_trusted_hist_lnd_glms_kva_details",6000,{'DeltaTableName':'glms_kva_details','ProcessName':processName,'SourceFile':tableName,'FileDate':fileDate})

# COMMAND ----------

