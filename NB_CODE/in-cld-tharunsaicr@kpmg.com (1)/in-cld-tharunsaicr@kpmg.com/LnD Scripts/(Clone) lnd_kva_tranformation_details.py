# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

# print(tableName)
# print(processName)
# print(fileDate)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import lit, col, from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat
from pyspark.sql.types import *
# from datetime import datetime,timedelta
from datetime import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys
import pyspark.sql.functions as f

trusted_stg_lnd_url =trusted_stg_savepath_url+"lnd/"
#print(trusted_stg_lnd_url)

# COMMAND ----------

# import datetime
fileYear = fileDate[:4]
fileYear_FY = "FY"+fileDate[2:4]
prevFileYear = int(fileYear) - 1
fileMonth = fileDate[4:6]
fileDay = fileDate[6:]

print("Curr File Year",fileYear)
print("Prev File Year",prevFileYear)

convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay
print(convertedFileDate)

convertedFileDate = datetime.strptime(convertedFileDate, '%Y-%m-%d').date()
print(convertedFileDate)

# COMMAND ----------

mydate = datetime.now()

filDate1 = convertedFileDate

month=filDate1.strftime("%B")
year=filDate1.strftime("%Y") 
print(month , year )

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

# DBTITLE 1,Load data from  KVA, Employee dump and Termination Dump 
#Load raw current KVA data

df_raw_curr_lnd_kva = spark.sql("select * from kgsonedatadb.test_raw_curr_lnd_kva_details")

#filtering out Successfactors records
df_raw_curr_lnd_kva=df_raw_curr_lnd_kva.filter(lower(coalesce(df_raw_curr_lnd_kva.Content_Provider,lit('NA'))) != 'successfactors')

#deleting duplicate records which are having same content title for the same employee
#print("before raw kva",df_raw_curr_lnd_kva.count())
# df_raw_curr_lnd_kva=df_raw_curr_lnd_kva.withColumn('row_num',row_number().over(Window.partitionBy(df_raw_curr_lnd_kva.Employee_Id,df_raw_curr_lnd_kva.Content_Title,df_raw_curr_lnd_kva.Content_Type).orderBy(df_raw_curr_lnd_kva.Duration.desc()))).filter(col('row_num')==1).drop('row_num')

# display(df_raw_curr_lnd_kva.groupBy(df_raw_curr_lnd_kva.CPE_Hours).count())

#Load Employee Dump
# df_emp_dump=spark.sql("select * from kgsonedatadb.trusted_hist_headcount_employee_dump where (upper(Entity) != 'KI') and (Position is not null and trim(Position) != '') and (Job_Name is not null and trim(Job_Name) != '') order by File_Date desc").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

# df_emp_dump=spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump A where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump B where B.Employee_Number=A.Employee_Number and to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) ed_hist where rank = 1").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

df_emp_dump= spark.sql("select * from kgsonedatadb.test_raw_curr_headcount_employee_dump").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))
print("df_emp_dump ",df_emp_dump.count())

df_emp_dump=df_emp_dump.withColumn('row_num',row_number().over(Window.partitionBy(df_emp_dump.Employee_Number).orderBy(df_emp_dump.Employee_Number))).where((col('row_num')==1)).drop('row_num')

df_emp_dump=rename_emp_dump_columns(df_emp_dump)

#Load Termination Dump
# df_termination_dump=spark.sql("select * from kgsonedatadb.trusted_hist_headcount_termination_dump where (upper(Entity) != 'KI') and (Position is not null and trim(Position) != '') and (Job_Name is not null and trim(Job_Name) != '') order by File_Date desc").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

# df_termination_dump = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_termination_dump A where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_termination_dump B where B.Employee_Number=A.Employee_Number and to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) td_hist where rank = 1").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

# df_termination_dump=spark.sql("select * from kgsonedatadb.test_raw_curr_headcount_termination_dump").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

df_termination_dump=spark.sql("select * from kgsonedatadb.test_raw_curr_headcount_termination_dump").withColumn("Job_Name", split(col("Job"), "\.").getItem(1))

df_termination_dump=df_termination_dump.withColumn('row_num',row_number().over(Window.partitionBy(df_termination_dump.Employee_Number).orderBy(df_termination_dump.Employee_Number))).filter(col('row_num')==1).drop('row_num')

print("df_termination_dump ",df_termination_dump.count())
df_termination_dump=rename_term_dump_columns(df_termination_dump)
df_termination_dump=df_termination_dump.withColumnRenamed('TD_Profit_Centre','TD_Cost_Centre')

# COMMAND ----------

df_emp_dump = df_emp_dump.join(df_termination_dump, df_emp_dump.Emp_Employee_Number == df_termination_dump.TD_Employee_Number, 'left_anti').select(df_emp_dump["*"])
df_termination_dump = df_termination_dump.join(df_emp_dump, df_emp_dump.Emp_Employee_Number == df_termination_dump.TD_Employee_Number, 'left_anti').select(df_termination_dump["*"])
# print(df_emp_dump.count())
# print(df_termination_dump.count())

# display(df_raw_curr_lnd_kva.select("File_Date").distinct())
# display(df_raw_curr_lnd_kva.agg(sum('CPE_Hours')))

# COMMAND ----------

# DBTITLE 1,Load Dimensional Tables - Level Wise, CC BU, CC BU SL, Global Function
#Load Level wise data
df_level_wise = spark.read.table("kgsonedatadb.config_dim_level_wise").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1));
df_level_wise=df_level_wise.drop("Dated_On","File_Date")
df_level_wise=rename_lw_columns(df_level_wise)

#Load config_cost_center_business_unit

# df_config_cost_center_business_unit = spark.sql("select distinct Cost_centre,Final_BU from (select *,row_number() over(partition by Cost_centre,Final_BU order by Dated_On desc) as rownum from kgsonedatadb.config_hist_cost_center_business_unit where File_Date = (select max(File_Date) from kgsonedatadb.config_hist_cost_center_business_unit where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1");

# df_config_cost_center_business_unit=spark.sql('select * from kgsonedatadb.test_config_cost_center_business_unit')

# df_config_cost_center_business_unit = spark.sql("select distinct Cost_centre,Final_BU from (select *,row_number() over(partition by Cost_centre,Final_BU order by Dated_On desc) as rownum from kgsonedatadb.config_hist_cc_bu_sl_master where File_Date = (select max(File_Date) from kgsonedatadb.config_hist_cc_bu_sl_master where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1");

df_config_cost_center_business_unit=spark.sql("select distinct Cost_centre,Final_BU from kgsonedatadb.config_test_lnd_cc_bu_sl ")

#Load config_cc_bu_sl to get SL

# df_config_cc_bu_sl = spark.sql("select distinct Cost_centre,Final_BU,Client_Geography,Service_Line from (select *,row_number() over(partition by Cost_centre,Final_BU,Client_Geography,Service_Line order by Dated_On desc) as rownum from kgsonedatadb.test_config_cc_bu_sl where File_Date = (select max(File_Date) from kgsonedatadb.test_config_cc_bu_sl where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1")

# df_config_cc_bu_sl=spark.sql("select * from kgsonedatadb.test_config_cc_bu_sl")

# df_config_cc_bu_sl = spark.sql("select distinct Cost_centre,Final_BU,Client_Geography,Service_Line from (select *,row_number() over(partition by Cost_centre,Final_BU,Client_Geography,Service_Line order by Dated_On desc) as rownum from kgsonedatadb.config_hist_cc_bu_sl_master where File_Date = (select max(File_Date) from kgsonedatadb.config_hist_cc_bu_sl_master where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1")
df_config_cc_bu_sl=spark.sql("select distinct Cost_centre,Final_BU,Client_Geography,Service_Line from kgsonedatadb.config_test_lnd_cc_bu_sl")

#Load config_dim_global_function to get Global Function
df_config_dim_global_function = spark.read.table("kgsonedatadb.config_dim_global_function");
df_config_dim_global_function=df_config_dim_global_function.drop('Dated_On','File_Date')
#print("df_config_dim_global_function ",df_config_dim_global_function.count())

# COMMAND ----------

# DBTITLE 1,Levelwise Mapping of Senior AD, Director +, AD to 'AD and Above'
df_level_wise=df_level_wise.withColumn('Mapping_Original',col('lw_Mapping'))

df_level_wise=df_level_wise.withColumn('lw_Mapping',when(col('lw_Mapping').contains('Senior Associate Director') | col('lw_Mapping').contains('Associate Director') | col('lw_Mapping').contains('Director +'),'AD and Above').otherwise(col('lw_Mapping')))
#display(df_level_wise)

# COMMAND ----------

# display(df_level_wise)
# df_level_wise.select('Mapping').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Content Type	Duration Unit	Conversion Logic:
# MAGIC ***************************************************
# MAGIC Course or Video: 	Hours To take the value form 'Duration' column, 
# MAGIC           as is.To be added in the new 'CPE Hours' column
# MAGIC 			
# MAGIC In case duration is blank or zero (0) - show it as zero (0)	
# MAGIC
# MAGIC ----------------------------------------------------------------			
# MAGIC Course or Video:  Minutes To divide the value in the 'Duration' column by 60	
# MAGIC 			
# MAGIC In case duration is blank or zero (0) - show it as zero (0)	
# MAGIC Course or Video	Seconds	To divide the 'Duration' column by 3600	
# MAGIC
# MAGIC --------------------------------------------------------------
# MAGIC In case duration is blank or zero (0):  - show it as zero (0)	
# MAGIC All other Content Type	All other Duration Unit 	To show Zero (0) hour in the new 'CPE Hours' column	
# MAGIC (Including blanks)	(Including blanks)	
# MAGIC
# MAGIC --------------------------------------------------------------

# COMMAND ----------

# DBTITLE 1,New calculation for CPE Hours
df_lnd_kva_col_updated = df_raw_curr_lnd_kva.withColumn("CPE_Hours",when(((col("Content_Type") == "Course") | (col("Content_Type") == "Video"))&(col("Duration_Unit")=='Hours'), col("Duration"))\
.when(((col("Content_Type") == "Course") | (col("Content_Type") == "Video"))&(col("Duration_Unit")=='Minutes'), col("Duration")/60)\
.when(((col("Content_Type") == "Course") | (col("Content_Type") == "Video"))&(col("Duration_Unit")=='Seconds'), col("Duration")/3600)\
.otherwise(0))

# COMMAND ----------

df_lnd_kva_col_updated= df_lnd_kva_col_updated.withColumn("CPE_Hours",df_lnd_kva_col_updated["CPE_Hours"].cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Replace Completion_Date Col values with Date_Added for Col(Completion_Date ) is NULL or Empty
df_lnd_kva_col_updated=df_lnd_kva_col_updated.withColumn("Completion_Date",when((df_lnd_kva_col_updated["Completion_Date"].isNull()) | (df_lnd_kva_col_updated["Completion_Date"] == "") | ((df_raw_curr_lnd_kva["Completion_Date"] == "1900-01-01") ) ,col("Date_Added")).otherwise(df_lnd_kva_col_updated.Completion_Date))

# COMMAND ----------

#df_lnd_kva_col_updated.count()

# COMMAND ----------

# DBTITLE 1,Filter out bad data records and keep only completion date is in current month and year 
# df_lnd_kva_col_updated=df_lnd_kva_col_updated.withColumn("YYYMM_Completion_Date", date_format("Completion_Date", "yyyyMM"))
df_lnd_kva_col_updated=df_lnd_kva_col_updated.withColumn("YYYY_Completion_Date", date_format("Completion_Date", "yyyy"))
df_lnd_kva_col_updated=df_lnd_kva_col_updated.withColumn("MM_Completion_Date", date_format("Completion_Date", "MM"))

# display(df_lnd_kva_col_updated.select("MM_Completion_Date","YYYY_Completion_Date").distinct())
        
# maxdm=df_lnd_kva_col_updated.groupBy('YYYMM_Completion_Date').agg(count('YYYMM_Completion_Date').alias('datemonth'))
# #display(maxdm)

# maxCount = maxdm.select(max(maxdm.datemonth)).rdd.flatMap(lambda x: x).collect()
# maxCount=maxCount[0]
# #print('maxCount  ',maxCount)

# monthYear = maxdm.select('YYYMM_Completion_Date').filter(maxdm.datemonth == maxCount).rdd.flatMap(lambda x: x).collect()
# monthYear=monthYear[0]
# #print('monthYear  ',monthYear)


df_lnd_kva_col_updated =df_lnd_kva_col_updated.filter((df_lnd_kva_col_updated.YYYY_Completion_Date ==fileYear)|((df_lnd_kva_col_updated.YYYY_Completion_Date ==prevFileYear)&(df_lnd_kva_col_updated.MM_Completion_Date.isin('10','11','12'))))
display(df_lnd_kva_col_updated.select("MM_Completion_Date","YYYY_Completion_Date").distinct())

df_lnd_kva_col_updated =df_lnd_kva_col_updated.drop('YYYY_Completion_Date','MM_Completion_Date')
# df_lnd_kva_col_updated =df_lnd_kva_col_updated.drop('YYYMM_Completion_Date')

print('df_lnd_kva_col_updated  ',df_lnd_kva_col_updated.count())


# COMMAND ----------

# DBTITLE 1,Join KVA Input data with Employee Dump and Termination Dump
#Join KVA data with Employee Dump and Termination DUmp

# df_lnd_kva_col_updated = df_lnd_kva_col_updated.alias('kva').join(df_emp_dump,(lower(df_lnd_kva_col_updated.Organization_Email) ==  lower(df_emp_dump.Emp_Email_Address)) | (df_lnd_kva_col_updated.Employee_Id==df_emp_dump.Emp_Employee_Number), 'left').select('kva.*','Emp_Employee_Number','Emp_Full_Name','Emp_Function','Emp_Employee_Subfunction','Emp_Employee_Subfunction_1','Emp_Cost_centre','Emp_Business_Category','Emp_Client_Geography','Emp_Location','Emp_Position','Emp_Job_Name','Emp_Email_Address','Emp_File_Date')

df_lnd_kva_col_updated = df_lnd_kva_col_updated.join(df_emp_dump,(lower(df_lnd_kva_col_updated.Organization_Email) ==  lower(df_emp_dump.Emp_Email_Address)) | (df_lnd_kva_col_updated.Employee_Id==df_emp_dump.Emp_Employee_Number), 'left').select(df_lnd_kva_col_updated['*'],'Emp_Employee_Number','Emp_Full_Name','Emp_Function','Emp_Sub_Function','Emp_Cost_centre','Emp_Business_Category','Emp_Client_Geography','Emp_Location','Emp_Position','Emp_Job_Name','Emp_Email_Address','Emp_File_Date')

print("df_lnd_kva_col_updated count",df_lnd_kva_col_updated.count())


# COMMAND ----------

# df_lnd_kva_col_updated=df_lnd_kva_col_updated.withColumn('row_num',row_number().over(Window.partitionBy(df_lnd_kva_col_updated.Employee_Id,df_lnd_kva_col_updated.Content_Title,df_lnd_kva_col_updated.Content_Type).orderBy(df_lnd_kva_col_updated.Duration.desc()))).filter(col('row_num')>1).display()

# COMMAND ----------

# df_lnd_kva_col_updated=df_lnd_kva_col_updated.alias('kva_EmpDump').join(df_termination_dump,(lower(df_lnd_kva_col_updated.Organization_Email) ==  lower(df_termination_dump.TD_email_Address)) | (df_lnd_kva_col_updated.Employee_Id==df_termination_dump.TD_Employee_Number), 'left').select('kva_EmpDump.*','TD_Employee_Number','TD_Full_Name','TD_Function','TD_Employee_Subfunction','TD_Employee_Subfunction_1','TD_Cost_Centre','TD_Business_Category','TD_Client_Geography','TD_Location','TD_Position','TD_Job_Name','TD_email_Address','TD_File_Date')

df_lnd_kva_col_updated=df_lnd_kva_col_updated.join(df_termination_dump,(lower(df_lnd_kva_col_updated.Organization_Email) ==  lower(df_termination_dump.TD_email_Address)) | (df_lnd_kva_col_updated.Employee_Id==df_termination_dump.TD_Employee_Number), 'left').select(df_lnd_kva_col_updated['*'],'TD_Employee_Number','TD_Employee_Name','TD_Function','TD_Sub_Function','TD_Cost_Centre','TD_Business_Category','TD_Client_Geography','TD_Location','TD_Position','TD_Job_Name','TD_email_Address','TD_File_Date')

print("df_lnd_kva_col_updated count",df_lnd_kva_col_updated.count())

# COMMAND ----------

# For Position
df_lnd_kva_col_updated=df_lnd_kva_col_updated.withColumn('Position',when(col('Emp_Position').isNull() & col('TD_Position').isNotNull(),col('TD_Position'))\
    .when(col('Emp_Position').isNotNull() & col('TD_Position').isNull(),col('Emp_Position'))\
    .when(col('Emp_Position').isNotNull() & col('TD_Position').isNotNull(),col('Emp_Position'))\
    .otherwise(None))

# COMMAND ----------

#For Job Name
df_lnd_kva_col_updated=df_lnd_kva_col_updated.withColumn('Job_Name',when(col('Emp_Job_Name').isNull() & col('TD_Job_Name').isNotNull(),col('TD_Job_Name'))\
    .when(col('Emp_Job_Name').isNotNull() & col('TD_Job_Name').isNull(),col('Emp_Job_Name'))\
    .when(col('Emp_Job_Name').isNotNull() & col('TD_Job_Name').isNotNull(),col('Emp_Job_Name'))\
    .otherwise(None))

# COMMAND ----------

#For Location
df_lnd_kva_col_updated=df_lnd_kva_col_updated.withColumn('Location',when(col('Emp_Location').isNull() & col('TD_Location').isNotNull(),col('TD_Location'))\
    .when(col('Emp_Location').isNotNull() & col('TD_Location').isNull(),col('Emp_Location'))\
    .when(col('Emp_Location').isNotNull() & col('TD_Location').isNotNull(),col('Emp_Location'))\
    .otherwise(None))

# COMMAND ----------

# DBTITLE 1,Join with Level wise - Position & Job Name to get Mapping
#Position and Job Name Join with Dim Level Wise to get Mapping
df_join_lnd_emp_sl_lw = df_lnd_kva_col_updated.join(df_level_wise, (lower(df_lnd_kva_col_updated.Position) == lower(df_level_wise.lw_Position)) & (lower(df_lnd_kva_col_updated.Job_Name) == lower(df_level_wise.lw_Job_Name)),'left')
#print("df_join_lnd_emp_sl_lw",df_join_lnd_emp_sl_lw.count())

# COMMAND ----------

#df_join_lnd_emp_sl_lw.select('lw_Mapping','Mapping_Original').distinct().display()

# COMMAND ----------

#df_join_lnd_emp_sl_lw.select('Employee_Id','Emp_Employee_Number','TD_Employee_Number','Position','Job_Name','lw_Position','lw_Job_Name',"Mapping_Original","lw_Mapping").filter(col('lw_Mapping').isNull()).distinct().display()

# COMMAND ----------

# DBTITLE 1,Sum of Total KVA,LL Hours and Its 40% Hours
total_hours=df_join_lnd_emp_sl_lw.select(sum(df_join_lnd_emp_sl_lw.CPE_Hours)).rdd.flatMap(lambda x: x).collect()
total_hours=total_hours[0]
# total_hours = df_join_lnd_emp_sl_lw.agg(sum('CPE_Hours')).collect()[0][0]

#print("total_hours" , total_hours )
enabling_hours=((total_hours * 40)/100)
#print("enabling_hours" ,enabling_hours)

# COMMAND ----------

# DBTITLE 1,Training Category to Enabling / Technology / Techno-functional
df_join_lnd_emp_sl_lw=df_join_lnd_emp_sl_lw.withColumn('Training_Category',lit(""))

df_join_lnd_joined=df_join_lnd_emp_sl_lw .withColumn('cumsum', f.sum(df_join_lnd_emp_sl_lw .CPE_Hours).over(Window.partitionBy().orderBy().rowsBetween(-sys.maxsize, 0)))

df_join_lnd_joined=df_join_lnd_joined.withColumn('Training_Category',when(col('cumsum')<=enabling_hours,'Enabling').otherwise('Technology/ Techno-functional'))

df_join_lnd_joined=df_join_lnd_joined.drop('cumsum')

# COMMAND ----------

# Leadership (Techno functional + Performance enablement)
df_join_lnd_joined = df_join_lnd_joined\
.withColumn('Training_Category',when(col('lw_Mapping').contains('AD and Above'),'Leadership')\
.otherwise(col('Training_Category')))

# COMMAND ----------

# DBTITLE 1,Derive Cost Centre from Emp Dump | Termination Dump
df_join_lnd_joined=df_join_lnd_joined.withColumn('Cost_Centre',when(col('Emp_Employee_Number').isNotNull() & col('TD_Employee_Number').isNull(),col('Emp_Cost_centre'))\
.when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNull(),col('TD_Cost_Centre'))\
                                                 .when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNotNull(),col('Emp_Cost_Centre'))\
                                                 .otherwise(None))

# COMMAND ----------

# DBTITLE 1,Derive Geo from Emp Dump | Termination Dump
df_join_lnd_joined=df_join_lnd_joined.withColumn('Geo',when(col('Emp_Employee_Number').isNotNull() & col('TD_Employee_Number').isNull(),col('Emp_Client_Geography'))\
.when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNull(),col('TD_Client_Geography'))\
                                                 .when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNotNull(),col('Emp_Client_Geography'))\
                                                 .otherwise(None))

# COMMAND ----------

# DBTITLE 1,Join with Cost center business unit table to get BU
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_cost_center_business_unit,lower(df_join_lnd_joined.Cost_Centre) == lower(df_config_cost_center_business_unit.Cost_centre),'left').select('df_join_lnd_joined.*','Final_BU')
#df_join_lnd_joined.count()

df_join_lnd_joined=df_join_lnd_joined.withColumnRenamed('Final_BU','BU')

# COMMAND ----------

# DBTITLE 1,Join with Dim Global Function table on BU to get global function of Employee
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_dim_global_function,lower(df_join_lnd_joined.BU)==lower(df_config_dim_global_function.Final_BU),'left').select('df_join_lnd_joined.*','Global_Function')
#df_join_lnd_joined.count()

# COMMAND ----------

# DBTITLE 1,Join with CC BU SL to get SL
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_cc_bu_sl,(lower(df_join_lnd_joined.Cost_Centre) == lower(df_config_cc_bu_sl.Cost_centre)) & (lower(df_join_lnd_joined.Geo) == lower(df_config_cc_bu_sl.Client_Geography)) & (lower(df_join_lnd_joined.BU) == lower(df_config_cc_bu_sl.Final_BU)),'left').select('df_join_lnd_joined.*','Service_Line')
#df_join_lnd_joined.count()

# COMMAND ----------

#display(df_join_lnd_joined.select('Cost_Centre','Geo','BU','Service_Line'))
# display(df_config_cost_center_business_unit.select('Cost_Centre').where(col('Cost_Centre').contains('Audit-C')))

# COMMAND ----------

# DBTITLE 1,Filter out bad data records when completion date is less than date added and completion date is in current month and year 
# if(date == '01'):
#     yestDate = (datetime.strptime(fileDate, '%Y%m%d') - timedelta(days=1) ).strftime('%Y%m%d')
#     yestDate = yestDate[:6] 
#     df_join_lnd_joined=df_join_lnd_joined.withColumn("YYYMM_Completion_Date", date_format("Completion_Date", "yyyyMM"))
#     df_join_lnd_joined =df_join_lnd_joined.filter(df_join_lnd_joined.YYYMM_Completion_Date ==yestDate)
#     df_join_lnd_joined=df_join_lnd_joined.drop('YYYMM_Completion_Date')
# else:
#     df_join_lnd_joined = df_join_lnd_joined.withColumn("YYYMM_Completion_Date", date_format("Completion_Date", "yyyyMM"))  # 2022-10-01--202211  202210
#     df_join_lnd_joined =df_join_lnd_joined.filter(df_join_lnd_joined.YYYMM_Completion_Date ==monthYear)
#     df_join_lnd_joined=df_join_lnd_joined.drop('YYYMM_Completion_Date')


# print(df_join_lnd_joined.count())
# .withColumn("Period", from_unixtime(unix_timestamp(col("Completion_Date"), "yyyy-MM-dd"), "MMMyy"))\

# COMMAND ----------

df_lnd_transformed = df_join_lnd_joined\
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
.withColumn('Content_Title',concat(lit('KVA - '),'Content_Title'))\
.withColumn("Level_Wise",col("lw_Mapping"))\
.withColumn("Item_Type_Category",when(col("Content_Type") == "ILT","VILT").when(col("Content_Type") == "VLT","VILT").otherwise("Self Paced"))\
.withColumn("Year",lit(year))\
.withColumn("Month",lit(month))\
.withColumn("File_Type",lit('KVA'))\
.withColumn("Kva_Dated_on",lit(mydate))

# df_lnd_transformed.count()

# COMMAND ----------

#df_lnd_transformed.count()

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
 
# badDf=finaldf.select('*').filter((col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull()) | (col('Organization_Email') =='kpmgindiaadmin@skillton.degreed.com') | col('Level_Wise').isNull() | col('Period').isNull() | col('Training_Category').isNull() | col('CPE_Hours').isNull() | col('Cost_centre').isNull() | col('BU').isNull() | col('Service_Line').isNull() | col('Geo').isNull() |  
# col('Location').isNull() | col('Item_Type_Category').isNull())  # Check 1 correct


badDf=finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull()) | (col('Organization_Email') == 'kpmgindiaadmin@skillton.degreed.com') | (col('Position').isNull()) | (trim(col('Position'))== '') | col('Cost_centre').isNull() | (trim(col('Cost_centre'))=='')) 



# badDf=finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull()) | (col('Organization_Email') =='kpmgindiaadmin@skillton.degreed.com'))  # Check 1 correct

badDf=badDf.withColumn('Remarks',when(col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull(),lit('Employee not present in both the Dumps'))\
                                .when(col('Organization_Email') == ('kpmgindiaadmin@skillton.degreed.com'), lit('Email Id is Invalid kpmgindiaadmin@skillton.degreed.com'))\
                                .when(col('Level_Wise').isNull(),lit('Level Wise is Null'))\
                                .when(col('Training_Category').isNull(),lit('Training Category is Null'))\
                                .when(col('CPE_Hours').isNull(),lit('CPE_Hours is Null'))\
                                .when(col('Cost_centre').isNull(),lit('Cost_centre is Null'))\
                                .when(col('BU').isNull(),lit('BU is Null'))\
                                .when(col('Service_Line').isNull(),lit('Service_Line is Null'))\
                                .when(col('Geo').isNull(),lit('Geo is Null'))\
                                .when(col('Location').isNull(),lit('Location is Null'))\
                                .when(col('Item_Type_Category').isNull(),lit('Item_Type_Category is Null'))\
                                .otherwise(None))

# COMMAND ----------

#Drop Unwanted Computed Columns
drop_cols=['Emp_Employee_Number','Emp_Full_Name','Emp_Function','Emp_Employee_Subfunction','Emp_Employee_Subfunction_1','Emp_Cost_centre','Emp_Business_Category','Emp_Client_Geography','Emp_Location','Emp_Position','Emp_Job_Name','Emp_Email_Address','TD_Employee_Number','TD_Full_Name','TD_Function','TD_Employee_Subfunction','TD_Employee_Subfunction_1','TD_Cost_Centre','TD_Business_Category','TD_Client_Geography','TD_Location','TD_Position','TD_Job_Name','TD_email_Address','TD_Location','TD_Position','RM_Email_Address','RM_Cost_Centre','RM_Client_Geography']

badDf=badDf.drop(*drop_cols)

# COMMAND ----------

# Commenting and changing logic on 8/14/2023 based on confirmation from Pooja

# finalDf=finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNotNull() & col('Emp_TD_Email_Address').isNotNull()) & (coalesce(col('Organization_Email'),lit('NA')) != 'kpmgindiaadmin@skillton.degreed.com') & (col('Position').isNotNull()) & (trim(coalesce(col('Position'),lit('')))!= '') & col('Cost_centre').isNotNull() & (trim(coalesce(col('Cost_centre'),lit('')))!='')) 

finalDf=finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNotNull() & col('Emp_TD_Email_Address').isNotNull()) & (coalesce(col('Organization_Email'),lit('NA')) != 'kpmgindiaadmin@skillton.degreed.com')) 

finalDf=finalDf.drop(*drop_cols)

# display(finalDf.agg(sum('CPE_Hours')))

# COMMAND ----------

finalDf.count()
display(finalDf.agg(sum('CPE_Hours'))) #6093.111388888854

# COMMAND ----------

display(finalDf.select(('Mapping_Original')).distinct())

# COMMAND ----------

badDfCount = badDf.count()

if int(badDfCount) > 0:
    badDf=colcaststring(badDf,badDf.columns)
    badDf = badDf.withColumn("Dated_On", badDf["Dated_On"].cast(TimestampType()))

    badDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true") \
    .option("path",bad_filepath_url+processName+"/"+tableName+"_bad_test") \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb_badrecords.test_trusted_hist_"+ processName + "_" + tableName+"_bad")

# COMMAND ----------

# DBTITLE 1,Load to Trusted Staging
finalDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path", trusted_stg_lnd_url+"kva_details_test") \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.test_trusted_stg_lnd_kva_details")

# COMMAND ----------

# DBTITLE 1,Load data to final trusted table
dbutils.notebook.run("/Users/in-cld-tharunsaicr@kpmg.com/LnD Scripts/(Clone) trustedstg_to_trusted_load",6000,{'DeltaTableName':'kva_details','ProcessName':'lnd'})

# COMMAND ----------

# DBTITLE 1,Load to final glms_kva table
dbutils.notebook.run("/Users/in-cld-tharunsaicr@kpmg.com/LnD Scripts/(Clone) glms_kva_to_final_trusted_hist_lnd_glms_kva_details",6000,{'DeltaTableName':'glms_kva_details','ProcessName':processName,'SourceFile':tableName,'FileDate':fileDate})