# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

print(tableName)
print(processName)
print(fileDate)

# COMMAND ----------

# MAGIC %run 
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# from pyspark.sql.functions import lit, col, from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat, sum
# from datetime import datetime,timedelta
from datetime import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType,TimestampType


trusted_stg_lnd_url =trusted_stg_savepath_url+"lnd/"
#print(trusted_stg_lnd_url)

# COMMAND ----------

# import datetime
fileYear = fileDate[:4]
fileYear_FY = "FY"+fileDate[2:4]
prevFileYear = int(fileYear) - 1
fileMonth = fileDate[4:6]
fileDay = fileDate[6:]

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

#Load raw current GLMS data and remove records having more than 1 record for the same Item_Title and  Local_HR_ID
df_raw_curr_lnd_glms = spark.read.table("kgsonedatadb.raw_curr_lnd_glms_details")

# commenting below 2 KVA filters and adding Source_Type column on 11/6/2023 post confirmation from Pooja that KVA data wil flow throught GLMS and need not be filtered
# df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.filter(~coalesce(df_raw_curr_lnd_glms.Item_Title,lit('NA')).contains('KVA - '))
# df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.filter(~lower(coalesce(df_raw_curr_lnd_glms.Last_Update_User_ID,lit('NA'))).contains('connector'))

df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn("CPD_CPE_Hours_Awarded",regexp_replace(col('CPD_CPE_Hours_Awarded'),',',''))
df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn("CPD_CPE_Hours_Awarded",df_raw_curr_lnd_glms.CPD_CPE_Hours_Awarded.cast("double"))

df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn('Item_Title_decode',unidecode_udf('Item_Title'))
df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn('Item_Title_decode',regexp_replace('Item_Title_decode', '\n', ' ')) 

df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn("Source_Type",when(coalesce(df_raw_curr_lnd_glms.Item_Title,lit('NA')).contains('KVA - '),lit('KVA')).otherwise(lit('GLMS')))
df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn("KVA_CPE_Hours",when(coalesce(df_raw_curr_lnd_glms.Item_Title,lit('NA')).contains('KVA - '),df_raw_curr_lnd_glms.CPD_CPE_Hours_Awarded).otherwise(0))

# print("before raw glms",df_raw_curr_lnd_glms.count())

# display(df_raw_curr_lnd_glms)

# COMMAND ----------

# DBTITLE 1,Load data from  GLMS , Employee dump and Termination Dump 


# df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn('row_num',row_number().over(Window.partitionBy(df_raw_curr_lnd_glms.Local_HR_ID,df_raw_curr_lnd_glms.Item_Title,df_raw_curr_lnd_glms.Completion_Date,df_raw_curr_lnd_glms.Item_ID).orderBy(df_raw_curr_lnd_glms.Total_Hours.desc()))).filter(col('row_num')==1).drop('row_num')
# print("after raw glms",df_raw_curr_lnd_glms.count())


#Load Employee Dump
# df_emp_dump=spark.sql("select * from kgsonedatadb.trusted_hist_headcount_employee_dump where (upper(Entity) != 'KI') and (Position is not null and trim(Position) != '') and (Job_Name is not null and trim(Job_Name) != '') order by File_Date desc").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

df_emp_dump=spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump A where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump B where B.Employee_Number=A.Employee_Number and to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) ed_hist where rank = 1").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

# print("df_emp_dump ",df_emp_dump.count())
# display(df_emp_dump.select("File_Date").distinct())

df_emp_dump=df_emp_dump.withColumn('row_num',row_number().over(Window.partitionBy(df_emp_dump.Employee_Number).orderBy(df_emp_dump.Employee_Number))).where((col('row_num')==1)).drop('row_num')

df_emp_dump=rename_emp_dump_columns(df_emp_dump)

df_termination_dump = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_termination_dump A where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_termination_dump B where B.Employee_Number=A.Employee_Number and to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) td_hist where rank = 1").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1))

df_termination_dump=df_termination_dump.withColumn('row_num',row_number().over(Window.partitionBy(df_termination_dump.Employee_Number).orderBy(df_termination_dump.Employee_Number))).filter(col('row_num')==1).drop('row_num')

# print("df_termination_dump ",df_termination_dump.count())
df_termination_dump=rename_term_dump_columns(df_termination_dump)

# COMMAND ----------

df_emp_dump = df_emp_dump.join(df_termination_dump, df_emp_dump.Emp_Employee_Number == df_termination_dump.TD_Employee_Number, 'left_anti').select(df_emp_dump["*"])
df_termination_dump = df_termination_dump.join(df_emp_dump, df_emp_dump.Emp_Employee_Number == df_termination_dump.TD_Employee_Number, 'left_anti').select(df_termination_dump["*"])
# print(df_emp_dump.count())
# print(df_termination_dump.count())

# COMMAND ----------

# DBTITLE 1,Load Dimensional Tables - Level Wise, Training Category, CC BU, CC BU SL, Global Function
#Load Level wise data
df_level_wise = spark.read.table("kgsonedatadb.config_dim_level_wise").withColumn("Job_Name", split(col("Job_Name"), "\.").getItem(1));
df_level_wise=df_level_wise.drop("Dated_On","File_Date")
df_level_wise=rename_lw_columns(df_level_wise)

#Load training category data
df_training_category = spark.read.table("kgsonedatadb.config_dim_training_category");
df_training_category=df_training_category.drop('Dated_On','File_Date')
# print("before df_training_category",df_training_category.count())
df_training_category==df_training_category.withColumn('row_num',row_number().over(Window.partitionBy(df_training_category.ItemTitle).orderBy(df_training_category.ItemTitle))).filter(col('row_num')==1).drop('row_num')
# print("after df_training_category",df_training_category.count())
 

#Load config_cost_center_business_unit
# df_config_cost_center_business_unit = spark.sql("select distinct Cost_centre,Final_BU from (select *,row_number() over(partition by Cost_centre,Final_BU order by Dated_On desc) as rownum from kgsonedatadb.config_hist_cost_center_business_unit where File_Date = (select max(File_Date) from kgsonedatadb.config_hist_cost_center_business_unit where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1");
df_config_cost_center_business_unit = spark.sql("select distinct Cost_centre,Final_BU from (select *,row_number() over(partition by Cost_centre,Final_BU order by Dated_On desc) as rownum from kgsonedatadb.config_hist_cc_bu_sl where File_Date = (select max(File_Date) from kgsonedatadb.config_hist_cc_bu_sl where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1");

#Load config_cc_bu_sl to get SL
df_config_cc_bu_sl = spark.sql("select distinct Cost_centre,Final_BU,Client_Geography,Service_Line from (select *,row_number() over(partition by Cost_centre,Final_BU,Client_Geography,Service_Line order by Dated_On desc) as rownum from kgsonedatadb.config_hist_cc_bu_sl where File_Date = (select max(File_Date) from kgsonedatadb.config_hist_cc_bu_sl where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1")

#Load config_dim_global_function to get Global Function
df_config_dim_global_function = spark.read.table("kgsonedatadb.config_dim_global_function");
df_config_dim_global_function=df_config_dim_global_function.drop('Dated_On','File_Date')
df_config_dim_global_function=df_config_dim_global_function.withColumnRenamed('Global_Function','GlobalFunction')
#print("df_config_dim_global_function ",df_config_dim_global_function.count())

# COMMAND ----------

# DBTITLE 1,Levelwise Mapping of Senior AD, Director +, AD to 'AD and Above'
df_level_wise=df_level_wise.withColumn('Mapping_Original',col('lw_Mapping'))

df_level_wise=df_level_wise.withColumn('lw_Mapping',when(col('lw_Mapping').contains('Senior Associate Director') | col('lw_Mapping').contains('Associate Director') | col('lw_Mapping').contains('Director +'),'AD and Above').otherwise(col('lw_Mapping')))

# COMMAND ----------

# df_level_wise.select("Mapping_Original","lw_Mapping").display()

# COMMAND ----------

# DBTITLE 1,Filtering out system_program_entity records and Instructed
# print("Before dropping Instructed,SYSTEM_PROGRAM_ENTITY records GLMS Count",df_raw_curr_lnd_glms.count())
df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.filter(coalesce(df_raw_curr_lnd_glms.Item_Type,lit('NA')) != 'SYSTEM_PROGRAM_ENTITY')

df_raw_curr_lnd_glms_instructor=df_raw_curr_lnd_glms.filter(df_raw_curr_lnd_glms.Completion_Status_Description == 'Instructed')

df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.filter(coalesce(df_raw_curr_lnd_glms.Completion_Status_Description,lit('NA')) != 'Instructed')

# print("After dropping Instructed,SYSTEM_PROGRAM_ENTITY records GLMS Count",df_raw_curr_lnd_glms.count())

# COMMAND ----------

df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn('Is_Client_Facing?',when(lower(col('Is_Client_Facing?')).contains('y'),'Yes')\
    .when(lower(col('Is_Client_Facing?')).contains('n'),'No')\
    .otherwise(col('Is_Client_Facing?')))

# COMMAND ----------

# DBTITLE 1,Filter out bad data records considering completion date is in current month and year 
# df_raw_curr_lnd_glms=df_raw_curr_lnd_glms.withColumn("YYYMM_Completion_Date", date_format("Completion_Date", "yyyyMM"))

# maxdm=df_raw_curr_lnd_glms.groupBy('YYYMM_Completion_Date').agg(count('YYYMM_Completion_Date').alias('datemonth'))
# # display(maxdm)

# maxCount = maxdm.select(max(maxdm.datemonth)).rdd.flatMap(lambda x: x).collect()
# maxCount=maxCount[0]
# # print('maxCount  ',maxCount)

# monthYear = maxdm.select('YYYMM_Completion_Date').filter(maxdm.datemonth == maxCount).rdd.flatMap(lambda x: x).collect()
# monthYear=monthYear[0]
# # print('monthYear  ',monthYear)


# df_raw_curr_lnd_glms =df_raw_curr_lnd_glms.filter(df_raw_curr_lnd_glms.YYYMM_Completion_Date ==monthYear)
# df_raw_curr_lnd_glms =df_raw_curr_lnd_glms.drop('YYYMM_Completion_Date')
# #print('df_raw_curr_lnd_glms  ',df_raw_curr_lnd_glms.count())

# COMMAND ----------

# DBTITLE 1,Join GLMS Input data with Employee Dump and Termination Dump
#Join GLMS data with Employee Dump and Termination Dump

df_join_lnd_emp_sl=df_raw_curr_lnd_glms.alias('glms').join(df_emp_dump,df_raw_curr_lnd_glms.Local_HR_ID ==  df_emp_dump.Emp_Employee_Number, 'left').select('glms.*','Emp_Employee_Number','Emp_Full_Name','Emp_Function','Emp_Employee_Subfunction','Emp_Employee_Subfunction_1','Emp_Cost_centre','Emp_Business_Category','Emp_Client_Geography','Emp_Location','Emp_Position','Emp_Job_Name','Emp_Email_Address','Emp_File_Date')
# print("df_join_lnd)join_ed",df_join_lnd_emp_sl.count())

df_join_lnd_emp_sl=df_join_lnd_emp_sl.alias('glms_EmpDump').join(df_termination_dump,df_join_lnd_emp_sl.Local_HR_ID ==  df_termination_dump.TD_Employee_Number, 'left').select('glms_EmpDump.*','TD_Employee_Number','TD_Full_Name','TD_Function','TD_Employee_Subfunction','TD_Employee_Subfunction_1','TD_Cost_Centre','TD_Business_Category','TD_Client_Geography','TD_Location','TD_Position','TD_Job_Name','TD_email_Address','TD_File_Date')

# print("df_join_lnd_join_td",df_join_lnd_emp_sl.count())

# display(df_join_lnd_emp_sl.agg(sum('CPD_CPE_Hours_Awarded')))

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
# print("df_join_lnd join levelwise",df_join_lnd_emp_sl_lw.count())

# COMMAND ----------

# DBTITLE 1,Join with training Category dimensional tables
#Join with Training category data to get Training category column
df_join_lnd_joined = df_join_lnd_emp_sl_lw.join(df_training_category, lower(df_join_lnd_emp_sl_lw.Item_Title_decode) == lower(df_training_category.ItemTitle), 'left')
# print("df_join_lnd_joined training category",df_join_lnd_joined.count())
df_join_lnd_joined=df_join_lnd_joined.drop("ItemTitle")

# COMMAND ----------

# DBTITLE 1,GLMS Training Category
# Leadership (Techno functional + Performance enablement)
#commenting the leadership category as suggested by L&D team
df_join_lnd_joined=df_join_lnd_joined.withColumn('TrainingCategory',when(lower(col('TrainingCategory')).contains('performance enablement'),'Enabling').otherwise(col('TrainingCategory')))
#.when(col('lw_Mapping').contains('AD and Above') & lower(col('TrainingCategory')).contains('enabling'),'Leadership')\


# COMMAND ----------

# DBTITLE 1,Added on 02/21 - Null item title to be updated as TBD and its Training Category to be updated as Technology
df_join_lnd_joined=df_join_lnd_joined.withColumn('Item_Title',when(col('Item_Title').isNull(),'TBD').otherwise(col('Item_Title')))                                     

# COMMAND ----------

df_join_lnd_joined=df_join_lnd_joined.withColumn('TrainingCategory',when(col('Item_Title')=='TBD','Technology/ Techno-functional').otherwise(col('TrainingCategory')))

# COMMAND ----------

# display(df_join_lnd_joined.select('TrainingCategory','lw_Mapping').where(col('lw_Mapping')=='AD and Above').distinct())
#display(df_join_lnd_joined.select('TrainingCategory').distinct())

# COMMAND ----------

# DBTITLE 1,Deriving Training Category for KVA - 11/06/2023
total_hours=df_join_lnd_joined.select(sum(df_join_lnd_joined.KVA_CPE_Hours)).rdd.flatMap(lambda x: x).collect()
total_hours=total_hours[0]

enabling_hours=((total_hours * 40)/100)

df_join_lnd_joined=df_join_lnd_joined.withColumn('cumsum',f.sum(col('KVA_CPE_Hours')).over(Window.partitionBy().orderBy().rowsBetween(-sys.maxsize, 0)))

df_join_lnd_joined=df_join_lnd_joined.withColumn('TrainingCategory',when((df_join_lnd_joined.Source_Type == 'KVA') & (df_join_lnd_joined.cumsum<=enabling_hours),'Enabling').when((df_join_lnd_joined.Source_Type == 'KVA') & (df_join_lnd_joined.cumsum>enabling_hours),'Technology/ Techno-functional').otherwise(df_join_lnd_joined.TrainingCategory))

df_join_lnd_joined=df_join_lnd_joined.drop('cumsum','KVA_CPE_Hours')

# display(df_join_lnd_joined.select(max(df_join_lnd_joined.cumsum))) #when(df_join_lnd_joined.Source_Type == 'KVA'

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

# print(df_join_lnd_joined.count())

# COMMAND ----------

## Fill Cost Centre from RM CC
df_join_lnd_joined=df_join_lnd_joined.withColumn('Cost_Centre',when(col('Cost_Centre').isNull(),col('RM_Cost_Centre')).otherwise(col('Cost_Centre')))

# COMMAND ----------

# DBTITLE 1,Derive Geo from Emp Dump | Termination Dump
df_join_lnd_joined=df_join_lnd_joined.withColumn('Geo',when(col('Emp_Employee_Number').isNotNull() & col('TD_Employee_Number').isNull(),col('Emp_Client_Geography'))\
.when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNull(),col('TD_Client_Geography'))\
                                                 .when(col('TD_Employee_Number').isNotNull() & col('Emp_Employee_Number').isNotNull(),col('Emp_Client_Geography'))\
                                                 .otherwise(None))

# COMMAND ----------

## Fill Geo from RM Geo
df_join_lnd_joined=df_join_lnd_joined.withColumn('Geo',when(col('Geo').isNull(),col('RM_Client_Geography')).otherwise(col('Geo')))

# COMMAND ----------

# DBTITLE 1,Join with Cost center business unit table to get BU
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_cost_center_business_unit,lower(df_join_lnd_joined.Cost_Centre) == lower(df_config_cost_center_business_unit.Cost_centre),'left').select('df_join_lnd_joined.*','Final_BU')

df_join_lnd_joined=df_join_lnd_joined.withColumnRenamed('Final_BU','BU')
# print("join cc bu",df_join_lnd_joined.count())

# COMMAND ----------

# display(df_join_lnd_joined.filter(col("Cost_Centre").isin("Capability Hubs-KM-GCM-GET","Capability Hubs-Research-GCM-Sectors","Tax-Indirect Tax","Capability Hubs-KM-GCM-Sectors")).groupBy("Cost_Centre").agg(sum(col('CPD_CPE_Hours_Awarded'))))

# COMMAND ----------

# DBTITLE 1,Join with Dim Global Function table on BU to get global function of Employee
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_dim_global_function,lower(df_join_lnd_joined.BU)==lower(df_config_dim_global_function.Final_BU),'left').select('df_join_lnd_joined.*','GlobalFunction')
# print("join global function",df_join_lnd_joined.count())

# COMMAND ----------

# DBTITLE 1,Join with CC BU SL to get SL
df_join_lnd_joined=df_join_lnd_joined.alias('df_join_lnd_joined').join(df_config_cc_bu_sl,(lower(df_join_lnd_joined.Cost_Centre) == lower(df_config_cc_bu_sl.Cost_centre)) & (lower(df_join_lnd_joined.Geo) == lower(df_config_cc_bu_sl.Client_Geography)) & (lower(df_join_lnd_joined.BU) == lower(df_config_cc_bu_sl.Final_BU)),'left').select('df_join_lnd_joined.*','Service_Line')
# print("join cc bu sl",df_join_lnd_joined.count())

# display(df_join_lnd_joined.filter(col('BU').isNull()).select(col('Cost_centre')).distinct())

# display(df_join_lnd_joined.groupBy(col('Item_Title'),col('Local_HR_ID')).agg(count(col('Item_Title'),col('Local_HR_ID'))).show())

# COMMAND ----------

# Commented below snippet on 11/6/2023 as KVA data is flowing through GLMS and this is already handled above
# .withColumn("Training_Category",when(upper(col("Item_Title")).contains("KVA"), "Enabling")\
#             .when(lower(col("Item_Title")).contains("linkedin learning"), "Enabling")\
#             .when(lower(col("Item_Title")).contains("linkedin"), "Enabling")\
#             .when(lower(col("Item_Title")).contains("performance enablement"), "Enabling")\
#             .otherwise(col("TrainingCategory")))\

df_lnd_transformed = df_join_lnd_joined\
.withColumn("Training_Category",df_join_lnd_joined["TrainingCategory"])\
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
.withColumn("Item_Type_Category",when(col("Item_Type") == "ILT","In-person").when(col("Item_Type").isin('VLT','HYBRID','EXT','CONF'),"Virtual").otherwise("Self Paced"))\
.withColumn("Local_Function", regexp_replace(col("Local_Function"), "IN_", ""))\
.withColumn("ORG_ID", regexp_replace(col("ORG_ID"), "KGS_", ""))\
.withColumn("Local_HR_ID",df_join_lnd_joined["Local_HR_ID"].cast(StringType()))\
.withColumn("Year",lit(year))\
.withColumn("Month",lit(month))\
.withColumn("GLMS_Dated_on",lit(mydate))\
.withColumn("File_Type",df_join_lnd_joined["Source_Type"])

df_lnd_transformed=df_lnd_transformed.drop('Source_Type')
df_join_lnd_joined=df_join_lnd_joined.drop('Source_Type')

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

badDf=finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNull() & col('Emp_TD_Email_Address').isNull()) | (col('Email_Address__User_') == 'kpmgindiaadmin@skillton.degreed.com') | (col('Position').isNull()) | (trim(col('Position'))== '') | col('Cost_centre').isNull() | (trim(col('Cost_centre'))=='')) 

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

# Commenting and changing logic on 8/14/2023 based on confirmation from Pooja

# finalDf=finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNotNull() & col('Emp_TD_Email_Address').isNotNull()) & (coalesce(col('Email_Address__User_'),lit('NA')) != 'kpmgindiaadmin@skillton.degreed.com') & (col('Position').isNotNull()) & (trim(coalesce(col('Position'),lit('')))!= '') & col('Cost_centre').isNotNull() & (trim(coalesce(col('Cost_centre'),lit('')))!='')) 

finalDf=finalDf.select('*').filter((col('Emp_TD_Employee_Number').isNotNull() & col('Emp_TD_Email_Address').isNotNull()) & (coalesce(col('Email_Address__User_'),lit('NA')) != 'kpmgindiaadmin@skillton.degreed.com'))

finalDf=finalDf.drop(*drop_cols)

# COMMAND ----------

finalDf = finalDf.dropDuplicates()
# display(finalDf.groupBy("BU").agg(sum(col('CPD_CPE_Hours_Awarded'))))
# display(finalDf.agg(sum(col('CPD_CPE_Hours_Awarded'))))

# COMMAND ----------

badDfCount = badDf.count()

if int(badDfCount) > 0:
    badDf=colcaststring(badDf,badDf.columns)
    badDf = badDf.withColumn("Dated_On", badDf["Dated_On"].cast(TimestampType()))

    badDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true") \
    .option("path",bad_filepath_url+processName+"/"+tableName+"_bad") \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb_badrecords.trusted_hist_"+ processName + "_" + tableName+"_bad")

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