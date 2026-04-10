# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "SourceFile", defaultValue = "")
sourceFile = dbutils.widgets.get("SourceFile")

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

print(tableName)
print(processName)
print(sourceFile)
print(FileDate)

# COMMAND ----------

# MAGIC %run /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

import datetime
fileYear = FileDate[:4]

fileMonth = FileDate[4:6]

fileDay = FileDate[6:]

convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay

print(convertedFileDate)

convertedFileDate = datetime.datetime.strptime(convertedFileDate, '%Y-%m-%d').date()

# COMMAND ----------

from pyspark.sql.functions import lit, col, from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Load data from  GLMS
if (sourceFile =="glms_details"):
    if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_lnd_glms_details')):
        df_trusted_curr_lnd_glms = spark.sql("select Item_ID,Training_Category,Item_Title,KBS_Classification_,Item_Type,Assignment_Type,Security_Domain__Item_,CPD_CPE_Hours__Item_,Item_Revision_Date,Class_ID,Scheduled_Offering_Start_Date,SO_Item_Start_Time,Class_End_Date,Scheduled_Offering_End_Time,Completion_Date,Period,Completion_Time,Completion_Status_ID,Completion_Status_Description,Total_Hours,Credit_Hours,Contact_Hours,CPD_CPE_Hours_Awarded,Primary_Instructor,Last_Update_User_ID,Last_Update_Timestamp,Last_Update_Time,Comments,Local_HR_ID,First_Name,Last_Name,Middle_Name,Email_Address__User_,User_ID,Account_ID,User_Status,Job_Code_ID,Job_Code_Description,Job_Title,Position,Job_Name,Level_Wise,Country_Region,Office_Location_ID,Department,GlobalFunction as Global_Function,Organisation_Description,CC,BU,Subfunction,Employee_Class_Description,Local_Job_Level,Is_Client_Facing,Manager_Email_Address,Last_Hire_Date,Local_Function,Local_Service_Line,Cost_Centre as Cost_Center,ORG_ID,Geo,Service_Line as SL,Location,Item_Type_Category,' ' as Full_Name,Quarter,Mapping_Original as Original_Levelwise_Mapping,File_Type,File_Date from (select rank() over(partition by file_date order by file_date desc) as rank, * from kgsonedatadb.trusted_lnd_glms_details where file_date = (select max(file_date) from kgsonedatadb.trusted_lnd_glms_details where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")

        df_glms_kva=df_trusted_curr_lnd_glms

# COMMAND ----------

# DBTITLE 1,Load data from  KVA
if (sourceFile =="kva_details"):
    if (spark._jsparkSession.catalog().tableExists('kgsonedatadb', 'trusted_hist_lnd_kva_details')):
        df_trusted_curr_lnd_kva = spark.sql("SELECT '' AS Item_ID,Training_Category,Content_Title AS Item_Title,'' as KBS_Classification_,Content_Type as Item_Type,'' as Assignment_Type,'' as Security_Domain__Item_,'' AS CPD_CPE_Hours__Item_,'' as Item_Revision_Date,'' as Class_ID,'' as Scheduled_Offering_Start_Date,'' as SO_Item_Start_Time,'' as Class_End_Date,'' as Scheduled_Offering_End_Time,Completion_Date,Period,'' as Completion_Time ,'' as Completion_Status_ID,'' as Completion_Status_Description,'' AS Total_Hours,'' AS Credit_Hours,'' AS Contact_Hours,CPE_Hours AS CPD_CPE_Hours_Awarded,'' as Primary_Instructor,'' as Last_Update_User_ID,'' as Last_Update_Timestamp,'' as Last_Update_Time,'' as Comments,Employee_Id AS Local_HR_ID,Employee_Name AS First_Name,'' AS Last_Name,'' AS Middle_Name,Organization_Email AS Email_Address__User_,'' as User_ID,'' as Account_ID,'' as User_Status,'' as Job_Code_ID,'' as Job_Code_Description,Position AS Job_Title,Position,Job_Name,Level_Wise,'' as Country_Region,'' as Office_Location_ID,'' as Department,Global_Function,'' as Organisation_Description,Cost_Centre as CC,BU,'' as Subfunction,'' AS Employee_Class_Description,'' as Local_Job_Level,'' as Is_Client_Facing,'' as Manager_Email_Address,'' as Last_Hire_Date,'' as Local_Function,Service_Line as Local_Service_Line,'' as Cost_Center,'' as ORG_ID,Geo,Service_Line as SL,Location,Item_Type_Category,'' as Full_Name,Quarter,Mapping_Original as Original_Levelwise_Mapping,File_Type,File_Date from (select rank() over(partition by file_date order by file_date desc) as rank, * from kgsonedatadb.trusted_hist_lnd_kva_details where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_lnd_kva_details where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) hist where rank = 1")
                                         
        df_glms_kva=df_trusted_curr_lnd_kva.withColumn("Item_Revision_Date",df_trusted_curr_lnd_kva.Item_Revision_Date.cast(DateType()))\
                                           .withColumn("Completion_Date",df_trusted_curr_lnd_kva.Completion_Date.cast(DateType()))
                                           
        display(df_glms_kva)

# COMMAND ----------

# DBTITLE 1,Load to raw_stg_lnd_glms_kva_details in Overwrite
df_glms_kva.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",raw_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.raw_stg_"+ processName + "_" +tableName)


# COMMAND ----------

# DBTITLE 1,Clean Up Script to remove duplicates, check for bad Record and Type Cast columns
layerName = 'trusted'

dbutils.notebook.run("/kgsonedata/raw/Data_Cleanup",6000, {'DeltaTableName':tableName, 'ProcessName':processName, 'LayerName':layerName})