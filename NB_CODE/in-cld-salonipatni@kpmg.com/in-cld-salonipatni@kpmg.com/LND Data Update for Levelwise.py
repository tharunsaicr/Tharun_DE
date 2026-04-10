# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- select count(*) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where file_date in  ('20230201','20230301','20230401')  --
# MAGIC
# MAGIC -- this should return 0
# MAGIC
# MAGIC  
# MAGIC
# MAGIC select distinct file_type from kgsonedatadb.trusted_hist_lnd_glms_kva_details where file_date in  ('20230228','20230331','20230430')---
# MAGIC
# MAGIC -- should return GLMS and KVA

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details --2646294
# MAGIC

# COMMAND ----------

# jdbcHostname = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseHostName")  
# jdbcDatabase = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseName")  
# jdbcPort = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databasePort")  
# username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNameUserName")  
# password = password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNamePassword")  
# jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)  
# connectionProperties = {  
#   "user" : username,  
#   "password" : password,  
#   "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"  
# }  

# hist_table = "trusted_hist_lnd_glms_kva_details"
# sampleDF=spark.sql('''select Item_ID,Training_Category,Item_Title,KBS_Classification_,Item_Type,Assignment_Type,Security_Domain__Item_,CPD_CPE_Hours__Item_,Item_Revision_Date,Class_ID,Scheduled_Offering_Start_Date,SO_Item_Start_Time,Class_End_Date,Scheduled_Offering_End_Time,Completion_Date,Period,Completion_Time,Completion_Status_ID,Completion_Status_Description,Total_Hours,Credit_Hours,Contact_Hours,CPD_CPE_Hours_Awarded,Primary_Instructor,Last_Update_User_ID,Last_Update_Timestamp,Last_Update_Time,Comments,Local_HR_ID,First_Name,Last_Name,Middle_Name,Email_Address__User_,User_ID,Account_ID,User_Status,Job_Code_ID,Job_Code_Description,Job_Title,Position,Job_Name,Level_Wise,Country_Region,Office_Location_ID,Department,Global_Function,Organisation_Description,CC,BU,Subfunction,Employee_Class_Description,Local_Job_Level,Is_Client_Facing,Manager_Email_Address,Last_Hire_Date,Local_Function,Local_Service_Line,Cost_Center,ORG_ID,Geo,SL,Location,Item_Type_Category,Full_Name,Quarter,Original_Levelwise_Mapping,File_Type,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+hist_table)

# sampleDF.write \
# .format("jdbc") \
# .mode("append") \
# .option("url",jdbcUrl) \
# .option("dbtable",hist_table) \
# .option("user", username) \
# .option("password", password) \
# .save()
    

# COMMAND ----------

from pyspark.sql.functions import lit, col, from_unixtime, unix_timestamp, regexp_replace, when, upper, lower, split, isnull, concat
from datetime import datetime,timedelta
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %run /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

testDf = spark.sql("select * from kgsonedatadb.ln_test_1 ")
display(testDf)

# COMMAND ----------


testDf = testDf.withColumn("New_Postion", when(testDf.ED_Postion.isNull() & testDf.TD_Postion.isNotNull(), col("TD_Postion"))\
    .when(testDf.TD_Postion.isNull() & testDf.ED_Postion.isNotNull(), col("ED_Postion"))\
    .otherwise(None)
    )

testDf = testDf.withColumn("New_Job_Name", when(testDf.ED_Job_Name.isNull() & testDf.TD_Job_Name.isNotNull(), col("TD_Job_Name"))\
    .when(testDf.TD_Job_Name.isNull() & testDf.ED_Job_Name.isNotNull(), col("ED_Job_Name"))\
    .otherwise(None)
    )

testDf.display()

# COMMAND ----------

levelwiseDf = spark.sql("select * from kgsonedatadb.config_dim_level_wise")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from  kgsonedatadb.config_dim_level_wise where Position like '%Partner COO%'
# MAGIC
# MAGIC -- in ('Associate 2',
# MAGIC -- 'Technical Manager','Operations Analyst',
# MAGIC -- 'Associate Partner-KGSnGDC',
# MAGIC -- 'Partner COO',
# MAGIC -- 'Associate Partner - Finance',
# MAGIC -- 'Manager',
# MAGIC -- 'Associate Partner - COO'
# MAGIC -- )

# COMMAND ----------

def rename_emp_td_columns(df):
    for c in df.columns:
        df=df.withColumnRenamed(c,"JD_"+c)
    return df

# COMMAND ----------

joinDf = testDf.join(levelwiseDf,(testDf.New_Postion == levelwiseDf.Position) & (testDf.New_Job_Name == levelwiseDf.Job_Name),"left" )

# joinDf = testDf.join(levelwiseDf,testDf.New_Postion == levelwiseDf.Position,"left")

joinDf = rename_emp_td_columns(joinDf)
display(joinDf)

# COMMAND ----------

# joinDf = joinDf.select("JD_Local_HR_ID","JD_New_Job_Name","JD_New_Job_Name","JD_Mapping")

# joinDf.select("JD_New_Postion","JD_New_Job_Name").filter(joinDf.JD_Mapping.isNull()).distinct().display()

# COMMAND ----------

df = spark.sql("select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details")

# COMMAND ----------

findalDf = df.join(joinDf, df.Local_HR_ID == joinDf.JD_Local_HR_ID, "left")
findalDf.display()

# COMMAND ----------

findalDf.select("Level_Wise").distinct().display()

# COMMAND ----------

findalDf.count()

# COMMAND ----------

findalDf = findalDf.withColumn("New_Levelwise",when(findalDf.Level_Wise.isNull() & findalDf.JD_Mapping.isNotNull(), col("JD_Mapping"))\
    .otherwise(col("Level_Wise")))

# COMMAND ----------

findalDf = findalDf.withColumn("Level_Wise",when(findalDf.Level_Wise.isNull(),col("New_Levelwise")).\
    otherwise(col("Level_Wise")))

# COMMAND ----------

findalDf = findalDf.drop('JD_Local_HR_ID','JD_ED_Postion','JD_ED_Job_Name','JD_TD_Postion','JD_TD_Job_Name','JD_New_Postion','JD_New_Job_Name','JD_Position','JD_Job_Name','JD_Mapping','JD_Dated_On','New_Levelwise')

# COMMAND ----------

findalDf.count()

# COMMAND ----------

findalDf.printSchema()

# COMMAND ----------

# findalDf.select("Local_HR_Id","Level_Wise").filter(col("Level_Wise").isNull()).distinct().display()

# COMMAND ----------

processName = "lnd"
tableName ="glms_kva_details"

# COMMAND ----------

# findalDf.write \
# .mode("overwrite") \
# .format("delta") \
# .option("overwriteSchema", "True") \
# .option("path",raw_stg_savepath_url+processName+"/"+tableName) \
# .option("compression","snappy") \
# .saveAsTable("kgsonedatadb.raw_stg_"+ processName + "_" +tableName)



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select count(1) from kgsonedatadb.raw_stg_lnd_glms_kva_details
# MAGIC
# MAGIC select DISTINCT local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_wise is Null

# COMMAND ----------

# %sql

# create table kgsonedatadb.ln_test_glms_kva_details 
# select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert into kgsonedatadb.ln_test_glms_kva_details
# MAGIC -- select * from kgsonedatadb.ln_test_updated_Levelwise 
# MAGIC
# MAGIC select count(1) from kgsonedatadb.ln_test_glms_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Local_HR_ID, lw_Job_Name,lw_Position from kgsonedatadb.trusted_hist_lnd_glms_details 
# MAGIC where File_Date = '20221001' 

# COMMAND ----------

