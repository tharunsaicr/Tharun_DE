# Databricks notebook source
processName = "employee_engagement"
tableName = "rock"

# COMMAND ----------

import datetime

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

fileYear = FileDate[:4]
fileYear_FY = "FY"+FileDate[2:4]
prevFileYear = int(fileYear) - 1
fileMonth = FileDate[4:6]
fileDay = FileDate[6:]
convertedFileDate = fileYear+'-'+fileMonth+'-'+fileDay

convertedFileDate = datetime.datetime.strptime(convertedFileDate, '%Y-%m-%d').date()

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

#Include business rule in trasformation delta table if needed 
# currentDf.select('End_Date').show(currentDf.count())


# COMMAND ----------

#currentDf = spark.sql("select * from kgsonedatadb.raw_curr_"+processName + "_"+ tableName)

import pyspark
from pyspark.sql.functions import lit, col, split
import pyspark.sql.functions as F 
from pyspark.sql.functions import *

# df = spark.sql("select * from kgsonedatadb.trusted_headcount_employee_dump where Entity = 'KGS'")

df = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump A where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump B where B.Employee_Number=A.Employee_Number and to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"'"+")) ed_hist where rank = 1")

# df_config_cc_bu = spark.sql("select distinct Cost_centre,BU from kgsonedatadb.config_cost_center_business_unit")

df_config_cc_bu = spark.sql("select distinct Cost_centre,Final_BU from (select *,row_number() over(partition by Cost_centre,Final_BU order by Dated_On desc) as rownum from kgsonedatadb.config_hist_cc_bu_sl where File_Date = (select max(File_Date) from kgsonedatadb.config_hist_cc_bu_sl where to_date(File_Date,'yyyyMMdd') <= '"+str(convertedFileDate)+"')) where rownum = 1")

df = df.join(df_config_cc_bu,df.Cost_centre == df_config_cc_bu.Cost_centre,"left").select(df["*"],df_config_cc_bu["Final_BU"])

# df = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+FileDate+"'"+"))) ed_hist where rank = 1")

# df = df.withColumnRenamed('Employee_Subfunction','Sub_Function').withColumn("Gratuity_Date", F.to_date("Gratuity_Date", 'd-MMM-yy'))

df = df.withColumnRenamed('Employee_Subfunction','Sub_Function')

df = df.filter((round(months_between(lit(convertedFileDate),col("Gratuity_Date"))/lit(12),0)<=20))

# df =  df.withColumn("Rock_Tenure",(F.datediff(lit(convertedFileDate),col("Gratuity_Date"))/365))

df =  df.withColumn("Rock_Tenure", int(fileYear)-F.year(col("Gratuity_date")))

# display(df)

df = df[df['Employee_Category'].isin(['Secondee Inward', 'Awaiting Termination', 'Abscondee']) == False]

df = df.withColumn("Amount", when(df.Rock_Tenure == "5","15000")
                                 .when(df.Rock_Tenure == "10","50000")
                                 .when(df.Rock_Tenure == "15","75000")
                                 .when(df.Rock_Tenure == "20","100000")
                                 .when(df.Rock_Tenure.isNull() ,"")
                                 .otherwise("0"))

df = df.withColumn("Tenure_as_on", when(df.Rock_Tenure == "5",F.add_months(col("Gratuity_Date"),int(60)))
                                 .when(df.Rock_Tenure == "10",F.add_months(col("Gratuity_Date"),int(120)))
                                 .when(df.Rock_Tenure == "15",F.add_months(col("Gratuity_Date"),int(180)))
                                 .when(df.Rock_Tenure == "20",F.add_months(col("Gratuity_Date"),int(240)))
                                 .when(df.Rock_Tenure.isNull() ,"")
                                 .otherwise("0"))

df = df.withColumn("Tenure_Month",lit(fileMonth)).withColumn("Tenure_Year",lit(fileYear))

list =[5,10,15,20]
df = df.where( ( col("Rock_Tenure").isin (list)) & (F.month(col("Gratuity_Date")) == int(fileMonth)))

# display(df.select("Date_First_Hired","Gratuity_Date","Rock_Tenure","Tenure_Month","Tenure_Year","Tenure_as_on").filter(col("Employee_Number") == "30840"))

currentDf=df.select("Employee_Number","Full_Name","Function","Sub_Function","Cost_centre","Final_BU","Location","Position","Employee_Category","Gratuity_Date","Date_First_Hired","Email_Address","Company_Name","desired_LWD","Approved_LWD","RM_Employee_Name","Rock_Tenure","Amount","Tenure_Month","Tenure_Year","Tenure_as_on")

# COMMAND ----------

# Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")


# COMMAND ----------

currentDf=currentDf.withColumn("File_Date", lit(FileDate))

# COMMAND ----------

# display(currentDf)

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