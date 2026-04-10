# Databricks notebook source
###FACTS###


# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "RawDeltaTableName", defaultValue = "")
RawDeltaTableName = dbutils.widgets.get("RawDeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "TrustedTableName", defaultValue = "")
tableName = dbutils.widgets.get("TrustedTableName")

dbutils.widgets.text(name = "ReportName", defaultValue ="")
ReportName = dbutils.widgets.get("ReportName")

print("RawDeltaTableName   :"+ RawDeltaTableName)
print("tableName  :"+tableName)
print("processName:"+ processName)
print("ReportName :"+ ReportName)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

# DBTITLE 1,Call Connection Module
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Cal Common Components Module
# MAGIC %run /kgsfinance/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Read data from raw layer
#Load data for raw layer
raw_curr_dim=finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+RawDeltaTableName
print(raw_curr_dim)
currentDf= spark.read.format("delta").load(raw_curr_dim)
display(currentDf)

# COMMAND ----------

# currentDf=currentDf.filter((~col("DATA").like("%#MISSING%")) & (~col("DATA").like("0")))
# display(currentDf)

# COMMAND ----------

accounts_list=['Base Compensation','Bonus','Company Transport','Gratuity and Leave Encashment','Personnel Cost - Others','Professional Fees - Secondee','Professional Fees- Loaned Staff','Recruitment Cost','Relocation Bonus','Stewardship cost','Base Compensation Additional','Bonus Additional','Company Transport Additional','Contractors cost','Gratuity and Leave Encashment Additional','Insurance premium','Internet Allowance','New Joiners Chargeable','New Joiners Support','Professional Fees- Loaned Staff Additional','Recruitment Cost additional','Retention Bonus','Revenue Generating BackFill','Secondee Salary','Shift Allowance','Stewardship cost Additional','Support BackFill','WAH Allowance','Personnel Cost - Others Additional','Background Verification Charges','Direct Cost','Employee Referral Cost','Joining Bonus','Joining Bonus Additional','Placement Agency Cost','Relocation Bonus Additional','RPO Agency','CMT Cost','Communication Cost','CSR','Database','Facilities and Seat cost','Management Allocation','Mark-up on Total Costs','Mark-up on Total Costs Additional','Miscellaneous','OMT Cost','Printing & Stationery','Professional Fees - Other','Shared Services','Staff Welfare - Offsite','Staff Welfare - Others','Training','Transformations','Actuarial Valuation LE and Gratuity','Audit Expenses','CMT Cost Additional','Enabling Training','Encore','Facilities and Seat cost Additional','GTP Bonus','Legal fees','Management Allocation Additional','Marketing Budget','PC Event','PC Event Additional','Professional Fees - Other Additional','R&R','Service tax refund related costs','Shared Services Tax','Staff Welfare - Offsite Additional','Staff Welfare - Others Additional','Subscriptions','Subscriptions_SL','Technical Training','TP Assessment related','TP Assessment related  - Accountants report','Training Cost Additional','Communication Cost Additional','Database Additional','Miscellaneous Additional','Other Misc','Printing & Stationery Additional','Shared Services Additional','Transformations Additional','All Year Meal Allowance','Fees and subscription','Secretarial Audit fees','Staff Welfare Client Visit','Staff Welfare CSR & ID','Staff Welfare EE','Staff Welfare TL','IGDC Service Cost','Subsistence and Travel Cost','BU Specific IT','IT Project Spend','Subsistence and Travel Cost Additional','IGDC Service Cost Additional']
currentDf = currentDf.withColumn("ACCOUNT_TYPE_FLAG", when(currentDf["ACCOUNT"].isin(accounts_list), "COST") \
                     .when(currentDf["Account"].contains("Hour"),"Hour") \
                     .when(currentDf["Account"].contains("FTE"),"FTE").when(currentDf["Account"].contains("Headcount"),"HC").when(currentDf["Account"].contains("HC"),"HC") \
                     .otherwise(None)) 

# COMMAND ----------

display(currentDf)

# COMMAND ----------

if tableName=="hcplan_forecast_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("forecast_hcplan_data_extract")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="opex_forecast_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("opex_hcplan_data_extract")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="projects_forecast_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("projects_hcplan_data_extract")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="hcplan_actuals_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("hcplan_actuals_pbi_report")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="opex_actuals_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("opex_actuals_pbi_report")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="projects_actuals_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("projects_actuals_pbi_report")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="hcplan_plan_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("hcplan_plan_pbi_report")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="opex_plan_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("opex_plan_pbi_report")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="projects_plan_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("projects_plan_pbi_report")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="hcplan_outlook_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("hcplan_outlook_pbi_report")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="opex_outlook_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("opex_outlook_pbi_report")) \
                         .withColumn("Source",lit("Excel"))
if tableName=="projects_outlook_pbi_report":
    currentDf = currentDf.withColumn("Scenario", when(currentDf["Scenario"].contains("Actual"), "Outlook1_Actual").otherwise(currentDf["Scenario"])) \
                         .withColumn("File_Name",lit("projects_outlook_pbi_report")) \
                         .withColumn("Source",lit("Excel"))

                        

# COMMAND ----------

display(currentDf)

# COMMAND ----------

# display(currentDf.filter(currentDf["Scenario"].contains("Actual")))

# COMMAND ----------

#change col type
currentDf = currentDf.withColumn("DATA", currentDf.DATA.cast("double")) \
                     .withColumn("Dated_On", to_timestamp(currentDf['Dated_On'],'yyyy-MM-dd HH:mm:ss'))
display(currentDf)



# COMMAND ----------

 # Replace empty value with None and drop null rows
from pyspark.sql.functions import col,when

currentDf=currentDf.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in currentDf.columns])
currentDf = currentDf.dropna("all")

# COMMAND ----------

# extracting number of rows from the Dataframe
row = currentDf.count()
print("Row ",row)

# extracting number of columns from the Dataframe
column = len(currentDf.columns)
print("Column ",column)

# COMMAND ----------

#trusted layer table name
saveTableName = "kgsfinancedb.trusted_curr_"+ReportName+"_"+processName + "_"+ tableName
print(saveTableName)
print("path          :",finance_raw_curr_savepath_url+ReportName+"/"+processName+"/"+tableName)

# COMMAND ----------

# DBTITLE 1,Load data to trusted current table
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",finance_trusted_curr_savepath_url+ReportName+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsfinancedb.trusted_curr_"+ReportName+"_"+ processName + "_" +tableName)

# COMMAND ----------

curr_table_name= saveTableName
hist_table_name="kgsfinancedb.trusted_hist_"+ReportName+"_"+processName+"_"+tableName
print(curr_table_name)
print(hist_table_name)

# COMMAND ----------

# DBTITLE 1,Load data to trusted history table
if(spark._jsparkSession.catalog().tableExists(hist_table_name)):
    dbutils.notebook.run("/kgsfinance/trusted/trusted_to_trusted_del_load",6000,{'curr_table_name':curr_table_name,'hist_table_name':hist_table_name,'tableName':tableName,'processName':processName,'ReportName':ReportName})
else:
    print("Creating Curr & Hist tables on trusted layer")
    dbutils.notebook.run("/kgsfinance/trusted/trustedcurr_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})


# COMMAND ----------

# DBTITLE 1,Delta To SQL Load
dbutils.notebook.run("/kgsfinance/trusted/Delta to SQL Load_Hyperion_DG",6000,{'DeltaTableName':tableName,'ProcessName':processName,'ReportName':ReportName})

# COMMAND ----------

