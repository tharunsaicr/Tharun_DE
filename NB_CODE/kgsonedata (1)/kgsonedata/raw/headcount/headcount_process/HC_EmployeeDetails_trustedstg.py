# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from dateutil.parser import parse

processName = "headcount"
tableName = "employee_details"
currentdatetime = datetime.now()

# COMMAND ----------

# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
current_cutoff_date = dbutils.widgets.get("CurrentCutOffDate")

print(current_cutoff_date)

# COMMAND ----------

# DBTITLE 1,Select only required columns from trusted stg ED and form dataframe
#pending - select only required columns
df_stg_ED = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,Status,File_Date from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+current_cutoff_date+"'"+"))) ed_hist where rank = 1")


# COMMAND ----------

# display(df_stg_ED)

# COMMAND ----------

# DBTITLE 1,Add BU from BU-CC mapping
# %sql

df_config_bu_cc = spark.sql("select * from kgsonedatadb.config_cost_center_business_unit")

df_stg_ED = df_stg_ED.join(df_config_bu_cc,df_stg_ED.Cost_centre == df_config_bu_cc.Cost_centre,"left").select(df_stg_ED["*"],df_config_bu_cc["BU"])

# COMMAND ----------

# display(df_stg_ED)

# COMMAND ----------

# DBTITLE 1,Filter only employee details records based on condition given in SOP
# display(df_stg_ED.select("Status").distinct())
#Filter data as per Employee Details SOP
df_stg_EmployeeDetails = df_stg_ED.filter((df_stg_ED.Status == "HC") | (df_stg_ED.Status == "New Joiners") | (df_stg_ED.Status == "KI - KGS"))

# COMMAND ----------

# display(df_stg_EmployeeDetails.select("Status").distinct())

# COMMAND ----------

# DBTITLE 1,Organization_Name to Operating Unit & Client Geography validation
df_stg_EmployeeDetails = df_stg_EmployeeDetails.withColumn("Remarks", lit(""))

df_stg_EmployeeDetails = df_stg_EmployeeDetails.withColumn("Remarks", when(upper(df_stg_EmployeeDetails.Organization_Name) != upper(concat(regexp_replace(df_stg_EmployeeDetails.Operating_Unit,"-"," "),lit("."),df_stg_EmployeeDetails.Client_Geography)),concat(df_stg_EmployeeDetails.Remarks,lit("Organization_name mismatching operating unit and client geography"))).otherwise(df_stg_EmployeeDetails.Remarks))

# print(df_stg_EmployeeDetails.count())

# display(df_stg_EmployeeDetails.select(upper(df_stg_EmployeeDetails.Organization_Name), upper(concat(regexp_replace(df_stg_EmployeeDetails.Operating_Unit,"-"," "),lit("."),df_stg_EmployeeDetails.Client_Geography))).distinct())

# COMMAND ----------

# DBTITLE 1,Validate all columns for Null or NA

# for columnName in df_stg_EmployeeDetails.columns:

#     if (columnName != "Remarks"):

#          df_stg_EmployeeDetails = df_stg_EmployeeDetails.withColumn("Remarks", when(((col(columnName) == " ") | (col(columnName) == "NA") | (col(columnName).isNull()) | (col(columnName) == "") | (col(columnName) == "#N/A")),concat(df_stg_EmployeeDetails.Remarks,lit(" | " + columnName + " is null or NA"))).otherwise(df_stg_EmployeeDetails.Remarks))

# display(df_stg_EmployeeDetails.select("Remarks").distinct())

# COMMAND ----------

# import pyspark.sql.functions as F

# def check_null_or_na_all_rows(df):

#    for col_name in df.columns:
#     df = df.withColumn("Remarks", when((col[col_name].isnull() | col[col_name] == "NA"),concat(df.Remarks,col[col_name],lit(" is null or NA"))).otherwise(df.Remarks))


#    return df

# COMMAND ----------

# check_null_or_na_all_rows(df_stg_EmployeeDetails)

# display(df_stg_EmployeeDetails.select("Remarks").distinct())

# COMMAND ----------

print(df_stg_EmployeeDetails.count())

# COMMAND ----------

df_stg_EmployeeDetails.createOrReplaceTempView("df_stg_EmployeeDetails_TempView")
df_stg_EmployeeDetails = spark.sql("select * from df_stg_EmployeeDetails_TempView where Employee_Number not in (select Employee_Number from (select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_secondee_outward where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_secondee_outward where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+current_cutoff_date+"'"+"))) ed_hist where rank = 1) trusted_hist_headcount_secondee_outward)")

# COMMAND ----------

# print(df_stg_EmployeeDetails.count())

# COMMAND ----------

#Drop additional columns like Status
df_stg_EmployeeDetails = df_stg_EmployeeDetails.drop("Status")
# display(df_stg_EmployeeDetails)

#Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
df_stg_EmployeeDetails = df_stg_EmployeeDetails.withColumn("Dated_On",lit(currentdatetime))

#added on 11/16/2022 - temporary fix to remove duplicates caused by dups in talentkonnectresignationfile
df_stg_EmployeeDetails = df_stg_EmployeeDetails.dropDuplicates()
########

df_stg_EmployeeDetails.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/trusted/maker_checker_validation",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})