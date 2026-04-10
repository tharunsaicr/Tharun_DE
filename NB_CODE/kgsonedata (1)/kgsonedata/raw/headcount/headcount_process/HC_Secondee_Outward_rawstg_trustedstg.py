# Databricks notebook source
dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
current_cutoff_date = dbutils.widgets.get("CurrentCutOffDate")

print(current_cutoff_date)

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from dateutil.parser import parse

processName = "headcount"
tableName = "secondee_outward"
currentdatetime = datetime.now()

# COMMAND ----------


# df_stg_ED = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address from kgsonedatadb.trusted_stg_headcount_employee_dump")

# df_stg_last_Secondee_Outward = spark.sql("select * from kgsonedatadb.trusted_hist_headcount_secondee_outward")
# # display(df_stg_ED.select("Status").distinct())

# df_stg_ED = df_stg_ED.join(df_stg_last_Secondee_Outward, df_stg_ED.Employee_Number == df_stg_last_Secondee_Outward.Employee_Number, "left").select(df_stg_ED["*"],df_stg_last_Secondee_Outward.Employee_Number.alias("Sec_Out_Emp_Num"))

# df_stg_SecondeeOutward = df_stg_ED.filter((df_stg_ED.Employee_Category == "Secondee-Outward-Without Pay") | ((upper(df_stg_ED.Employee_Category) == "SERVING NOTICE PERIOD") & (df_stg_ED.Sec_Out_Emp_Num.isNotNull())))

# COMMAND ----------

# df_config_bu_cc = spark.sql("select * from kgsonedatadb.config_cost_center_business_unit")

# df_stg_ED = df_stg_ED.join(df_config_bu_cc,df_stg_ED.Cost_centre == df_config_bu_cc.Cost_centre,"left").select(df_stg_ED["*"],df_config_bu_cc["BU"])

# COMMAND ----------

# Commented on 5/19/2023
# df_stg_ED = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,File_Date from kgsonedatadb.trusted_stg_headcount_employee_dump where Entity != 'KI'")

df_stg_ED = spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,File_Date from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump where Entity != 'KI' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+current_cutoff_date+"'"+"))) ed_hist where rank = 1")

df_config_bu_cc = spark.sql("select * from kgsonedatadb.config_cost_center_business_unit")

df_stg_ED = df_stg_ED.join(df_config_bu_cc,df_stg_ED.Cost_centre == df_config_bu_cc.Cost_centre,"left").select(df_stg_ED["*"],df_config_bu_cc["BU"])

df_stg_last_Secondee_Outward = spark.sql("select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_secondee_outward where file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_secondee_outward where to_date(File_Date,'yyyyMMdd') < to_date("+"'"+current_cutoff_date+"'"+"))) last_secondee_outward where rank = 1")
df_stg_last_Secondee_Outward = df_stg_last_Secondee_Outward.drop("rank")
# display(df_stg_ED.select("Status").distinct())

df_stg_ED = df_stg_ED.join(df_stg_last_Secondee_Outward, df_stg_ED.Employee_Number == df_stg_last_Secondee_Outward.Employee_Number, "left").select(df_stg_ED["*"],df_stg_last_Secondee_Outward.Employee_Number.alias("Sec_Out_Emp_Num"))

df_stg_SecondeeOutward = df_stg_ED.filter((df_stg_ED.Employee_Category == "Secondee-Outward-Without Pay") | ((upper(df_stg_ED.Employee_Category) == "SERVING NOTICE PERIOD") & (df_stg_ED.Sec_Out_Emp_Num.isNotNull())))



# COMMAND ----------

# display(df_stg_SecondeeOutward)

# COMMAND ----------

df_stg_SecondeeOutward = df_stg_SecondeeOutward.withColumn("Date_First_Hired", df_stg_SecondeeOutward["Date_First_Hired"].cast(DateType()))

df_stg_SecondeeOutward = df_stg_SecondeeOutward.drop("Sec_Out_Emp_Num")
# display(df_stg_SecondeeOutward)

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
df_stg_SecondeeOutward = df_stg_SecondeeOutward.withColumn("Dated_On",lit(currentdatetime))

df_stg_SecondeeOutward.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",trusted_stg_savepath_url+"/"+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/trusted/maker_checker_validation",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})