# Databricks notebook source
import datetime

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
current_cutoff_date = dbutils.widgets.get("CurrentCutOffDate")

current_cutoff_date = datetime.datetime.strptime(current_cutoff_date, '%Y-%m-%d').date()

currentMonth = current_cutoff_date.strftime("%B")
currentMonthLeft = currentMonth + " Left"

tableName ="contingent_worker_resigned"
processName = "headcount"

print(currentMonthLeft)

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse
from pyspark.sql.functions import concat, concat_ws, lit, col, trim

# COMMAND ----------

finalDf = spark.sql("select * from (select rank() over(partition by candidate_id,file_date order by dated_on desc) as rank, * from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent where file_date = (select max(file_date) from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+str(current_cutoff_date)+"'"+"))) C_hist where rank = 1")

# COMMAND ----------

# display(finalDf.select("Status").distinct())

# COMMAND ----------

# display(finalDf.filter(col("Status").like("%Left%")))

# COMMAND ----------

# DBTITLE 1, Current month terminated - left cases (to be moved to Loaned Contingent Resigned tab in Final HC)
finaldf = finalDf.filter(finalDf.Status == currentMonthLeft)
# display(finaldf)

# COMMAND ----------

# DBTITLE 1,Derive function, subfunction, subfunction1
mappingDf = spark.sql("select * from hive_metastore.kgsonedatadb.config_cost_center_function_subfunction")
mappingDf = mappingDf.withColumnRenamed("Cost_centre","Mapping_Cost_centre") 
# display(mappingDf)

joinedDf = finaldf.join(mappingDf,finaldf.Cost_Centre == mappingDf.Mapping_Cost_centre,"left")
joinedDf = joinedDf.drop("Mapping_Cost_centre")
# display(joinedDf)

# COMMAND ----------

# DBTITLE 1,Derive Organization Name
joinedDf = joinedDf.withColumn("Organization_Name", concat(col('Operating_Unit'),lit("."),col('Client_Geography')))
# display(joinedDf)

# COMMAND ----------

# DBTITLE 1,Deriving BU
costcenterBUdf = spark.sql("select * from hive_metastore.kgsonedatadb.config_cost_center_business_unit")
costcenterBUdf = costcenterBUdf.withColumnRenamed("Cost_centre","Mapping_Cost_centre").drop("Dated_On","File_Date")
# display(costcenterBUdf)

joinedTDDf = joinedDf.join(costcenterBUdf,joinedDf.Cost_Centre == costcenterBUdf.Mapping_Cost_centre,"left")
# display(joinedDf)

finaldf = joinedTDDf.drop("Mapping_Cost_centre")
# display(finaldf)


# COMMAND ----------

finaldf = finaldf.withColumnRenamed("Business_CategoryX","Business_Category")

# COMMAND ----------

# DBTITLE 1,Select required columns
finaldf = finaldf.select("Candidate_Id","Full_Name","Function","Employee_Subfunction","Employee_Subfunction_1","Organization_Name","Cost_centre","Operating_Unit","Client_Geography","User_Type","Business_Category","Location","Sub_Location","Job","Position","People_Group","Date_of_Joining","Gender","Company","Subcontract_Agency","LWD","Official_Email_ID","Assignment_Category","BU","File_Date")
# display(finaldf)


# COMMAND ----------

# DBTITLE 1,drop duplicates
finaldf = finaldf.dropDuplicates() 

# COMMAND ----------

# display(finaldf)

# COMMAND ----------

# Adding current timestamp to Dated_On for current processing records
currentdatetime= datetime.now()
finaldf = finaldf.withColumn("Dated_On",lit(currentdatetime))

finaldf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema", "True") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+processName+"_"+tableName)

# COMMAND ----------

# dbutils.notebook.run("/kgsonedata/trusted/maker_checker_validation",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned group by File_Date