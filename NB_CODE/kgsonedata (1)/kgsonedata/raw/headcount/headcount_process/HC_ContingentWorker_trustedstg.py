# Databricks notebook source
# DBTITLE 1,Call connection configuration notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
current_cutoff_date = dbutils.widgets.get("CurrentCutOffDate")

tableName = "contingent_worker"
processName = "headcount"

print(tableName)
print(processName)
print(current_cutoff_date)

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse
from pyspark.sql.functions import concat, concat_ws, lit, col, trim

# COMMAND ----------

finalDf = spark.sql("select * from (select rank() over(partition by candidate_id,file_date order by dated_on desc) as rank, * from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent where file_date = (select max(file_date) from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent where to_date(File_Date,'yyyyMMdd') <= to_date("+"'"+current_cutoff_date+"'"+"))) C_hist where rank = 1")

# display(finalDf)

# COMMAND ----------

# DBTITLE 1,# Status : New Joiner, HC - active cases (to be moved to Contingent Worker Tab in final HC)
contingentDf = finalDf.filter((finalDf.Status == "New Joiners") | (finalDf.Status == "HC") )
# display(contingentDf)

# COMMAND ----------

# DBTITLE 1,Update Business category # Business col (current contingent file):  # PS - group A, Non PS - group B
contingentDf = contingentDf.withColumn("Business_Category", when(col("Business_CategoryX") == "PS", "Group A")\
                                       .when(col("Business_CategoryX") == "Non PS", "Group A")\
                                       .otherwise("Not Applicable"))
# display(contingentDf)

# COMMAND ----------

# Update Staff type(New Column) col based on Assignment category col
# Assignment category = contractor then contractor or sub contractor (manual cannot handle, sometimes contractor, otherwise sub contractor)
# Assignment category =  Sub contractor then sub contractor
# Assignment category = Third party then sub contractor
# Assignment category = Academic intern then academic trainee

finaldf = contingentDf.withColumn("Staff_Type",when(col("Assignment_Category") == "Contractor", "Contractor")\
                                      .when(col("Assignment_Category") == "Sub - Contractor", "Sub - Contractor")\
                                      .when(col("Assignment_Category") == "Third-Party", "Third-Party")\
                                      .when(col("Assignment_Category") == "Academic Intern", "Academic Trainee")\
                                      .otherwise(col("Assignment_Category")))

# display(finaldf)

# COMMAND ----------

# DBTITLE 1,Derive function, subfunction, subfunction1
mappingDf = spark.sql("select * from hive_metastore.kgsonedatadb.config_cost_center_function_subfunction")
mappingDf = mappingDf.withColumnRenamed("Cost_centre","Mapping_Cost_centre").drop("File_Date")
display(mappingDf)

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

# DBTITLE 1,Select required columns
finaldf = finaldf.select("Candidate_Id","Full_Name","Function","Employee_Subfunction","Employee_Subfunction_1","Organization_Name","Cost_Centre","Operating_Unit","Client_Geography","User_Type","Business_CategoryX","Location","Sub_Location","Job","Position","People_Group","Date_of_Joining","Gender","Company","Subcontract_Agency","Official_Email_ID","Assignment_Category","Staff_Type","BU","File_Date")

# display(finaldf)

# COMMAND ----------

# DBTITLE 1,drop duplicates
finaldf = finaldf.dropDuplicates() 

# COMMAND ----------

#Adding current timestamp to Dated_On for current processing records
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

# DBTITLE 1,Maker Checker Validation
# dbutils.notebook.run("/kgsonedata/trusted/maker_checker_validation",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})