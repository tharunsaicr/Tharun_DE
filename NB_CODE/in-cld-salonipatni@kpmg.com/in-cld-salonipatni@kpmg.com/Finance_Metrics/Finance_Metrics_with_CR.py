# Databricks notebook source
tableName='finance_metrics'
processName='compensation'

# COMMAND ----------

dbutils.widgets.text(name = "FileDate", defaultValue = "")
fileDate = dbutils.widgets.get("FileDate")

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,date_sub,to_date,count,ltrim,trim,upper,regexp_replace
from datetime import datetime
from pyspark.sql.types import *
from dateutil.parser import parse
from pyspark.sql.functions import concat, concat_ws, lit, col, trim
from pyspark.sql.functions import sum,avg,max

# COMMAND ----------

currentdatetime= datetime.now()

# COMMAND ----------

paysheetDf = spark.sql("select * from kgsonedatadb.trusted_hist_compensation_paysheet where file_date = '20230101'")

paysheetDf.count()

# COMMAND ----------

# DBTITLE 1,Get details from Employee_Details
edDf = spark.sql("select Employee_Number,Cost_centre,Position,Client_Geography,BU,Employee_Category,File_Date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where File_Date = '20230118'")

# Exclude Secondee Inward and Outward
employeeDetailsDf = edDf.filter(~(col("Employee_Category").isin("Secondee-Inward-With Pay","Secondee-Inward-Without Pay","Secondee-Outward-With Pay","Secondee-Outward-Without Pay")))

employeeDetailsDf = employeeDetailsDf.drop("Employee_Category")

# COMMAND ----------

# DBTITLE 1,Get details from Resign_and_Left and Secondee_Outward
resignAndLeftDf = spark.sql("select Employee_Number,Cost_centre,Position,Client_Geography,BU,File_Date from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where File_Date = '20230118'")

secondeeOutwardDf = spark.sql("select Employee_Number,Cost_centre,Position,Client_Geography,BU,File_Date from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward where File_Date = '20230118'")

# COMMAND ----------

# DBTITLE 1,Combine all the data from Headcount Report
unionDf = employeeDetailsDf.union(resignAndLeftDf).union(secondeeOutwardDf)

# COMMAND ----------

unionDf = unionDf.withColumn("Position", when(col("Position").isin("Director","Director GDC","Director-CFO-GDC","Technical Director","Associate Partner","Associate Partner - Finance","Associate Partner-KGSnGDC") ,lit("TD/D/AP"))\
                                                 .otherwise(col("Position")))

unionDf = unionDf.withColumn("Client_Geography", when(col("Position").isin("TD/D/AP") ,lit(""))\
                                                 .otherwise(col("Client_Geography")))
                                                 
 
unionDf = unionDf.withColumn("Cost_centre", when(col("Position").isin("TD/D/AP") ,col("BU"))\
                                                 .otherwise(col("Cost_centre")))

unionDf = unionDf.filter(~(col("Position").isin("Partner","Partner COO")))

# Exclude Partho Bandopadhyay because he is considered as a partner with KGS however his designation is Managing Director
unionDf = unionDf.filter((col("Employee_Number") != 30840))
                                          


unionDf = unionDf.withColumnRenamed("File_Date","Headcount_File_Date")

# display(employeeDetailsDf)

# COMMAND ----------

# DBTITLE 1,Get Cost_centre,Position,Client_Geography from Headcount Report
joinDf = paysheetDf.join(unionDf,paysheetDf.EMP_NO == unionDf.Employee_Number,"left")\
    .select(paysheetDf["*"],unionDf.Cost_centre,unionDf.Position,unionDf.Client_Geography,unionDf.Employee_Number,unionDf.Headcount_File_Date)

# COMMAND ----------

# DBTITLE 1, If Employee is available in paysheet and not in Headcount report then, do not include them in final aggregation.
finalDf =  joinDf.filter(col("Employee_Number").isNotNull())

# COMMAND ----------

# DBTITLE 1, If Employee is available in paysheet and not in Headcount report then, move that to bad Record
Headcount_File_Date = finalDf.select("Headcount_File_Date").distinct().rdd.flatMap(lambda x: x).collect()[0]

badDf = joinDf.filter(col("Employee_Number").isNull())
badDf = badDf.withColumn("Headcount_File_Date",lit(Headcount_File_Date))


# Adding Employee Category from Employee_Details for Audit purpose i.e. to know if employee is present in Employee_Details then why they were excluded

badDfDropCol = ["Cost_centre","Position","Client_Geography","Employee_Number"]

edDf = edDf.select("Employee_Number","Employee_Category")
edDf = edDf.withColumnRenamed("Employee_Number","ED_Employee_Number")\
    .withColumnRenamed("Employee_Category","ED_Employee_Category")

badDf = badDf.join(edDf, badDf.EMP_NO == edDf.ED_Employee_Number,"left").select(badDf["*"],edDf.ED_Employee_Category)
badDf = badDf.drop(*badDfDropCol)
display(badDf)


# COMMAND ----------

#Typce cast CTC to double for aggregation
finalDf = finalDf.withColumn("CTC", finalDf.CTC.cast(DoubleType()))

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max

group_cols = ["Cost_centre","Position","Client_Geography"]

aggDf = finalDf.groupBy(group_cols) \
    .agg(count("EMP_NO").alias("Aggregated_HC"), \
         sum("CTC").alias("Annual_Aggregated_CTC")
     )

# COMMAND ----------

aggDf=aggDf.withColumn("Dated_On", lit(currentdatetime))
aggDf=aggDf.withColumn("File_Date", lit(fileDate))

display(aggDf)

# COMMAND ----------

# DBTITLE 1, Trusted stg

aggDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true") \
.option("path",trusted_stg_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadb.trusted_stg_"+ processName + "_" +tableName)

# COMMAND ----------

# DBTITLE 1,Load to Bad Records
badDfCount = badDf.count()

if int(badDfCount) > 0:
    badDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true") \
    .option("path",bad_filepath_url+processName+"/"+tableName+"_bad") \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb_badrecords.trusted_hist_"+ processName + "_" + tableName+"_bad")

# COMMAND ----------

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

