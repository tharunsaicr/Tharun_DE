# Databricks notebook source
tableName='finance_metrics'
processName='compensation'

# COMMAND ----------

dbutils.widgets.text(name = "FileDate", defaultValue = "")
FileDate = dbutils.widgets.get("FileDate")

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

paysheetDf = spark.sql("select * from kgsonedatadb.trusted_hist_compensation_paysheet where file_date = '20221101'")

paysheetDf.count()

# COMMAND ----------

employeeDetailsDf = spark.sql("select Employee_Number,Cost_centre,Position,Client_Geography,BU from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where File_Date = '20221118'")


employeeDetailsDf = employeeDetailsDf.withColumn("Position", when(col("Position").isin("Director","Director GDC","Director-CFO-GDC","Technical Director","Associate Partner","Associate Partner - Finance","Associate Partner-KGSnGDC") ,lit("TD/D/AP"))\
                                                 .otherwise(col("Position")))

employeeDetailsDf = employeeDetailsDf.withColumn("Client_Geography", when(col("Position").isin("TD/D/AP") ,lit(""))\
                                                 .otherwise(col("Client_Geography")))
                                                 
 
employeeDetailsDf = employeeDetailsDf.withColumn("Cost_centre", when(col("Position").isin("TD/D/AP") ,col("BU"))\
                                                 .otherwise(col("Cost_centre")))

employeeDetailsDf = employeeDetailsDf.filter(~(col("Position").isin("Partner","Partner COO")))

# Exclude Partho Bandopadhyay because he is considered as a partner with KGS however his designation is Managing Director
employeeDetailsDf = employeeDetailsDf.filter((col("Employee_Number") != 30840))
                                          

# Exclude Secondee Inward and Outward
employeeDetailsDf = employeeDetailsDf.filter(~(col("Employee_Category").isin("Secondee-Inward-With Pay","Secondee-Inward-Without Pay","Secondee-Outward-With Pay","Secondee-Outward-Without Pay")))


employeeDetailsDf = employeeDetailsDf.withColumnRenamed("Employee_Number","ED_Employee_Number")
employeeDetailsDf = employeeDetailsDf.withColumnRenamed("Cost_centre","ED_Cost_centre")
employeeDetailsDf = employeeDetailsDf.withColumnRenamed("Position","ED_Position")
employeeDetailsDf = employeeDetailsDf.withColumnRenamed("Client_Geography","ED_Client_Geography")
employeeDetailsDf = employeeDetailsDf.withColumnRenamed("BU","ED_BU")

# display(employeeDetailsDf)

# COMMAND ----------

sabbaticalDf = spark.sql("select Employee_Number,Cost_centre,Position,Client_Geography from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical  where file_date  = '20221118'")


sabbaticalDf = sabbaticalDf.withColumn("Position", when(col("Position").isin("Director","Director GDC","Director-CFO-GDC","Technical Director","Associate Partner","Associate Partner - Finance","Associate Partner-KGSnGDC") ,lit("TD/D/AP"))\
                                                 .otherwise(col("Position")))

sabbaticalDf = sabbaticalDf.withColumn("Client_Geography", when(col("Position").isin("TD/D/AP") ,lit(""))\
                                                 .otherwise(col("Client_Geography")))


sabbaticalDf = sabbaticalDf.withColumnRenamed("Employee_Number","Sabbatical_Employee_Number")
sabbaticalDf = sabbaticalDf.withColumnRenamed("Cost_centre","Sabbatical_Cost_centre")
sabbaticalDf = sabbaticalDf.withColumnRenamed("Position","Sabbatical_Position")
sabbaticalDf = sabbaticalDf.withColumnRenamed("Client_Geography","Sabbatical_Client_Geography")  

display(sabbaticalDf)

# COMMAND ----------

joinDf = paysheetDf.join(employeeDetailsDf,paysheetDf.EMP_NO == employeeDetailsDf.ED_Employee_Number,"left")\
    .join(sabbaticalDf,paysheetDf.EMP_NO == sabbaticalDf.Sabbatical_Employee_Number,"left")\
    .select(paysheetDf["*"],employeeDetailsDf.ED_Cost_centre,employeeDetailsDf.ED_Position,employeeDetailsDf.ED_Client_Geography,employeeDetailsDf.ED_Employee_Number,sabbaticalDf.Sabbatical_Cost_centre,sabbaticalDf.Sabbatical_Position,sabbaticalDf.Sabbatical_Client_Geography,sabbaticalDf.Sabbatical_Employee_Number)

joinDf.count()

# COMMAND ----------

joinDf = joinDf.withColumn("Cost_centre", lit(None))\
        .withColumn("Position",lit(None))\
        .withColumn("Client_Geography",lit(None))

# COMMAND ----------

finalDf = joinDf.withColumn("Cost_centre",when(col('ED_Employee_Number').isNotNull(),joinDf.ED_Cost_centre)\
    .when(col('Sabbatical_Employee_Number').isNotNull(),joinDf.Sabbatical_Cost_centre)\
    .otherwise(joinDf.Cost_centre))


finalDf = finalDf.withColumn("Position",when(col('ED_Employee_Number').isNotNull(),joinDf.ED_Position)\
    .when(col('Sabbatical_Employee_Number').isNotNull(),joinDf.Sabbatical_Position)\
    .otherwise(joinDf.Position))

finalDf = finalDf.withColumn("Client_Geography",when(col('ED_Employee_Number').isNotNull(),joinDf.ED_Client_Geography)\
        .when(col('Sabbatical_Employee_Number').isNotNull(),joinDf.Sabbatical_Client_Geography)\
        .otherwise(joinDf.Client_Geography))

# COMMAND ----------

#Typce cast CTC to double for aggregation
finalDf = finalDf.withColumn("CTC", finalDf.CTC.cast(DoubleType()))

# COMMAND ----------

finalDf.filter((col("ED_Employee_Number").isNull())).count()

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max

group_cols = ["Cost_centre","Position","Client_Geography"]

aggDf = finalDf.groupBy(group_cols) \
    .agg(count("EMP_NO").alias("Aggregated_HC"), \
         sum("CTC").alias("Annual_Aggregated_CTC")
     )

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

# DBTITLE 1,Load Data to Final trusted tables
dbutils.notebook.run("/kgsonedata/trusted/trustedstg_to_trusted_load",6000,{'DeltaTableName':tableName,'ProcessName':processName})