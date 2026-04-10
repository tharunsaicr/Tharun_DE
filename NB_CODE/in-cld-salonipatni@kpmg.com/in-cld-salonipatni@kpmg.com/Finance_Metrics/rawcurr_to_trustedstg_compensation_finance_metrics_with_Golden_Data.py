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

# employeeGoldenDataDf = spark.sql("select * from kgsonedatadb.employee_golden_data")

# COMMAND ----------

employeeGoldenDataDf = spark.sql("select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details")

# COMMAND ----------

paysheetDf = spark.sql("select * from kgsonedatadb.trusted_compensation_paysheet ")


# COMMAND ----------

paysheetDf.count()

# COMMAND ----------

employeeGoldenDataDf = employeeGoldenDataDf.withColumn("Position", when(col("Position").isin("Director","Director GDC","Director-CFO-GDC","Technical Director","Associate Partner","Associate Partner - Finance","Associate Partner-KGSnGDC") ,lit("TD/D/AP"))\
                                                 .otherwise(col("Position")))

employeeGoldenDataDf = employeeGoldenDataDf.withColumn("Client_Geography", when(col("Position").isin("TD/D/AP") ,lit(""))\
                                                 .otherwise(col("Client_Geography")))
                                                 
 
employeeGoldenDataDf = employeeGoldenDataDf.withColumn("Cost_centre", when(col("Position").isin("TD/D/AP") ,col("BU"))\
                                                 .otherwise(col("Cost_centre")))

# COMMAND ----------

employeeGoldenDataDf = employeeGoldenDataDf.filter(~(col("Position").isin("Partner","Partner COO")))

# Exclude Partho Bandopadhyay because he is considered as a partner with KGS however his designation is Managing Director
employeeGoldenDataDf = employeeGoldenDataDf.filter((col("Employee_Number") != 30840))
                                          

# COMMAND ----------

joinPaysheetemployeeGoldenDataDf = paysheetDf.join(employeeGoldenDataDf,paysheetDf.EMP_NO == employeeGoldenDataDf.Employee_Number,"left").select(paysheetDf["*"],employeeGoldenDataDf.Cost_centre,employeeGoldenDataDf.Position,employeeGoldenDataDf.Client_Geography,employeeGoldenDataDf.Employee_Number)


# joinPaysheetemployeeGoldenDataDf = paysheetDf.join(employeeGoldenDataDf,paysheetDf.EMP_NO == employeeGoldenDataDf.Employee_Number,"left").select(paysheetDf["*"],employeeGoldenDataDf.Cost_centre,employeeGoldenDataDf.Position,employeeGoldenDataDf.Client_Geography,employeeGoldenDataDf.Employee_Number,employeeGoldenDataDf.Headcount_Status)


display(joinPaysheetemployeeGoldenDataDf)

# COMMAND ----------

joinPaysheetemployeeGoldenDataDf.filter(col('Employee_Number').isNull()).display()

# COMMAND ----------

# 10.	If the employee is present in Paysheet and not HC file , then Compensation team goes back to Manoranjan to check this scenarios (possibility that employee has left the organization hence not present in Employee Details).

# Need to check if we need to send email alert or action required

# COMMAND ----------

finalDf = joinPaysheetemployeeGoldenDataDf

# COMMAND ----------

finalDf.filter(col('Cost_Centre').isNull()).display()

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max

group_cols = ["Cost_centre","Position","Client_Geography"]

aggDf = finalDf.groupBy(group_cols) \
    .agg(count("EMP_NO").alias("Aggregated_HC"), \
         sum("CTC").alias("Annual_Aggregated_CTC")
     )

display(aggDf)

# COMMAND ----------

# display(aggDf.filter(col("Position").isin("TD/D/AP")))
display(aggDf.select("Position").distinct())

# COMMAND ----------

currentdatetime= datetime.now()
aggDf = aggDf.withColumn("Dated_On",lit(currentdatetime))


# COMMAND ----------

aggDf=aggDf.withColumn("File_Date", lit(FileDate))

# COMMAND ----------

aggDf.filter(col('Cost_Centre').isNull()).display()

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

# COMMAND ----------

