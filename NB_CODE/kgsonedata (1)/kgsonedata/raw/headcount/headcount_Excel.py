# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

def colToExcel(col): # col is 1 based
    excelCol = str()
    div = col 
    while div:
        (div, mod) = divmod(div-1, 26) # will return (x, 0 .. 25)
        excelCol = chr(mod + 65) + excelCol

    return excelCol

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit
import pytz

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata'))

Date_to_str = currentdatetime.strftime("%Y%m%d_%H%M%S")
print("YYYMMDD:", Date_to_str)

fileName = "Headcount_"+Date_to_str+".xlsx"
print("FileName:",fileName )

mountPath = "/mnt/outputfiles/headcount/"


# COMMAND ----------

#employee_details
employee_details = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_employee_details")

# employee_details = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_employee_details")
employee_details = employee_details  .dropDuplicates()

colCount1 = len(employee_details.columns)
rowCount1 = employee_details.select("*").count()

print("Column Count:",colCount1) 
print("Row Count: ",rowCount1)

lastCell1 = str(colToExcel(colCount1)) + str(rowCount1)
#print(lastCell1)

employee_details.write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Employee Details'!A1:"+lastCell1)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

# DBTITLE 1,resigned_and_left
#resigned_and_left
resigned_and_left = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_resigned_and_left")

# resigned_and_left = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_resigned_and_left")
resigned_and_left  = resigned_and_left .dropDuplicates()
display(resigned_and_left)

colCount2 = len(resigned_and_left.columns)
rowCount2 = resigned_and_left.select("*").count()

print("Column Count:",colCount2) 
print("Row Count: ",rowCount2)

lastCell2 = str(colToExcel(colCount2)) + str(rowCount2)
#print(lastCell2)

resigned_and_left.write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Resigned and Left'!A1:"+lastCell2)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

# DBTITLE 1,sabbatical
#sabbatical 
sabbatical = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_sabbatical")

# sabbatical = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_sabbatical")
sabbatical  = sabbatical .dropDuplicates()

colCount3 = len(sabbatical.columns)
rowCount3 = sabbatical.select("*").count()

print("Column Count:",colCount3) 
print("Row Count: ",rowCount3)

lastCell3 = str(colToExcel(colCount3)) + str(rowCount3)
#print(lastCell3)

sabbatical.write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Sabbatical'!A1:"+lastCell3)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

# DBTITLE 1,contingent_worker
#contingent_worker
contingent_worker = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_contingent_worker")

# contingent_worker = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_contingent_worker")

colCount4 = len(contingent_worker.columns)
rowCount4 = contingent_worker.select("*").count()

print("Column Count:",colCount4) 
print("Row Count: ",rowCount4)

lastCell4 = str(colToExcel(colCount4)) + str(rowCount4)
# print(lastCell4)


contingent_worker.write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Contingent Worker'!A1:"+lastCell4)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

# DBTITLE 1,academic_trainee
#academic_trainee

academic_trainee_table = 'hive_metastore.kgsonedatadb.trusted_headcount_academic_trainee'

if(spark._jsparkSession.catalog().tableExists(academic_trainee_table)):
    academic_trainee = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_academic_trainee")

    colCount5 = len(academic_trainee.columns)
    rowCount5 = academic_trainee.select("*").count()

    print("Column Count:",colCount5) 
    print("Row Count: ",rowCount5)

    lastCell5 = str(colToExcel(colCount5)) + str(rowCount5)
    print(lastCell5)

    academic_trainee.write.mode("append")\
    .format("com.crealytics.spark.excel")\
    .option("dateFormat", "yyyy-MM-dd")\
    .option("dataAddress","'Academic Trainee'!A1:"+lastCell5)\
    .option("header",True)\
    .save(mountPath+fileName)


# COMMAND ----------

# DBTITLE 1,loaned_staff_from_ki
#loaned_staff_from_ki
loaned_staff_from_ki = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_loaned_staff_from_ki")

# loaned_staff_from_ki = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_loaned_staff_from_ki")

colCount6 = len(loaned_staff_from_ki.columns)
rowCount6 = loaned_staff_from_ki.select("*").count()

print("Column Count:",colCount6) 
print("Row Count: ",rowCount6)

lastCell6 = str(colToExcel(colCount6)) + str(rowCount6)
#print(lastCell6)

loaned_staff_from_ki .write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Loaned Staff from KI'!A1:"+lastCell6)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

# DBTITLE 1,contingent_resigned
#loaned_contingent_resigned
loaned_contingent_resigned= spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_contingent_worker_resigned")

# loaned_contingent_resigned= spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_loaned_contingent_resigned")

colCount7 = len(loaned_contingent_resigned.columns)
rowCount7 = loaned_contingent_resigned.select("*").count()

print("Column Count:",colCount7) 
print("Row Count: ",rowCount7)

lastCell7 = str(colToExcel(colCount7)) + str(rowCount7)
#print(lastCell7)

loaned_contingent_resigned.write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Contingent Resigned'!A1:"+lastCell7)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

# DBTITLE 1,loaned_resigned
#loaned_contingent_resigned
loaned_contingent_resigned= spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_loaned_staff_resigned")

# loaned_contingent_resigned= spark.sql("select * from hive_metastore.kgsonedatadb.trusted_stg_headcount_loaned_resigned")

colCount10 = len(loaned_contingent_resigned.columns)
rowCount10 = loaned_contingent_resigned.select("*").count()

print("Column Count:",colCount10) 
print("Row Count: ",rowCount10)

lastCell10 = str(colToExcel(colCount7)) + str(rowCount10)
#print(lastCell7)

loaned_contingent_resigned.write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Loaned-Resigned'!A1:"+lastCell10)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

# DBTITLE 1,loaned_secondee_outward
#loaned_secondee_outward
loaned_secondee_outward = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_secondee_outward")

# loaned_secondee_outward = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_secondee_outward")

colCount8 = len(loaned_secondee_outward.columns)
rowCount8 = loaned_secondee_outward.select("*").count()

print("Column Count:",colCount8) 
print("Row Count: ",rowCount8)

lastCell8 = str(colToExcel(colCount8)) + str(rowCount8)
#print(lastCell8)

loaned_secondee_outward.write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Secondee Outward'!A1:"+lastCell8)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

# DBTITLE 1,loaned_maternity_cases
#loaned_maternity_cases
loaned_maternity_cases = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_headcount_maternity_cases")

# loaned_maternity_cases = spark.sql("select * from hive_metastore.kgsonedatadb.trusted_hist_headcount_maternity_cases")

colCount9 = len(loaned_maternity_cases.columns)
rowCount9 = loaned_maternity_cases.select("*").count()

print("Column Count:",colCount9) 
print("Row Count: ",rowCount9)

lastCell9 = str(colToExcel(colCount9)) + str(rowCount9)
#print(lastCell8)

loaned_maternity_cases.write.mode("append")\
.format("com.crealytics.spark.excel")\
.option("dateFormat", "yyyy-MM-dd")\
.option("dataAddress","'Maternity Cases'!A1:"+lastCell9)\
.option("header",True)\
.save(mountPath+fileName)

# COMMAND ----------

