# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import col, lit,lower,upper,format_number,to_timestamp,date_format,year
from datetime import datetime
import pytz
import string

# COMMAND ----------

# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName").lower()

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

# dbutils.widgets.text(name = "ReportName", defaultValue ="")
# ReportName = dbutils.widgets.get("ReportName")

print(tableName)
print(processName)
# print(ReportName)

# COMMAND ----------

#Extract filename & date - switch case for each file

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /Workspace/kgsonedataedw/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /Workspace/kgsonedataedw/common_utilities/common_components

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit,to_timestamp
import pytz


currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
print(currentdatetime)

# COMMAND ----------

# DBTITLE 1,Format Headers Data
currentDf = spark.sql("select * from kgsonedatadbedw.raw_curr_"+tableName)
currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))

display(currentDf)

# COMMAND ----------

table_dict = {
    'FACT_AP_AMEX': ['PAYMENT_DATE'],
    'FACT_AP_INVOICE_REGISTER': ['INVOICE_DATE'],
    'FACT_AP_PREPAYMENT_STATUS_INVOICE_PAYMENT': ['GL_DATE'],
    'FACT_AP_PREPAYMENT_STATUS_INVOICE_QUERY': ['GL_DATE'],
    'FACT_AP_PREPAYMENT_TRANSACTION': ['APPLIED_GL_DATE'],
    'FACT_AP_TRIAL_INVOICE': ['INVOICE_DATE'],
    'FACT_AP_TRIAL_PAYMENT': ['PAYMENT_GL_DATE'],
    'FACT_APAR': ['NEW_GL_DATE'],
    'FACT_AR_BILLING': ['INVOICE_DATE'],
    'FACT_AR_COLLECTION': ['GL_APPLIED_DATE'],
    'FACT_AR_CUSTOMER_CONTACT_COUNTRY': ['TRX_DATE'],
    'FACT_AR_HISTORY': ['APPLICATION_DATE'],
    'FACT_CENVAT_COLLECTED': ['AP_INVOICE_DATE'],
    'FACT_FA_MASTER': ['INVOICE_DATE'],
    'FACT_GL_Q1': ['GL_DATE'],
    'FACT_GL_Q10': ['GL_DATE'],
    'FACT_GL_Q11': ['GL_DATE'],
    'FACT_GL_Q12': ['GL_DATE'],
    'FACT_GL_Q13': ['GL_DATE'],
    'FACT_GL_Q18': ['GL_DATE'],
    'FACT_GL_Q2': ['GL_DATE'],
    'FACT_GL_Q21': ['GL_DATE'],
    'FACT_GL_Q23': ['GL_DATE'],
    'FACT_GL_Q26': ['GL_DATE'],
    'FACT_GL_Q3': ['GL_DATE'],
    'FACT_GL_Q4': ['GL_DATE'],
    'FACT_GL_Q5': ['GL_DATE'],
    'FACT_GL_Q6': ['GL_DATE'],
    'FACT_GL_Q7': ['GL_DATE'],
    'FACT_GL_Q8': ['GL_DATE'],
    'FACT_HR_LEAVE_DETAILS': ['EXPENDITURE_ITEM_DATE'],
    'FACT_P_EXPENDITURE': ['EXPENDITURE_ITEM_DATE'],
    'FACT_P_KGS_STAFF_UTILIZATION': ['EXPENDITURE_ITEM_DATE'],
    'FACT_P_PROJECT_INVOICE': ['INVOICE_DATE'],
    'FACT_P_SU_STD_HOURS': ['GL_DATE'],
    'FACT_P_WIP_NFR': ['WIP_GL_DATE'],
    'FACT_PAYMENT_REGISTER': ['INVOICE_DATE'],
    'FACT_PO_DETAILS_WITH_CHEQUE_NUMBERS': ['PO_CREATION_DATE'],
    'FACT_RECIEPT_REGISTER': ['GL_DATE'],
    'FACT_REQUISITION_PURCHASE_STATUS': ['REQUISITION_RAISED_DATE'],
    'FACT_TDS_DETAILS': ['GL_DATE']
}


# COMMAND ----------

month_list=["OCT","NOV","DEC"]
if tableName.upper() in table_dict:
    key_column=table_dict[tableName.upper()]
    print(key_column)
    currentDf = currentDf.withColumn("MONTH", upper(date_format(currentDf[key_column[0]], 'MMM'))) \
                         .withColumn("CALENDAR_YEAR", year(currentDf[key_column[0]])) \
                         .withColumn("FINANCIAL_YEAR", when(col('MONTH').isin(month_list),col('CALENDAR_YEAR') + 1).otherwise(col('CALENDAR_YEAR')))

# COMMAND ----------

# DBTITLE 1,Load Data to Trusted Current 
currentDf.write \
.mode("overwrite") \
.format("delta") \
.option("overwriteSchema","true")  \
.option("path",edw_trusted_curr_savepath_url+processName+"/"+tableName) \
.option("compression","snappy") \
.saveAsTable("kgsonedatadbedw.trusted_curr_"+tableName)

# COMMAND ----------

# DBTITLE 1,Loading the trusted table to SQL Database
dbutils.notebook.run("/Workspace/kgsonedataedw/trusted/Delta_to_SQL_with_Select",8000,{'DeltaTableName':tableName,'ProcessName':processName})

# COMMAND ----------

# Add Validation to count Number of records inserted into table matches with the original file