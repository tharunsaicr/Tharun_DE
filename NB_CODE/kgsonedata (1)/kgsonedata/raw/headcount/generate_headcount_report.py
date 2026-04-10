# Databricks notebook source
# Generate individual tabs within headcount report

# COMMAND ----------

dbutils.widgets.text(name = "CurrentMonth", defaultValue = "")
currentMonth= dbutils.widgets.get("CurrentMonth")

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
currentCutOff = dbutils.widgets.get("CurrentCutOffDate")

dbutils.widgets.text(name = "LastCutOffDate", defaultValue = "")
lastCutoff = dbutils.widgets.get("LastCutOffDate")

print(currentMonth)
print(currentCutOff)
print(lastCutoff)

# COMMAND ----------

# DBTITLE 1,Employee Details
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_EmployeeDetails_trustedstg",6000)

# COMMAND ----------

# DBTITLE 1,Resigned and Left
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_Resign_And_Left_trustedstg",6000,{'CurrentMonth':currentMonth})


# COMMAND ----------

# DBTITLE 1,Contingent Worker
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_ContingentWorker_trustedstg",6000)


# COMMAND ----------

# DBTITLE 1,Contingent Resigned 
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_Contingent_Worker_Resigned_trustedstg",6000,{'CurrentMonth':currentMonth})

# COMMAND ----------

# DBTITLE 1,Secondee Outward
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_Secondee_Outward_rawstg_trustedstg",6000,{'CurrentCutOffDate':currentCutOff})

# COMMAND ----------

# DBTITLE 1,Loaned Staff from KI
# File is not available for Aug/Sep so it failed
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_Loaned_Staff_From_KI",6000)

# COMMAND ----------

# DBTITLE 1,Loaned Resigned
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_Loaned_Staff_Resigned",6000)

# COMMAND ----------

# DBTITLE 1,Maternity Cases
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_Maternity_rawstg_trustedstg",6000,{'CurrentCutOffDate':currentCutOff})

# COMMAND ----------

# DBTITLE 1,Sabbatical
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/HC_Sabbatical_rawstg_trustedstg",6000,{'CurrentCutOffDate':currentCutOff})

# COMMAND ----------

