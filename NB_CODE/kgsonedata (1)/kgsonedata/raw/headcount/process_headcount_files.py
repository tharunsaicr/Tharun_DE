# Databricks notebook source
dbutils.widgets.text(name = "CurrentMonth", defaultValue = "")
currentMonth = dbutils.widgets.get("CurrentMonth")

dbutils.widgets.text(name = "CurrentCutOffDate", defaultValue = "")
currentCutOff = dbutils.widgets.get("CurrentCutOffDate")

dbutils.widgets.text(name = "LastCutOffDate", defaultValue = "")
lastCutoff = dbutils.widgets.get("LastCutOffDate")

print(currentMonth)
print(currentCutOff)
print(lastCutoff)

# COMMAND ----------

# Added this for ADF testing
# dbutils.notebook.exit(0)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 0,Call common components module
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Employee Dump
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/EmployeeDump_rawcurr_trustedstg",6000,{'CurrentMonth':currentMonth,'CurrentCutOffDate':currentCutOff,'LastCutOffDate':lastCutoff})

# COMMAND ----------

# DBTITLE 1,Termination Dump
dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/TerminationDump_rawcurr_trustedstg",6000,{'CurrentMonth':currentMonth,'CurrentCutOffDate':currentCutOff,'LastCutOffDate':lastCutoff})

# COMMAND ----------

# DBTITLE 1,Contingent
# dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/Contingent_raw_trustedstg",6000,{'CurrentMonth':currentMonthLeft,'CurrentCutOffDate':currentCutOff,'LastCutOffDate':lastCutoff})

currentCutOff1 = "2022-08-16"

dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_process/Contingent_raw_trustedstg",6000,{'CurrentMonth':currentMonth,'CurrentCutOffDate':currentCutOff1,'LastCutOffDate':lastCutoff})

# COMMAND ----------

# DBTITLE 1,Update Head Count
dbutils.notebook.run("/kgsonedata/raw/headcount/generate_headcount_report",6000, {'CurrentMonth':currentMonth,'CurrentCutOffDate':currentCutOff,'LastCutOffDate':lastCutoff})

# COMMAND ----------

# DBTITLE 1,Generate Headcount File
# dbutils.notebook.run("/kgsonedata/raw/headcount/headcount_Excel",6000)