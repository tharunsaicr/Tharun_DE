# Databricks notebook source
#dbutils.widgets.remove("IpPath")
#/mnt/finance/landinglayer/pending/test/Test_IP/

# COMMAND ----------

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
ProcessName = dbutils.widgets.get("ProcessName")

dbutils.widgets.text(name = "IpPath", defaultValue = "")
IpPath = dbutils.widgets.get("IpPath")

dbutils.widgets.text(name = "DestinationPath", defaultValue = "")
DestinationPath = dbutils.widgets.get("DestinationPath")

dbutils.widgets.text(name = "FileName", defaultValue = "")
FileName = dbutils.widgets.get("FileName")

print(IpPath)
print(ProcessName)
print(FileName)
print(DestinationPath)

# COMMAND ----------

dbutils.fs.mv('/mnt'+IpPath+FileName,'/mnt'+DestinationPath+FileName, True)


# COMMAND ----------

#dbutils.fs.rm(IpPath+FileName)