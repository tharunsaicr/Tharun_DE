# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_risk_usb_access

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_risk_usb_access

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_risk_usb_access

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_risk_usb_access

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_risk_usb_access

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_risk_usb_access

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/risk/usb_access

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/risk/usb_access

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/risk/usb_access

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/risk/usb_access

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/risk/usb_access

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/risk/usb_access

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.trusted_hist_risk_usb_access_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/risk/usb_access_bad

# COMMAND ----------

