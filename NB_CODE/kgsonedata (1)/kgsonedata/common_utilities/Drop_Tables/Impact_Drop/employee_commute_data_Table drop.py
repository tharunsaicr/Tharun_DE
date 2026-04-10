# Databricks notebook source
# MAGIC %sql
# MAGIC --select * from kgsonedatadb.trusted_curr_impact_employee_commute_data
# MAGIC drop table kgsonedatadb.raw_curr_impact_employee_commute_data   

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_impact_employee_commute_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_impact_employee_commute_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_impact_employee_commute_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_impact_employee_commute_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_impact_employee_commute_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/impact/employee_commute_data

# COMMAND ----------

# MAGIC   %fs
# MAGIC   rm -r  dbfs:/mnt/trustedlayermount/current/impact/employee_commute_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/impact/employee_commute_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/impact/employee_commute_data

# COMMAND ----------

# MAGIC   %fs
# MAGIC     rm -r dbfs:/mnt/rawlayermount/staging/impact/employee_commute_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/impact/employee_commute_data

# COMMAND ----------

