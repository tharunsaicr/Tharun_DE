# Databricks notebook source
# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/headcount/employee_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/headcount/employee_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/headcount/employee_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/headcount/employee_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/headcount/employee_dump

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/headcount/employee_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_headcount_employee_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_headcount_employee_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_headcount_employee_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_headcount_employee_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_headcount_employee_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_headcount_employee_dump