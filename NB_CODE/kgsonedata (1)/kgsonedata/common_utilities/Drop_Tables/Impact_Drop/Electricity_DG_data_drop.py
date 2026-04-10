# Databricks notebook source
# MAGIC %sql
# MAGIC --select * from kgsonedatadb.raw_curr_impact_electricity_and_dg_data
# MAGIC drop table kgsonedatadb.raw_curr_impact_electricity_and_dg_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_impact_electricity_and_dg_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_impact_electricity_and_dg_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_impact_electricity_and_dg_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_impact_electricity_and_dg_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_impact_electricity_and_dg_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/impact/electricity_and_dg_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/impact/electricity_and_dg_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/impact/electricity_and_dg_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/impact/electricity_and_dg_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/impact/electricity_and_dg_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/impact/electricity_and_dg_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC truncate table kgsonedatadb_badrecords.impact_electricity_and_dg_data_bad

# COMMAND ----------

