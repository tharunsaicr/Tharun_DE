# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_impact_wlb_attrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_impact_wlb_attrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_impact_wlb_attrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_impact_wlb_attrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_impact_wlb_attrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_impact_wlb_attrition_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/impact/wlb_attrition_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/impact/wlb_attrition_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/impact/wlb_attrition_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/impact/wlb_attrition_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/impact/wlb_attrition_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/impact/wlb_attrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb_badrecords.impact_wlb_attrition_data_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/impact/wlb_attrition_data_bad