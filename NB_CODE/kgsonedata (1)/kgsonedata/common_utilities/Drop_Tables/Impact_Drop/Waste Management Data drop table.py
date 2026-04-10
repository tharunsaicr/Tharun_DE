# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_impact_waste_management_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_impact_waste_management_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_impact_waste_management_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_impact_waste_management_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_impact_waste_management_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_impact_waste_management_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/impact/waste_management_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/impact/waste_management_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/impact/waste_management_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/impact/waste_management_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/impact/waste_management_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/impact/waste_management_data