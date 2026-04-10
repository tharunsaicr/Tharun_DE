# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_curr_impact_gps_wellbeing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_impact_gps_wellbeing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_impact_gps_wellbeing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_impact_gps_wellbeing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_impact_gps_wellbeing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_impact_gps_wellbeing

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/impact/gps_wellbeing

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/impact/gps_wellbeing

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/impact/gps_wellbeing

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/impact/gps_wellbeing

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/impact/gps_wellbeing

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/impact/gps_wellbeing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb_badrecords.impact_gps_wellbeing_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/impact/gps_wellbeing_bad