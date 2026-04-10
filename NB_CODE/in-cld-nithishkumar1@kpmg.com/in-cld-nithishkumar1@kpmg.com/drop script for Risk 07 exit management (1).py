# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_risk_laptop_recovery

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_risk_laptop_ageing

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_risk_laptop_recovery

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_risk_laptop_aging

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_risk_laptop_recovery

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_risk_laptop_ageing

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_risk_laptop_recovery

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_risk_laptop_ageing

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_risk_laptop_recovery

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_risk_laptop_ageing

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_risk_laptop_recovery

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_risk_laptop_ageing

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.trusted_hist_risk_laptop_recovery_bad

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.trusted_hist_risk_laptop_ageing_bad

# COMMAND ----------

# %sql
# drop table kgsonedatadb_badrecords.risk_independence_violations_bad

# COMMAND ----------

# %fs
# rm -r dbfs:/mnt/badfiles/risk/independence_violations_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/risk/laptop_recovery

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/risk/laptop_recovery

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/risk/laptop_recovery

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/risk/laptop_recovery

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/risk/laptop_recovery

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/risk/laptop_recovery

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/risk/laptop_recovery_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/risk/laptop_ageing_bad