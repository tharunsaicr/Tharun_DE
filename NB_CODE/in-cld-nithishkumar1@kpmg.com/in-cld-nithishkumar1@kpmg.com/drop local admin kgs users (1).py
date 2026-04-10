# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_risk_local_admin_users_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_risk_local_admin_users_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_risk_local_admin_users_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_risk_local_admin_users_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_risk_local_admin_users_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_risk_local_admin_users_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.trusted_hist_risk_local_admin_users_kgs_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/risk/local_admin_users_kgs_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/risk/local_admin_users_kgs

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/risk/local_admin_users_kgs

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/risk/local_admin_users_kgs

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/risk/local_admin_users_kgs

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/risk/local_admin_users_kgs

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/risk/local_admin_users_kgs

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/risk/local_admin_users_kgs_bad

# COMMAND ----------

