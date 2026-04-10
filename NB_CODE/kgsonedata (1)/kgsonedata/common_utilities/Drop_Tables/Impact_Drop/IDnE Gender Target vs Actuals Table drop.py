# Databricks notebook source
# MAGIC %sql
# MAGIC --select * from kgsonedatadb.raw_curr_impact_idne_gender_target_vs_actuals
# MAGIC drop table kgsonedatadb.raw_curr_impact_idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_hist_impact_idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.raw_stg_impact_idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_hist_impact_idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_impact_idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table kgsonedatadb.trusted_stg_impact_idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/impact/idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/impact/idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/impact/idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/impact/idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/impact/idne_gender_target_vs_actuals

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/impact/idne_gender_target_vs_actuals