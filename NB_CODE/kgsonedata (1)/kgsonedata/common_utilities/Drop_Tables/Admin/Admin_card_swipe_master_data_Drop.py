# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_stg_admin_card_swipe_master_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_admin_card_swipe_master_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_admin_card_swipe_master_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/admin/card_swipe_master_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/admin/card_swipe_master_data

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/admin/card_swipe_master_data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb_badrecords.admin_card_swipe_master_data_bad

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/badfiles/admin/card_swipe_master_data_bad

# COMMAND ----------

