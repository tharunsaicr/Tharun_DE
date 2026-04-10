# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_bgv_joined_Candidate_ki_loaned

# COMMAND ----------

# MAGIC %sql                    
# MAGIC drop table kgsonedatadb.trusted_hist_bgv_joined_Candidate_ki_loaned

# COMMAND ----------

# MAGIC %sql                    
# MAGIC drop table kgsonedatadb.trusted_stg_bgv_joined_Candidate_ki_loaned

# COMMAND ----------

# MAGIC %sql                    
# MAGIC drop table kgsonedatadb.raw_stg_bgv_joined_Candidate_ki_loaned

# COMMAND ----------

# MAGIC %sql                    
# MAGIC drop table kgsonedatadb.raw_hist_bgv_joined_Candidate_ki_loaned

# COMMAND ----------

# MAGIC %sql                    
# MAGIC drop table kgsonedatadb.raw_curr_bgv_joined_Candidate_ki_loaned

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/history/bgv_joined_candidate/ki_loaned

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/current/bgv_joined_candidate/ki_loaned

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/trustedlayermount/staging/bgv_joined_candidate/ki_loaned

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/bgv_joined_candidate/ki_loaned

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/bgv_joined_candidate/ki_loaned

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/bgv_joined_candidate/ki_loaned

# COMMAND ----------

