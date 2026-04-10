# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_cost_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_cost_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_cost_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_cost_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_cost_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_cost_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_cost_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_cost_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_cost_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_cost_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_cost_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_cost_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_team_cost_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_team_cost_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_team_cost_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_team_cost_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_team_cost_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_team_cost_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_team_cost_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_team_cost_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_team_cost_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_team_cost_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_team_cost_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_team_cost_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_hiring_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_hiring_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_hiring_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_hiring_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_hiring_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_hiring_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_hiring_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_hiring_plan

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_hiring_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_hiring_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_hiring_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_hiring_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_fr_recruitment_hiring_outlook

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_fr_recruitment_hiring_outlook

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_hiring_outlook

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_hiring_outlook

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_dim_account_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_dim_account_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_dim_category_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_dim_category_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_dim_designation

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_dim_designation

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_dim_geo_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_dim_geo_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_dim_mi_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_dim_mi_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_curr_fr_recruitment_dim_source_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.trusted_hist_fr_recruitment_dim_source_mapping

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/current/fr/recruitment

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/rawlayer/history/fr/recruitment

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/current/fr/recruitment

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/finance/trustedlayer/history/fr/recruitment

# COMMAND ----------

