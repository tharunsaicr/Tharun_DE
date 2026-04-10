# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_actual_opex_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_actual_opex_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_actual_projects_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_actual_projects_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_actual_hcplan_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_actual_hcplan_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_plan_opex_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_plan_opex_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_plan_projects_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_plan_projects_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_plan_hcplan_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_plan_hcplan_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_forecast_opex_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_forecast_opex_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_forecast_projects_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_forecast_projects_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_curr_bu_report_bu_pack_forecast_hcplan_data_extract

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsfinancedb.raw_hist_bu_report_bu_pack_forecast_hcplan_data_extract

# COMMAND ----------

# %sql
# drop table kgsfinancedb.raw_curr_BU_cf_pack_cf_function

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Financial_Year from kgsfinancedb.raw_hist_bu_report_bu_pack_forecast_hcplan_data_extract
# MAGIC

# COMMAND ----------

