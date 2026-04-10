# Databricks notebook source
# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_fund_recevied_in_csr_account

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_indirect_exp_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_indirect_exp_kgsmpl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_indirect_exp_krc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_kgspl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_kgsmpl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_krcpl_gl

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_curr_csr_format_kgs

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_curr_csr_format_krc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_ngo_fund_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_ngo_fund_util

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_kgs_raw_data_cy

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_curr_csr_ngo_fund_disburse

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_per_hour_rate

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_fund_recevied_in_csr_account_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_kgdcpl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_indirect_expense_kgdc

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_curr_csr_format_kgdcl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_ngo_fund_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_curr_csr_ngo_fund_util_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_fund_recevied_in_csr_account

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_indirect_exp_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_indirect_exp_kgsmpl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_indirect_exp_krc

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_hist_csr_format_kgs

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_hist_csr_format_krc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_ngo_fund_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_ngo_fund_util

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_kgspl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_kgsmpl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_krcpl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_kgs_raw_data_cy

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_hist_csr_ngo_fund_disburse

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_per_hour_rate

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_fund_recevied_in_csr_account_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_indirect_expense_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_kgdcpl_gl

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_hist_csr_format_kgdcl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_ngo_fund_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_hist_csr_ngo_fund_util_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_fund_recevied_in_csr_account

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_indirect_exp_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_indirect_exp_kgsmpl

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_stg_csr_format_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_ngo_fund_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_ngo_fund_util

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_stg_csr_format_krc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_indirect_exp_krc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_kgspl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_kgsmpl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_krcpl_gl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_kgs_raw_data_cy

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_stg_csr_ngo_fund_disburse

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_per_hour_rate

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_fund_recevied_in_csr_account_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_indirect_expense_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_kgdcpl_gl

# COMMAND ----------

# %sql
# drop table kgsonedatadb.raw_stg_csr_format_kgdcl

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_ngo_fund_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.raw_stg_csr_ngo_fund_util_kgdc

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/current/csr/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/history/csr/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r dbfs:/mnt/rawlayermount/staging/csr