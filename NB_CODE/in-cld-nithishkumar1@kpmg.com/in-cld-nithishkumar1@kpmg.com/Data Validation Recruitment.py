# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_recruitment_hiring_actual where Financial_Year='2024'

# COMMAND ----------

# MAGIC %sql
# MAGIC select   * from kgsfinancedb.trusted_hist_fr_recruitment_hiring_actual where Financial_Year='2020
# MAGIC ' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_recruitment_hiring_forecast where Financial_Year='2023'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_recruitment_hiring_outlook where Financial_Year='2023'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_recruitment_hiring_outlook where Financial_Year='2023'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsfinancedb.trusted_hist_fr_recruitment_hiring_forecast where Financial_Year='2023'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_recruitment_hiring_plan where Financial_Year='2024'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_actual where financial_Year='2023'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_Actual
# MAGIC where Financial_Year='2019'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_Recruitment_cost_actual where Financial_Year='2024'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_Recruitment_cost_plan where  Financial_Year='2024'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_Recruitment_cost_forecast where Financial_Year='2023'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_Recruitment_cost_outlook where Financial_Year='2023'
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(value),month from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_Actual where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_Actual) and value is not null and Financial_Year='2024'
# MAGIC group by month
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(value),month from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_outlook where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_outlook) and value is not null and Financial_Year='2023'
# MAGIC group by month
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(amount),month from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_plan where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_plan) and Amount is not null and Financial_Year='2024'
# MAGIC group by month
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(value),month from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_forecast where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_forecast) and value is not null and Financial_Year='2023'
# MAGIC group by month
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(amount),month from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_plan where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_plan) and amount is not null and Financial_Year='2022'
# MAGIC group by month
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsfinancedb.trusted_hist_fr_recruitment_hiring_actual where Financial_Year='2022'--20497
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_recruitment_hiring_forecast where Financial_Year='2021'--20497
# MAGIC

# COMMAND ----------

990708-3516792

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(total) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_Actual where Financial_Year='2019' and Total in (-146533,82559)--3516792
# MAGIC --select sum(total) from kgsfinancedb.trusted_hist_fr_recruitment_team_cost_Actual where Financial_Year='2019' and Total=82559--990708
# MAGIC

# COMMAND ----------

