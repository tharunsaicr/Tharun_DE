# Databricks notebook source
# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners --66
# MAGIC -- select distinct BGV_Status,BGV_Reference_No,`Candidate_Email/Candidate_Email_Perosnal_Email` from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where BGV_Status is Null

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table kgsonedatadb.trusted_hist_bgv_upcoming_joiners

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select KPMG_Ref_No,External_Status from kgsonedatadb.trusted_hist_bgv_kcheck where Email in ('rr.krishna26@gmail.com',
# MAGIC 'village.naveen@gmail.com',
# MAGIC 'abhaynikita2020@gmail.com',
# MAGIC 'nivedita.maloo@gmail.com',
# MAGIC 'nayanaannbiju123@gmail.com',
# MAGIC 'saranyanagaiah@gmail.com',
# MAGIC 'shaluranasr1993@gmail.com',
# MAGIC 'harishmj08@gmail.com',
# MAGIC 'bharatibaishnavi100@gmail.com',
# MAGIC 'arshitagarwal9933@gmail.com',
# MAGIC 'farzan.shaikh97@live.com',
# MAGIC 'tony03alphina@gmail.com',
# MAGIC 'sujithasathyan21@gmail.com')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_cost_center_business_unit where Cost_Centre in ('DA Core – Infra Capital Projects & Asset management',
# MAGIC 'DA Core – Infra Commercial Advisory')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_data_type_cast

# COMMAND ----------

