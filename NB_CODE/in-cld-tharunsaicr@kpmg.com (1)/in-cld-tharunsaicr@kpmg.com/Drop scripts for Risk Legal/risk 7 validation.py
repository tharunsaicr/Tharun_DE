# Databricks notebook source
# MAGIC %sql
# MAGIC select * from kgsonedatadb_badrecords.trusted_hist_risk_intune_compliance_bad where File_Date='20240422' and left(Dated_On,10)='2024-04-23' and COMPLIANCE='Compliant'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_risk_intune_compliance

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb_badrecords.risk_exit_report_bad where File_Date='20230919'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb_badrecords.risk_late_arrival_to_it_bin_bad

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_bad_record_check where Process_Name='risk'

# COMMAND ----------

# MAGIC %sql
# MAGIC select PRIMARY_USER_EMAIL_ADDRESS,count(PRIMARY_USER_EMAIL_ADDRESS) from kgsonedatadb.trusted_risk_intune_compliance group by PRIMARY_USER_EMAIL_ADDRESS having count(*) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_risk_intune_compliance

# COMMAND ----------

