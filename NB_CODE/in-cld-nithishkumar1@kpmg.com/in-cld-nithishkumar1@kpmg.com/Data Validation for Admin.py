# Databricks notebook source
# List all tables in the kgsfinancedb schema
tables = spark.sql("SHOW TABLES IN kgsfinancedb")
 
# Filter tables with 'admin' in their name
admin_tables = tables.filter(tables.tableName.contains("raw_hist_fr_admin"))
 
# Extract the table names
admin_table_names = [row.tableName for row in admin_tables.collect()]
 
# Print the table names
print(admin_table_names)

# COMMAND ----------

from datetime import datetime
 
# Define the cutoff date
cutoff_date = datetime(2024, 4, 30).strftime('%Y-%m-%d')
 
# Iterate over admin tables
for table_name in admin_table_names:
    # Construct delete query
    delete_query = f"DELETE FROM kgsfinancedb.{table_name} WHERE Dated_On > '{cutoff_date}'"
 
    # Execute delete query using Spark SQL
    spark.sql(delete_query)

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/install_mssql

# COMMAND ----------


from datetime import datetime
 

 
# Define the cutoff date
cutoff_date = datetime(2024, 4, 30).strftime('%Y-%m-%d')
 
# Iterate over admin tables
for table_name in admin_table_names:
    # Construct delete query
    delete_query = f"DELETE FROM dbo.{table_name} WHERE dated_on > '{cutoff_date}'"
 
    # Execute delete query
    cursor = conn.cursor()
    cursor.execute(delete_query)
    conn.commit()
    cursor.close()
 
# Close the connection
conn.close()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct dated_on,financial_year from kgsfinancedb.trusted_hist_fr_admin_seats_forecast

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(value),sum(value),Financial_year,Month from kgsfinancedb.trusted_curr_fr_admin_seats_plan group by Financial_year,Month having Financial_Year=2024

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsfinancedb.trusted_hist_fr_admin_facility_cost_location_actual

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/install_mssql

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(value),Financial_Year from kgsfinancedb.trusted_hist_fr_admin_seats_actual group by Financial_Year

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(value),sum(value),Financial_year,Month from kgsfinancedb.trusted_hist_fr_admin_hc_plan group by Financial_year,Month having Financial_Year=2024

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Financial_Year from kgsfinancedb.trusted_hist_fr_admin_hc_actual --where Financial_Year=2022

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsfinancedb.trusted_curr_fr_admin_facility_cost_location_actual

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(net_amount) from kgsfinancedb.trusted_hist_fr_admin_facility_bu_geo_plan where Financial_Year=2024

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(net_amount) from kgsfinancedb.trusted_hist_fr_admin_transport_bu_geo_actual where Financial_Year='2023'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_admin_facility_bu_geo_forecast where Financial_Year='2023'

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Amount) from kgsfinancedb.trusted_hist_fr_admin_facility_bu_geo_plan where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_facility_bu_geo_plan ) and Amount is not null and Financial_Year ='2024' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from kgsfinancedb.trusted_hist_fr_admin_facility_bu_geo_plan where Financial_Year='2023'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsfinancedb.trusted_hist_fr_admin_facility_bu_geo_forecast where Financial_Year='2022'

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Amount) from kgsfinancedb.trusted_hist_fr_admin_facility_bu_geo_forecast where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_facility_bu_geo_forecast ) and Amount is not null and Financial_Year ='2023' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Amount) from kgsfinancedb.trusted_hist_fr_admin_transport_bu_geo_plan where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_transport_bu_geo_plan ) and Amount is not null and Financial_Year ='2024' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Amount) from kgsfinancedb.trusted_hist_fr_admin_transport_bu_geo_plan where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_transport_bu_geo_plan ) and Amount is not null and Financial_Year ='2024' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_hist_fr_admin_transport_bu_geo_actual where Financial_Year='2024'

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_seats_actual where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_seats_actual ) and Value is not null and Financial_Year ='2023' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_seats_plan where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_seats_plan ) and Value is not null and Financial_Year ='2022' group by Month 
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsfinancedb.trusted_hist_fr_admin_seats_plan where Financial_Year ='2023'

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_seats_forecast where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_seats_forecast ) and Value is not null and Financial_Year ='2023' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select  sum(Value) from kgsfinancedb.trusted_hist_fr_admin_hc_actual 

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_hc_actual where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_hc_actual ) and Value is not null and Financial_Year ='2022' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_hc_plan where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_hc_plan ) and Value is not null and Financial_Year ='2023' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_hc_forecast where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_hc_forecast ) and Value is not null and Financial_Year ='2023' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_facility_cost_location_actual where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_facility_cost_location_actual ) and Value is not null and Financial_Year ='2023' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_facility_cost_location_plan where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_facility_cost_location_plan ) and Value is not null and Financial_Year ='2023' group by Month 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select Month, count(Month), sum(Value) from kgsfinancedb.trusted_hist_fr_admin_facility_cost_location_forecast where Month in (SELECT DISTINCT(month) from kgsfinancedb.trusted_hist_fr_admin_facility_cost_location_forecast ) and Value is not null and Financial_Year ='2023' group by Month 
# MAGIC

# COMMAND ----------

