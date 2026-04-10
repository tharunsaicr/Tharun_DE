-- Databricks notebook source
-- select * from kgsonedatadb.config_bad_record_check
-- select * from kgsonedatadb.config_data_type_cast

select period,amount_in_dollar_000 as amount_in_dollar_000 from kgsfinancedb.trusted_hist_fr_travel_actual where Financial_Year='2024'and period='2023-11-01'

select 
period,amount_in_dollar_000
-- sum(amount_in_dollar_000) as amount_in_dollar_000 
from kgsfinancedb.raw_curr_fr_travel_actual where Financial_Year='2024'
and period='Nov 2023'

-- COMMAND ----------

select count(1) from kgsonedatadb.trusted_hist_talent_acquisition_requisition_raised where file_date='20230831'

-- COMMAND ----------

select file_date,dated_on,count(1) , '[trusted_hist_talent_acquisition_requisition_raised]' from kgsonedatadb.trusted_hist_talent_acquisition_requisition_raised group by  file_date,dated_on
union
select file_date,dated_on,count(1) , 'trusted_hist_talent_acquisition_requisition_dump' from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump group by  file_date,dated_on
union
select file_date,dated_on,count(1) ,'trusted_hist_talent_acquisition_kgs_joiners_report' from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report group by  file_date,dated_on
--[dbo].[trusted_hist_talent_acquisition_requisition_dump] 
--dbo.trusted_hist_talent_acquisition_kgs_joiners_report

-- COMMAND ----------

select file_date,dated_on,count(1) , '[trusted_hist_talent_acquisition_requisition_raised]' from kgsonedatadb.raw_hist_talent_acquisition_requisition_raised group by  file_date,dated_on
union
select file_date,dated_on,count(1) , 'trusted_hist_talent_acquisition_requisition_dump' from kgsonedatadb.raw_hist_talent_acquisition_requisition_dump group by  file_date,dated_on
union
select file_date,dated_on,count(1) ,'trusted_hist_talent_acquisition_kgs_joiners_report' from kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report group by  file_date,dated_on

-- COMMAND ----------

select * from kgsonedatadb.config_data_type_cast where Process_Name='legal'

-- COMMAND ----------

select distinct file_date from kgsonedatadb.trusted_hist_lnd_glms_kva_details order by file_date desc

-- COMMAND ----------

select count(*) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where file_date=20230801

-- COMMAND ----------

select count(*) from kgsonedatadb.trusted_hist_headcount_Monthly_Employee_Details where file_date=20230919

-- COMMAND ----------

select count(*) from kgsonedatadb.raw_hist_headcount_Monthly_Employee_Details where file_date=20230919

-- COMMAND ----------

select count(*) from kgsonedatadb.trusted_hist_headcount_Sabbatical
 where file_date=20231003
   select count(*) from kgsonedatadb.trusted_hist_headcount_Secondee_Outward
 where file_date=20231003

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.window import Window

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table('kgsonedatadb.config_dim_training_category').select('ItemTitle','TrainingCategory').where(col('ItemTitle').like('%WD HCM%'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=df.withColumn('ItemTitle',regexp_replace(col('ItemTitle'), '\n', ' '))
-- MAGIC df1=df1.withColumn('row_num',row_number().over(Window.partitionBy(df1.ItemTitle).orderBy(df1.ItemTitle))).filter(col('row_num')==1).drop('row_num')
-- MAGIC display(df1.select('*',length(col('ItemTitle'))))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_fact=spark.sql("select distinct Item_Title,Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Item_Title like '%WD HCM 2023%' and Item_Title not like 'KVA%'")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_fact)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_fact1=df_fact.withColumn('Item_Title',regexp_replace(col('Item_Title'), '\n', ' '))
-- MAGIC # display(df_fact1)
-- MAGIC display(df_fact1.select('*',length(col('Item_Title'))))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_fact1=df_fact1.withColumn('Item_Title',trim('Item_Title'))
-- MAGIC df_fact1=df_fact1.withColumn('row_num',row_number().over(Window.partitionBy(df_fact1.Item_Title).orderBy(df_fact1.Item_Title))).filter(col('row_num')==1).drop('row_num')
-- MAGIC display(df_fact1.select('*',length(col('Item_Title'))))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_fact2=df_fact1.withColumn('Item_Title_1',trim('Item_Title'))
-- MAGIC display(df_fact2.select(length(col('Item_Title_1'))))

-- COMMAND ----------

-- join the two tables on the 'item_title' column with new line characters replaced
joined_table = table1.join(table2,regexp_replace(table1['item_title'], '\n', ' ') == regexp_replace(table2['item_title'], '\n', ' '),'left')

-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_lnd_glms_kva_details

-- COMMAND ----------

