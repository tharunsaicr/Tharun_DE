# Databricks notebook source


# COMMAND ----------

# -- delete from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker_resigned where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_resigned_bad  where File_Date = '20230316';


# -- delete from kgsonedatadb.raw_hist_headcount_monthly_employee_details where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_employee_details_bad  where File_Date = '20230316';



# -- delete from kgsonedatadb.raw_hist_headcount_monthly_resigned_and_left where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_resigned_and_left_bad  where File_Date = '20230316';


# -- delete from kgsonedatadb.raw_hist_headcount_monthly_sabbatical where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_sabbatical_bad  where File_Date = '20230316';

# -- delete from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_bad  where File_Date = '20230316';

# -- delete from kgsonedatadb.raw_hist_headcount_monthly_academic_trainee where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_academic_trainee_bad  where File_Date = '20230316';


# -- delete from kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_from_ki where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_from_ki_bad  where File_Date = '20230316';


# -- delete from kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_resigned where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_resigned_bad  where File_Date = '20230316';

# -- delete from kgsonedatadb.raw_hist_headcount_monthly_secondee_outward where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_secondee_outward_bad  where File_Date = '20230316';

# -- delete from kgsonedatadb.raw_hist_headcount_monthly_maternity_cases where File_Date = '20230316';
# -- delete from kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases where File_Date = '20230316';
# -- delete from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_maternity_cases_bad  where File_Date = '20230316';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- select File_Date,count(1) from 
# MAGIC -- -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_employee_details_bad 
# MAGIC -- -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_bad
# MAGIC -- -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_from_ki_bad
# MAGIC -- -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_resigned_bad
# MAGIC -- -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_resigned_bad
# MAGIC
# MAGIC
# MAGIC -- group by File_Date order by File_Date desc
# MAGIC
# MAGIC -- order by File_Date
# MAGIC
# MAGIC
# MAGIC select * from 
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_employee_details_bad 
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_from_ki_bad
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_resigned_bad
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_bad
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_resigned_bad -- Need to fix this
# MAGIC
# MAGIC
# MAGIC where Candidate_Id not like 'NULL%'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Cost_centre,BU,count(1) from kgsonedatadb.config_cost_center_business_unit group by Cost_centre,BU having count(1)>1

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct YEAR(replace(Start_Date,'00','20')) from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned where File_Date in ('20191022','20210216') and YEAR(Start_Date) NOT BETWEEN '1900' AND '2030'
# MAGIC
# MAGIC -- select distinct YEAR(replace(Start_Date,'00','20')) from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned where File_Date in ('20191022','20210216') and YEAR(Start_Date) NOT BETWEEN '1900' AND '2030'
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned set Start_Date  = replace(Start_Date,'00','20') where File_Date in ('20191022','20210216') and YEAR(Start_Date) NOT BETWEEN '1900' AND '2030'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct Date_First_Hired from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where File_Date = '20210420' 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Start_Date,File_Date from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SELECT * FROM kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left WHERE Employee_Number IN (
# MAGIC -- SELECT Employee_Number
# MAGIC -- FROM kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left
# MAGIC -- GROUP BY Employee_Number
# MAGIC -- HAVING COUNT(Employee_Number) > 1)
# MAGIC
# MAGIC select Date_First_Hired,Termination_Date,File_Date,* from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left WHERE Employee_Number IN (
# MAGIC SELECT Employee_Number
# MAGIC FROM kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left
# MAGIC GROUP BY Employee_Number
# MAGIC HAVING COUNT(Employee_Number) > 1) order by Employee_Number,File_Date desc
# MAGIC
# MAGIC -- Move Employee from Resign and Left to Employee_Details for old record
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Location from kgsonedatadb.trusted_hist_headcount_monthly_employee_details --where File_Date like '%202104%'
# MAGIC
# MAGIC -- Fix data in kgsonedatadb.trusted_hist_headcount_monthly_employee_details - FileDate 20200721  -- Data Misplace BU had Email ID
# MAGIC -- Relaod Data and fix Date format in Date First Hired - employee_details - FileDate - 20210420
# MAGIC -- Need to change BU Digital Nexus to DN and Cap-Hubs to CH in every Dimention table where there is BU
# MAGIC -- Gurgaon or Gurugram are both present in Employee Detail, Keep any one. --Gurugram
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump WHERE YEAR(Date_First_Hired) NOT BETWEEN '1900' AND '2030'
# MAGIC
# MAGIC select distinct File_Date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details WHERE YEAR(Date_First_Hired) NOT BETWEEN '1900' AND '2030'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Date_First_Hired,File_Date from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical where File_Date = '20210318'
# MAGIC -- EMployee_Number = '35019'
# MAGIC -- 4/14/2014
# MAGIC
# MAGIC -- 2014-04-14
# MAGIC -- 2019-08-08

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical where File_Date = '20210318'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Start_Date,File_Date from 
# MAGIC kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_sabbatical
# MAGIC -- Leave_Start_Date
# MAGIC -- Leave_End_Date
# MAGIC
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_sabbatical
# MAGIC -- Date_First_Hired
# MAGIC -- 20210318
# MAGIC -- 20210519 - only 1 record -Done
# MAGIC
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases
# MAGIC -- Start_Date
# MAGIC -- 20200318

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select File_Date,count(1) from 
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_employee_details 
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_sabbatical
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker
# MAGIC kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases
# MAGIC
# MAGIC
# MAGIC group by File_Date order by File_Date desc
# MAGIC
# MAGIC -- order by File_Date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from 
# MAGIC --kgsonedatadb.raw_hist_headcount_monthly_contingent_worker_resigned 
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_resigned_bad
# MAGIC -- kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_resigned
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_resigned_bad
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_Loaned_staff_from_KI_bad
# MAGIC kgsonedatadb.trusted_hist_headcount_monthly_Loaned_staff_from_KI
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_headcount_monthly_Employee_Details_bad
# MAGIC -- kgsonedatadb.raw_hist_headcount_monthly_Employee_Details
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_Employee_Details
# MAGIC where File_Date = '20210120' 
# MAGIC -- and Employee_Name='54020'
# MAGIC and Email_Id='rachitgupta@kpmg.com'
# MAGIC -- and Employee_Number like '%New%' 
# MAGIC
# MAGIC --or Full_Name like '%Mahajan, Amruta Govind%')

# COMMAND ----------

# %sql

# drop table kgsonedatadb.trusted_hist_bgv_upcoming_joiners

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Cost_centre,BU,count(1) from kgsonedatadb.config_cost_center_business_unit group by Cost_centre,BU having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select File_Date,Employee_Number,Cost_centre,Function,Employee_Subfunction,Employee_Subfunction_1 from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('113059',
# MAGIC '113059',
# MAGIC '121205',
# MAGIC '121209',
# MAGIC '121217',
# MAGIC '121258',
# MAGIC '121263',
# MAGIC '121272',
# MAGIC '121279',
# MAGIC '121295',
# MAGIC '121347',
# MAGIC '121388',
# MAGIC '121438',
# MAGIC '123759',
# MAGIC '130990',
# MAGIC '131076',
# MAGIC '131090',
# MAGIC '131314',
# MAGIC '131340',
# MAGIC '131467',
# MAGIC '131474',
# MAGIC '131480',
# MAGIC '131545',
# MAGIC '131546',
# MAGIC '131573',
# MAGIC '131576',
# MAGIC '131579',
# MAGIC '131582',
# MAGIC '131587',
# MAGIC '131588',
# MAGIC '131590',
# MAGIC '131591',
# MAGIC '27449',
# MAGIC '48687',
# MAGIC '77333',
# MAGIC '87334',
# MAGIC '87334',
# MAGIC '98494',
# MAGIC '98494') and File_Date ='20221214'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- truncate table 
# MAGIC -- kgsonedatadb_badrecords.trusted_hist_compensation_finance_metrics_bad
# MAGIC -- kgsonedatadb.trusted_hist_compensation_finance_metrics
# MAGIC
# MAGIC -- select File_Date,Headcount_File_Date,count(1),Dated_On from kgsonedatadb_badrecords.trusted_hist_compensation_finance_metrics_bad group by File_Date,Headcount_File_Date,Dated_On

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_compensation_finance_metrics where File_Date = '20221001'
# MAGIC select count(1) from kgsonedatadb.trusted_hist_compensation_finance_metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_compensation_finance_metrics where Cost_Centre like '%CF%'
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_compensation_finance_metrics where Cost_Centre like 'CF%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump order by File_Date
# MAGIC
# MAGIC -- kgsonedatadb.trusted_hist_headcount_monthly_employee_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select distinct Local_HR_ID,Email_Address__User_,Position,Job_Name,Level_Wise,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_Wise is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from kgsonedatadb.config_cost_center_business_unit where Cost_centre like
# MAGIC -- '%Capability Hubs-Business Services-RM%'
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_stg_lnd_kva_details  where 'Emp_Employee_Number'= '131535'
# MAGIC
# MAGIC select Email_Address,* from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('132181','132198')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct Position, JOb_Name,File_Date,File_Type from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_wise in (
# MAGIC --   --'Analysts to Seniors/Team Leaders', 
# MAGIC --   'Analysts to Seniors/Team Leaders')
# MAGIC
# MAGIC -- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details 
# MAGIC --where Level_wise in ('Analysts to Seniors/Team Leaders ')
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct Original_Levelwise_Mapping from kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC
# MAGIC -- select distinct Local_HR_ID,Cost_Center,BU from kgsonedatadb.trusted_hist_lnd_glms_kva_details where BU is Null
# MAGIC -- select Local_HR_ID,Position,Job_Name,Original_Levelwise_Mapping,Level_Wise,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Original_Levelwise_Mapping is Null
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Original_Levelwise_Mapping is null
# MAGIC --Level_Wise in ('Manager')
# MAGIC
# MAGIC select  distinct Original_Levelwise_Mapping,Level_Wise, File_Type from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Original_Levelwise_Mapping is Null and Level_Wise IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Position, Job_Name,Original_Levelwise_Mapping,Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Original_Levelwise_Mapping is null and File_Date in (
# MAGIC '20221001',
# MAGIC '20221031',
# MAGIC '20221101',
# MAGIC '20221130',
# MAGIC '20221201',
# MAGIC '20221231',
# MAGIC '20230101',
# MAGIC '20230131',
# MAGIC '20230201',
# MAGIC '20230228',
# MAGIC '20230301',
# MAGIC '20230331',
# MAGIC '20230401',
# MAGIC '20230430')
# MAGIC
# MAGIC -- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Original_Levelwise_Mapping is Null and Level_Wise IS NOT NULL and Level_Wise = 'Assistant Manager'
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set Original_Levelwise_Mapping = 'Assistant Manager' where Level_Wise = 'Assistant Manager' and Original_Levelwise_Mapping is Null

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct File_Date,File_Type from kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC
# MAGIC -- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where 
# MAGIC -- File_Date = '20230201' 
# MAGIC -- File_Date = '20230228'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select Location,Employee_Number,Position,Job_Name,Company_Name,Entity,File_Date,Cost_Centre from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('131899') order by Employee_Number, File_Date
# MAGIC
# MAGIC -- Position  = 'Consultant'
# MAGIC -- Job_Name = 'Senior'
# MAGIC
# MAGIC
# MAGIC -- select Local_HR_ID,Position,Job_Name from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date not in ('20230228') and 
# MAGIC -- Level_Wise is Null or trim(Level_Wise) = ''
# MAGIC --and BU is Null
# MAGIC -- where Email_Address__User_ = 'sapnapathania@kpmg.com'
# MAGIC -- Local_HR_Id = '131899'
# MAGIC -- -- Original_Levelwise_Mapping = 'Analysts to Seniors/Team Leaders'
# MAGIC -- -- Location ='Gurugram'
# MAGIC -- -- Position  = 'Consultant'
# MAGIC -- -- Job_Name = 'Senior'
# MAGIC -- -- Level_Wise = 'Analysts to Seniors/Team Leaders'
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set 
# MAGIC -- Original_Levelwise_Mapping = 'Analysts to Seniors/Team Leaders ',
# MAGIC -- Level_Wise = 'Analysts to Seniors/Team Leaders '
# MAGIC -- where Local_HR_Id = '131899'
# MAGIC
# MAGIC -- Location ='Gurugram',
# MAGIC -- Position  = 'Consultant',
# MAGIC -- Job_Name = 'Senior',
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_Id = '131899'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct Local_HR_ID,Position ,Job_Name,Original_Levelwise_Mapping,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where level_wise is null
# MAGIC
# MAGIC -- select distinct training_category from kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC
# MAGIC -- select distinct Item_Title, File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Training_Category is null
# MAGIC
# MAGIC select distinct Local_HR_ID,Email_Address__User_,Cost_Center,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where (BU is null) or (Cost_Center is null) and (Local_HR_ID IS NOT NULL)
# MAGIC -- Local_HR_ID is null or Email_Address__User_ is null

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct File_Date from kgsonedatadb.raw_hist_lnd_glms_details 
# MAGIC
# MAGIC -- delete from kgsonedatadb.raw_hist_lnd_glms_details 
# MAGIC --  where File_Date = '20230201'
# MAGIC
# MAGIC -- delete from 
# MAGIC --  kgsonedatadb.trusted_hist_lnd_glms_details
# MAGIC --  where File_Date = '20230201'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select Employee_Number,Position,Job_Name,Company_Name,Entity,File_Date,Cost_Centre from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('131899') order by Employee_Number, File_Date
# MAGIC
# MAGIC -- select Employee_Number,Position,Job_Name,Company_Name,Entity,File_Date from kgsonedatadb.trusted_hist_headcount_termination_dump where Employee_Number in ('131899') order by Employee_Number, File_Date
# MAGIC
# MAGIC select Employee_Number,Email_Address,Position,Job_Name,Company_Name,Entity,Cost_Centre  from  kgsonedatadb.trusted_hist_headcount_employee_dump where Email_Address  in  ('sapnapathania@kpmg.com')
# MAGIC -- ('sapnapathania@kpmg.com','mandadapuhimab@kpmg.com','dharshikgopan@kpmg.com')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select count(1) from (select *, row_number() over (partition by Employee_Number order by Employee_Number) as Row_Num from kgsonedatadb.trusted_hist_headcount_employee_dump) where Row_Num=1
# MAGIC
# MAGIC
# MAGIC select File_Date,Count(1) from (select * from (select *, row_number() over (partition by Employee_Number order by Employee_Number) as Row_Num from kgsonedatadb.trusted_hist_headcount_Termination_dump) where Row_Num=1) group by File_Date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_headcount_Termination_dump
# MAGIC -- trusted_headcount_employee_dump
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select Position,Job_Name,Count(1) from (select distinct Position, Job_Name from kgsonedatadb.trusted_hist_headcount_employee_dump
# MAGIC -- union
# MAGIC -- select distinct Position, Job_Name from kgsonedatadb.trusted_hist_headcount_termination_dump) group by Position,Job_Name having count(1)>1
# MAGIC
# MAGIC select * from (select distinct Position, Job_Name,'ED' as Source from kgsonedatadb.trusted_hist_headcount_employee_dump where Company_Name in ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited')
# MAGIC union
# MAGIC select distinct Position, Job_Name,'TD' as Source  from kgsonedatadb.trusted_hist_headcount_termination_dump where Company_Name  in ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited')) where 
# MAGIC Position in ('UK-R&C-ROCKETS.','Contract Compliance.')
# MAGIC --Position IS NOT NULL and 
# MAGIC
# MAGIC -- select distinct File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump
# MAGIC
# MAGIC -- select distinct File_Date from kgsonedatadb.trusted_hist_headcount_termination_dump
# MAGIC
# MAGIC -- select * from kgsonedatadb.config_dim_level_wise

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select t1.Position,t1.Job_Name,t2.Position as Mapping_Position,t2.Job_Name as Mapping_Job_Name, t2.Mapping as Mapping  from (select * from (select distinct Position, Job_Name from kgsonedatadb.trusted_hist_headcount_employee_dump where Company_Name in ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited')
# MAGIC union
# MAGIC select distinct Position, Job_Name  from kgsonedatadb.trusted_hist_headcount_termination_dump where Company_Name  in ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited')) where Position IS NOT NULL and Position not in ('UK-R&C-ROCKETS.','Contract Compliance.'))t1 left join kgsonedatadb.config_dim_level_wise t2 on t1.Position = t2.Position and t1.Job_Name = t2.Job_Name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SELECT distinct Is_Client_Facing
# MAGIC --   FROM kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC   -- update  kgsonedatadb.trusted_hist_lnd_glms_kva_details set Is_Client_Facing = 'Yes' where Is_Client_Facing = 'Y'
# MAGIC
# MAGIC   select distinct Company_Name, Entity from kgsonedatadb.trusted_hist_headcount_employee_dump where Company_Name in ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Year from kgsonedatadb.trusted_hist_employee_engagement_encore_output
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%' and Year = 'FY23'
# MAGIC
# MAGIC -- select count(1) from kgsonedatadb.trusted_hist_employee_engagement_encore_output where category not in ('Appreciation Month-Influencer','Festive Gift') and category not like '%Service Anniversary Award%' and Year = 'FY23' and File_Date = '20230221'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Host_Country,Travel_Model from kgsonedatadb.trusted_hist_global_mobility_secondment_tracker 
# MAGIC -- where Employee_No in ('62343',
# MAGIC -- '70418',
# MAGIC -- '70503',
# MAGIC -- '85291',
# MAGIC -- '75109',
# MAGIC -- '39469',
# MAGIC -- '53417',
# MAGIC -- '103655')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_Wise is null;
# MAGIC select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC
# MAGIC -- -- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_Wise is null
# MAGIC -- select * from Employee_Dump where select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_Wise is null
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- df_join_lnd_emp_sl_lw = df_join_lnd_emp_sl.join(df_level_wise, (df_join_lnd_emp_sl.Emp_Position == df_level_wise.lw_Position) & (df_join_lnd_emp_sl.Emp_Job_Name == df_level_wise.lw_Job_Name), 'left')
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC -- select * from kgsonedatadb.config_dim_level_wise

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table kgsonedatadb.ln_test_1  select * from (select lnd.Local_HR_ID,ed.Position as ED_Postion,ed.Job_Name as ED_Job_Name,td.Position as TD_Postion,td.Job_Name as TD_Job_Name from (select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_Wise is null) lnd left join kgsonedatadb.trusted_headcount_employee_dump ed on ed.Employee_Number = lnd.Local_HR_ID left join kgsonedatadb.trusted_headcount_termination_dump td on lnd.Local_HR_ID=td.Employee_Number)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_headcount_termination_dump where Employee_Number in (select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Level_Wise is null and Local_HR_ID not like '%KPMG%')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table 
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_encore_output
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_gps
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_rock
# MAGIC --kgsonedatadb.trusted_hist_employee_engagement_year_end

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select File_Date,dated_on,count(1) from
# MAGIC  kgsonedatadb.trusted_hist_employee_engagement_encore_output
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_gps
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_rock
# MAGIC --kgsonedatadb.trusted_hist_employee_engagement_year_end
# MAGIC
# MAGIC group by File_Date,Dated_On order by File_Date asc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Tenure_Length_of_Service from kgsonedatadb.trusted_hist_employee_engagement_gps

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number = '111478'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end where Function_tag = 'null' or Function_tag IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Location,* from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in (select distinct 
# MAGIC -- Emp_ID  from kgsonedatadb.trusted_hist_employee_engagement_year_end where Location = 'KGDCPL SEZ Bangalore 4th Floor-Wing1') and File_Date = '20230228'
# MAGIC
# MAGIC
# MAGIC select distinct 
# MAGIC Work_experience_prior_to_KPMG__Years_
# MAGIC   from kgsonedatadb.trusted_hist_employee_engagement_year_end 
# MAGIC --where Location = 'KGDCPL SEZ Bangalore 4th Floor-Wing1'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Work_experience_prior_to_KPMG__Years_ from  kgsonedatadb.trusted_hist_compensation_yec

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end --53456
# MAGIC
# MAGIC -- select distinct File_Date from kgsonedatadb.trusted_hist_employee_engagement_year_end 
# MAGIC
# MAGIC -- truncate table kgsonedatadb.trusted_hist_employee_engagement_year_end 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_dim_level_wise --where Position in ('Partner COO','Associate Partner')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Local_HR_ID, Emp_Position,lw_Job_Name from kgsonedatadb.trusted_hist_lnd_glms_details where Level_Wise is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select 
# MAGIC -- distinct Position,Job_Name 
# MAGIC
# MAGIC -- from kgsonedatadb.trusted_headcount_employee_dump 
# MAGIC -- where Employee_Number in  (select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_details where Level_Wise is null)
# MAGIC
# MAGIC -- select * from (select 
# MAGIC -- distinct Position ,Job_Name 
# MAGIC
# MAGIC -- from kgsonedatadb.trusted_headcount_employee_dump 
# MAGIC -- where Employee_Number in  (select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_details where Level_Wise is null)) ed left join kgsonedatadb.config_dim_level_wise lw on ed.Position=lw.Position --ed.Job_Name = lw.Job_Name --and 
# MAGIC
# MAGIC select * from (select 
# MAGIC distinct Position ,Job_Name 
# MAGIC
# MAGIC from kgsonedatadb.trusted_headcount_termination_dump 
# MAGIC where Employee_Number in  (select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_details where Level_Wise is null)) td left join kgsonedatadb.config_dim_level_wise lw on td.Position=lw.Position and td.Job_Name = lw.Job_Name

# COMMAND ----------



# COMMAND ----------

# from datetime import *
# import random
# import numpy as np
# import pandas as pd
# from pyspark.sql.types import IntegerType
# # from datetime import timedelta

# def get_count_working_days(startDate,endDate):
#     # busniness days including Start date and End Date
#     businessDayCount = pd.bdate_range(startDate,endDate)
#     return len(businessDayCount)

# get_count_working_days_udf = udf(get_count_working_days,IntegerType())

# COMMAND ----------

import datetime
import random
import numpy as np
from pyspark.sql.types import IntegerType

df = spark.sql("select Employee_Number, Leave_Start_Date ,Leave_End_Date FROM kgsonedatadb.trusted_headcount_leave_report where Employee_Number = '85312'")

df_workingdays = df.withColumn("working_days_count", get_count_working_days_udf(df['Leave_Start_Date'],df['Leave_End_Date']))

df_workingdays = 

display(df_workingdays)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details --355501  --361304(latest count 4/26/2023)
# MAGIC
# MAGIC -- select Item_Title,Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Training_Category is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Local_HR_ID,Cost_Center,BU from kgsonedatadb.trusted_hist_lnd_glms_kva_details 
# MAGIC where 
# MAGIC -- --Cost_Center is null or Cost_Center=''
# MAGIC (BU is null or BU = 'null') --and Cost_Center in ('TS-FDD','Capability Hubs-Resourcing-RM')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC -- delete
# MAGIC  from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in (select Employee_Number from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date = '20230228' group by Employee_Number having count(1)>1) and File_Date = '20230228' 
# MAGIC and Dated_On = (select max(Dated_On) from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date = '20230228')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- 20221017
# MAGIC -- 20221118
# MAGIC -- 20221213
# MAGIC -- 20230118
# MAGIC -- 20230228
# MAGIC select Employee_Number,count(1) from kgsonedatadb.trusted_hist_headcount_termination_dump where File_Date = '20230228' group by Employee_Number having count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Cost_Center from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Bu is null
# MAGIC
# MAGIC -- select distinct Item_Title,File_Type,File_Date from  kgsonedatadb.trusted_hist_lnd_glms_kva_details 
# MAGIC -- where 
# MAGIC -- Training_Category is null
# MAGIC --  or 
# MAGIC -- Training_Category like '%null%'
# MAGIC
# MAGIC
# MAGIC -- select File_Date,Item_Title,File_Type,count(1) from (select * from  kgsonedatadb.trusted_hist_lnd_glms_kva_details where 
# MAGIC -- Training_Category is null
# MAGIC --  or 
# MAGIC -- Training_Category like '%null%') group by File_Date,Item_Title,File_Type --having count(1)=1
# MAGIC
# MAGIC select 
# MAGIC Item_Title,Training_Category,File_Date from (select distinct(Item_Title),File_Date,Training_Category ,row_number() over(PARTITION BY Item_Title ORDER BY File_Date) as rw_num from kgsonedatadb.trusted_hist_lnd_glms_kva_details where 
# MAGIC Training_Category is null
# MAGIC  or 
# MAGIC Training_Category like '%null%') where  rw_num =1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_dim_training_category where ItemTitle = 'Security & Privacy Exam'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_employee_engagement_rock where Employee_Number IN (select Employee_Number from kgsonedatadb.trusted_hist_employee_engagement_rock group by File_Date,employee_number having count(1)>1 )
# MAGIC
# MAGIC -- (select File_Date,employee_number,count(1) from kgsonedatadb.trusted_hist_employee_engagement_rock group by File_Date,employee_number having count(1)>1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_lnd_glms_details where BU IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- truncate table kgsonedatadb.trusted_hist_employee_engagement_rock

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Count(1), File_Date from kgsonedatadb.trusted_hist_employee_engagement_encore_output
# MAGIC  group by File_Date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Rock_Tenure from kgsonedatadb.trusted_hist_employee_engagement_rock

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_date from 
# MAGIC kgsonedatadb.trusted_hist_employee_engagement_rock
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_thanks_dump
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_encore_output
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_gps
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_year_end
# MAGIC -- kgsonedatadb.trusted_hist_compensation_yec

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_date from 
# MAGIC -- kgsonedatadb.trusted_hist_employee_engagement_encore_output
# MAGIC -- kgsonedatadb.trusted_hist_compensation_yec
# MAGIC
# MAGIC -- kgsonedatadb.trusted_hist_headcount_leave_report
# MAGIC kgsonedatadb.trusted_hist_headcount_termination_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_global_mobility_secondment_details where Host_Country is null
# MAGIC
# MAGIC select distinct File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC CASE when End_Date == ' ' then NULL else End_Date END as End_Date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details
# MAGIC
# MAGIC
# MAGIC select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,CASE when End_Date == ' ' then NULL else End_Date END as End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,Remarks,Current_Base_Location_of_the_Candidate,Office_Location_in_the_KGS_Offer_Letter,WFA_Option__Permanent_12_Months_,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1)
# MAGIC from kgsonedatadb.trusted_hist_headcount_monthly_employee_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select len(Date_of_Joining) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details  where end_date = ' '

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct end_date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details  where end_date like '%1900%'
# MAGIC --where Employee_Number ='100002'
# MAGIC --where end_date like '%2024%'
# MAGIC
# MAGIC
# MAGIC select count(1)
# MAGIC from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where End_Date like '%1900%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Candidate_Id from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- alter table kgsonedatadb.trusted_hist_lnd_glms_kva_details RENAME COLUMN Cost_Center_1 TO Cost_Center;
# MAGIC -- alter table kgsonedatadb.trusted_lnd_glms_kva_details RENAME COLUMN Cost_Centre TO Cost_Center_1
# MAGIC
# MAGIC -- ALTER TABLE kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC -- DROP COLUMN Cost_Center
# MAGIC
# MAGIC -- alter table kgsonedatadb.trusted_hist_lnd_glms_kva_details SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')
# MAGIC
# MAGIC -- alter table kgsonedatadb.trusted_lnd_glms_kva_details SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- drop table kgsonedatadb.trusted_global_mobility_secondment_details;
# MAGIC -- drop table kgsonedatadb.trusted_hist_global_mobility_secondment_details;
# MAGIC -- drop table kgsonedatadb.trusted_stg_global_mobility_secondment_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_adhoc_convert_column_to_date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from 
# MAGIC kgsonedatadb.trusted_hist_global_mobility_secondment_details
# MAGIC -- kgsonedatadb.trusted_hist_global_mobility_secondment_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Cost_Centre
# MAGIC -- ,cost_centre,File_Type 
# MAGIC from kgsonedatadb.trusted_hist_lnd_glms_kva_details 
# MAGIC
# MAGIC -- select distinct File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from 
# MAGIC kgsonedatadb.trusted_hist_global_mobility_secondment_details
# MAGIC -- kgsonedatadb.trusted_hist_global_mobility_secondment_tracker
# MAGIC -- kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report
# MAGIC -- kgsonedatadb.trusted_hist_bgv_progress_sheet
# MAGIC -- kgsonedatadb.trusted_hist_bgv_upcoming_joiners

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Inbound___Outbound,Home_Country,Host_Country,Assignment_Start_Date from kgsonedatadb.trusted_hist_global_mobility_secondment_tracker

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select File_Date,count(1) from kgsonedatadb.trusted_hist_employee_engagement_encore_output group by File_Date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bu_update_table_list

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_cost_center_business_unit where Cost_centre in ('Audit-Corporates Listed and Regulated', 'MC-EWT-SAP-Logistics')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct cost_centre,Bu from (
# MAGIC select Employee_Number,Cost_centre,BU,File_Date from kgsonedatadb.trusted_hist_headcount_employee_details where Employee_Number in ('108080',
# MAGIC '118152',
# MAGIC '46386',
# MAGIC '129673',
# MAGIC '87442',
# MAGIC '120091',
# MAGIC '121530',
# MAGIC '131969',
# MAGIC '23799',
# MAGIC '123102',
# MAGIC '18998',
# MAGIC '52288',
# MAGIC '112285',
# MAGIC '132189',
# MAGIC '70583',
# MAGIC '121384',
# MAGIC '132843',
# MAGIC '132409',
# MAGIC '41196',
# MAGIC '112754',
# MAGIC '128340',
# MAGIC '125637',
# MAGIC '122158',
# MAGIC '132839',
# MAGIC '131535',
# MAGIC '106770',
# MAGIC '111453',
# MAGIC '124135',
# MAGIC '119115',
# MAGIC '96794',
# MAGIC '132157',
# MAGIC '132830',
# MAGIC '123671',
# MAGIC '131793',
# MAGIC '132877',
# MAGIC '132368',
# MAGIC '126932',
# MAGIC '132435',
# MAGIC '131034',
# MAGIC '59294',
# MAGIC '38693',
# MAGIC '50895',
# MAGIC '72863',
# MAGIC '114509',
# MAGIC '117296',
# MAGIC '109877',
# MAGIC '131796',
# MAGIC '130768',
# MAGIC '131943',
# MAGIC '132840',
# MAGIC '56406',
# MAGIC '83851',
# MAGIC '94576',
# MAGIC '45634',
# MAGIC '132597',
# MAGIC '105907',
# MAGIC '92767',
# MAGIC '104100',
# MAGIC '130686',
# MAGIC '125598',
# MAGIC '132709',
# MAGIC '125907',
# MAGIC '101913',
# MAGIC '132223',
# MAGIC '129134',
# MAGIC '108546',
# MAGIC '132414',
# MAGIC '131899',
# MAGIC '56799',
# MAGIC '127739',
# MAGIC '114992',
# MAGIC '108393',
# MAGIC '109986',
# MAGIC '88033',
# MAGIC '116146',
# MAGIC '123683') and BU IS NOT NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_date from kgsonedatadb.trusted_hist_headcount_employee_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Cost_centre from kgsonedatadb.trusted_hist_compensation_finance_metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC -- distinct Location 
# MAGIC -- distinct File_Date
# MAGIC distinct Local_HR_ID
# MAGIC from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Location in ('KGDCPL SEZ Bangalore 4th Floor-Wing1') 
# MAGIC -- or Location is Null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC * 
# MAGIC -- distinct Location
# MAGIC -- from kgsonedatadb.employee_golden_data
# MAGIC from kgsonedatadb.trusted_hist_headcount_monthly_employee_details
# MAGIC where Employee_Number = '130547'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC *
# MAGIC -- distinct Location
# MAGIC from kgsonedatadb.employee_golden_data where Employee_Number = '130547'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Employee_Number,File_Date, count(1) from kgsonedatadb.trusted_hist_headcount_maternity_cases group by File_Date,Employee_Number having count(1)>1
# MAGIC
# MAGIC -- select distinct Dated_On from kgsonedatadb.trusted_hist_headcount_maternity_cases where File_Date = '20221018'
# MAGIC
# MAGIC -- DELETE FROM kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned where Dated_On = (SELECT MAX(Dated_On) as Dated_On from  
# MAGIC --   kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned)
# MAGIC
# MAGIC -- delete FROM kgsonedatadb.trusted_hist_headcount_maternity_cases where Dated_On <> (SELECT MIN(Dated_On) as Dated_On from kgsonedatadb.trusted_hist_headcount_maternity_cases where File_Date = '20221018') and  File_Date = '20221018'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Employee_Number,File_Date, count(1) from kgsonedatadb.trusted_hist_headcount_sabbatical group by File_Date,Employee_Number having count(1)>1
# MAGIC
# MAGIC -- select distinct Dated_On from kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date = '20230112';
# MAGIC
# MAGIC
# MAGIC -- select Dated_On ,count(1) from (select * from kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date = '20230112') group by Dated_On
# MAGIC -- DELETE FROM kgsonedatadb.kgsonedatadb.trusted_hist_headcount_sabbatical where Dated_On = (SELECT MAX(Dated_On) as Dated_On from  
# MAGIC --   kgsonedatadb.trusted_hist_headcount_sabbatical)
# MAGIC
# MAGIC
# MAGIC -- select *  FROM kgsonedatadb.trusted_hist_headcount_sabbatical where Dated_On <> (SELECT MIN(Dated_On) as Dated_On from kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date = '20230112') and  File_Date = '20230112'
# MAGIC
# MAGIC
# MAGIC -- select Employee_Number,File_Date, count(1)  from (select * from kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date = '20221018') group by File_Date,Employee_Number having count(1)>1
# MAGIC
# MAGIC
# MAGIC -- delete from kgsonedatadb.trusted_hist_headcount_sabbatical where Dated_On = (SELECT MAX(Dated_On) as Dated_On from    kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date = '20230112') and File_Date = '20230112'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_compensation_yec
# MAGIC where EMP_ID='101549'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from 
# MAGIC -- kgsonedatadb.config_bad_record_check where process_Name like '%comp%'
# MAGIC kgsonedatadb.config_data_type_cast 
# MAGIC where process_Name like '%eng%'
# MAGIC
# MAGIC -- update kgsonedatadb.config_data_type_cast  set Type_Cast_To = 'string' where Process_Name ='employee_engagement' and  Delta_Table_Name = 'encore_output' and Column_Name = 'Year'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bu_update_table_list where Table_Name = "offer_release"
# MAGIC
# MAGIC -- update kgsonedatadb.config_bu_update_table_list set Column_Name = 'Business_Unit___Name' where Table_Name = "offer_release"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Last_Hire_Date,File_Type,File_Date
# MAGIC -- -- ,Local_HR_ID  
# MAGIC -- from kgsonedatadb.trusted_hist_lnd_glms_kva_details order by File_Date
# MAGIC
# MAGIC select Local_HR_ID,File_Date,Last_Hire_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Last_Hire_Date ='0013-08-19'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Last_Hire_Date,File_Date,Local_HR_ID from kgsonedatadb.raw_hist_lnd_glms_details where File_Date = '20221001'
# MAGIC
# MAGIC select Last_Hire_Date,File_Date,Local_HR_ID from kgsonedatadb.raw_hist_lnd_glms_details where File_Date = '20221001'
# MAGIC and Local_HR_ID = '31665'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_bu_update_table_list

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct(Employee_Category) from kgsonedatadb.trusted_hist_headcount_employee_dump --where employee_Number = '100080'

# COMMAND ----------

