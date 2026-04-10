-- Databricks notebook source
select distinct Leave_Start_Date,Leave_End_Date from kgsonedatadb.raw_hist_headcount_leave_report where file_date = '20240422'

-- COMMAND ----------

select distinct Cost_centre,BU, Final_BU from kgsonedatadb.config_hist_cc_bu_sl where file_date = '20240320' and Cost_centre in ('Capability Hubs-KM-GDN People Enablement 2','Audit-Operations')
-- like '%GDC%Audit%AMDC%'
-- union all
select distinct Cost_centre,BU from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where file_date = '20240320' and Cost_centre like '%GDC%Audit%AMDC%'
-- in ('Advisory Operations','Capability Hubs-Research-Legal Support','KGS Adv Ops-Consulting BU Dedicated Support','KGS Adv Ops-Consulting Dedicated Support','KGS Adv Ops-Digital Nexus Dedicated Support','KGS Adv Ops-F&A Dedicated Support','KGS Adv Ops-RAS BU Dedicated Support','KGS Adv Ops-RAS Dedicated Support','KGS-Adv-Ops-Centralized Reporting & US Ops','KGS-Adv-Ops-Dedicated Support','KGS-Leads  Dedicated Ops','KGS-Leeds Dedicated Ops')

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_bgv_progress_sheet where Personal_Email_ID = 'Prakbeno@gmail.com'
-- ReferanceNo_ like  '%KPMG%00154326%KGS%2019' or Referance_No_ = '%KPMG%00154326%KGS%2019' order by Case_Initiation_Date desc

-- COMMAND ----------

-- delete from kgsonedatadb.raw_hist_headcount_employee_dump where left(File_Date,6) = '202402'
-- group by File_Date,Dated_On;
select File_Date,Dated_On,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump group by File_Date,Dated_On

select Employee_number, count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = '20240129' group by Employee_number having count(1)>1

-- drop table kgsonedatadb.temp_ED
-- select distinct * from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = '20240129' and employee_number = '51300'
-- select File_Date,Dated_On,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump where left(File_Date,6) = '202402' group by File_Date,Dated_On;

-- delete from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = '20240129' and employee_number = '51300'
-- insert into kgsonedatadb.trusted_hist_headcount_employee_dump select * from kgsonedatadb.temp_ED

select * from kgsonedatadb.raw_hist_headcount_employee_dump where file_date = '20240129' and employee_number = '51300'

select * from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = '20240129' and  employee_number in ('122493',
'23277',
'123347',
'106381',
'105209',
'140350',
'88255',
'143134')

-- delete from kgsonedatadb.trusted_hist_headcount_employee_dump where file_date = '20240129' and  employee_number in ('122493',
-- '23277',
-- '123347',
-- '106381',
-- '105209',
-- '140350',
-- '88255',
-- '143134') and Person_Type_Flag is null

-- COMMAND ----------

select B.Position,round(A.WORK_EXPERIENCE_PRIOR_TO_KPMG__YY_MM_FORMAT_),round(a.YEARS_AT_CURRENT_DESIGNATION) from kgsonedatadb.trusted_compensation_yec A
left join kgsonedatadb.trusted_headcount_monthly_employee_details B on A.EMPLOYEE_NUMBER = B.Employee_Number
where B.BU = 'Consulting' and B.Position in ('Assistant Manager','Manager')
group by B.Position,round(A.WORK_EXPERIENCE_PRIOR_TO_KPMG__YY_MM_FORMAT_),round(a.YEARS_AT_CURRENT_DESIGNATION) 
order by B.Position desc,round(A.WORK_EXPERIENCE_PRIOR_TO_KPMG__YY_MM_FORMAT_) desc,round(a.YEARS_AT_CURRENT_DESIGNATION) desc

select * from kgsonedatadb.trusted_headcount_monthly_employee_details where full_name like '%Sandesh%'

-- COMMAND ----------

with recursive_cte as 
(
  select employee_number,Full_Name from kgsonedatadb.trusted_headcount_employee_details where Email_Address = 'anandsaravanan@kpmg.com'
  union all
  select E.employee_number,E.Full_Name from kgsonedatadb.trusted_headcount_employee_details E join recursive_cte r on E.Performance_Manager = r.employee_number
)
select * from recursive_cte

-- COMMAND ----------

select Designation,round(Years_in_the_Firm_as_on_30_Sep_Current_FY),count(1) from kgsonedatadb.trusted_employee_engagement_year_end where BU = 'Consulting' and Designation in ('Assistant Manager','Manager') group by Designation,round(Years_in_the_Firm_as_on_30_Sep_Current_FY);

-- COMMAND ----------

select * from kgsonedatadb.raw_hist_headcount_employee_dump where File_Date = '20240201'

-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date not in (select File_Date from kgsonedatadb.raw_hist_headcount_employee_dump group by File_Date) group by File_Date

-- delete from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date in ('20230517','20230616','20230919','20231018','20231213')

-- COMMAND ----------

select A.File_Date,a.trusted_count,b.File_Date,b.raw_count from (select File_Date,count(1) as trusted_count from kgsonedatadb.trusted_hist_headcount_employee_dump group by File_Date) A left join (select File_Date,count(1) as raw_count from kgsonedatadb.raw_hist_headcount_employee_dump group by File_Date) B
on A.File_Date = B.File_Date

-- COMMAND ----------

-- select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Location = 'Kolkata' group by file_date

select location,count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where left(File_Date,6) = '202210' and employee_number in (35761,37159,37779,44431,83302,83976,87802,88389,95070,97854,101752,105806,108439,108530,110111,115849,115980,116517,117587,120359,123994,124141,124355,124360,124363,124364,124372,124375,124376,124379,124382,124393,124398,124400,124403,124411,124415,124422,124426,124427,124429,124433,124437,124466,124468,124470,124475,124483,124489,124490,124505,124517,124522,124529,124530,124545,124552,124989,125009,125025,125047,125132,125137,125153,125189,125327,125332,125361,125372,125387,125448,125485,125509,125517,125521,125547,125556,125560,125565,125567,125568,125724,125737,125739,125746,125804,125845,125878,125898,126102,126108,126124,126156,126161,126259,126267,126285,126301,126325,126330,126366,126433,126452,126551,126554,126705,126738,126765,126771,126774,126849,126863,127055,127071,127079,127100,127106,127115,127123,127185,127193,127209,127217,127333,127344,127506,127512,127514,127522,127555,127556,127561,127572,127601,127612,127639,127641,127643,127660,127678,127776,127854,127859,127870,127875,127885,127894,127895,127900,127916,127938,127939,127997,128106,128163,128189,128209,128237) group by location

-- update kgsonedatadb.trusted_hist_headcount_monthly_employee_details set Location = 'Kolkata' where left(File_Date,6) = '202210' and employee_number in (35761,37159,37779,44431,83302,83976,87802,88389,95070,97854,101752,105806,108439,108530,110111,115849,115980,116517,117587,120359,123994,124141,124355,124360,124363,124364,124372,124375,124376,124379,124382,124393,124398,124400,124403,124411,124415,124422,124426,124427,124429,124433,124437,124466,124468,124470,124475,124483,124489,124490,124505,124517,124522,124529,124530,124545,124552,124989,125009,125025,125047,125132,125137,125153,125189,125327,125332,125361,125372,125387,125448,125485,125509,125517,125521,125547,125556,125560,125565,125567,125568,125724,125737,125739,125746,125804,125845,125878,125898,126102,126108,126124,126156,126161,126259,126267,126285,126301,126325,126330,126366,126433,126452,126551,126554,126705,126738,126765,126771,126774,126849,126863,127055,127071,127079,127100,127106,127115,127123,127185,127193,127209,127217,127333,127344,127506,127512,127514,127522,127555,127556,127561,127572,127601,127612,127639,127641,127643,127660,127678,127776,127854,127859,127870,127875,127885,127894,127895,127900,127916,127938,127939,127997,128106,128163,128189,128209,128237) and Location in ('Bangalore','Kochi')
--9 records affected

select location,count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where left(File_Date,6) = '202211' and employee_number in (128349,128350,128444,128452,128474,128499,128526,128547,128582,128716,128721,128736,128738,128754,128758,128759,128767,128795,128798,128806,128921,129036,129064,129067,129111,129114,129115,129117,129121,129122,129124,129131,129133,129135,129138,129140,129158,129166,129173,129177,129214,129269,129534,129537,129543,129545,129558,129575,129593,129594,129595,129601,129610,129631,129642,129648,129657,129743) group by location

-- update kgsonedatadb.trusted_hist_headcount_monthly_employee_details set Location = 'Kolkata' where left(File_Date,6) = '202211' and employee_number in (128349,128350,128444,128452,128474,128499,128526,128547,128582,128716,128721,128736,128738,128754,128758,128759,128767,128795,128798,128806,128921,129036,129064,129067,129111,129114,129115,129117,129121,129122,129124,129131,129133,129135,129138,129140,129158,129166,129173,129177,129214,129269,129534,129537,129543,129545,129558,129575,129593,129594,129595,129601,129610,129631,129642,129648,129657,129743)
-- 58 rows affected

select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where left(File_Date,6) = '202211' and location = 'Kolkata' and employee_number not in (128349,128350,128444,128452,128474,128499,128526,128547,128582,128716,128721,128736,128738,128754,128758,128759,128767,128795,128798,128806,128921,129036,129064,129067,129111,129114,129115,129117,129121,129122,129124,129131,129133,129135,129138,129140,129158,129166,129173,129177,129214,129269,129534,129537,129543,129545,129558,129575,129593,129594,129595,129601,129610,129631,129642,129648,129657,129743)

-- update kgsonedatadb.trusted_hist_headcount_monthly_employee_details set Location = 'Bangalore', Sub_Location='Bangalore One' where left(File_Date,6) = '202211' and employee_number = 108439
--1 row affected

-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_headcount_monthly_employee_details set location = 'Kolkata' where left(File_Date,6) = '202212' and employee_number in ('129816','129827','129840','129984','129985','129987','129988','129995','130012','130013','130054','130068','130083','130265','130268');

-- COMMAND ----------

select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump group by file_date

-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.config_hist_cc_bu_sl group by File_Date

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import datetime
-- MAGIC from pyspark.sql.functions import concat
-- MAGIC
-- MAGIC month_list = [11,12]
-- MAGIC current_date = datetime.datetime.now()
-- MAGIC current_month = current_date.month
-- MAGIC current_year = current_date.year
-- MAGIC
-- MAGIC print(current_month,current_year)
-- MAGIC
-- MAGIC if current_month in month_list:
-- MAGIC     month_key = str(current_year)+"10"
-- MAGIC     print(month_key)
-- MAGIC else:
-- MAGIC     month_key = str(current_year-1)+"10"
-- MAGIC     print(month_key)