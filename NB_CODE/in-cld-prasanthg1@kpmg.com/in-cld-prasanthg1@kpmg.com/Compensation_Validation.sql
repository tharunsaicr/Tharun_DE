-- Databricks notebook source
select emp_id, employee_name, sub_function, bu, doj from kgsonedatadb.trusted_stg_employee_engagement_year_end where (Last_FY_Rating is null or Last_FY_Rating = '') and doj < '2023-07-01' and sub_function <> 'HR';

select File_Date,* from kgsonedatadb.trusted_hist_compensation_non_sensitive_qualification_dump where emp_id = '93056' order by File_Date desc;

select * from kgsonedatadb.raw_hist_compensation_employee_ratings where EMPLOYEE_NUMBER = '40045'

-- COMMAND ----------

-- Observation 2 -- 2 records designation and job code matches after latest C&B file load
select Designation,Job_Code from kgsonedatadb.trusted_stg_employee_engagement_year_end where Emp_ID in ('143136','86652');

-- Observation 3 -- 455 records are Pitching details are blank as they are not part of YEC file
select * from kgsonedatadb.trusted_hist_compensation_yec where FILE_DATE = '20240407' and EMPLOYEE_NUMBER in (
select distinct Emp_ID,employee_name,DOJ,BU,Sub_Function,Cost_centre from kgsonedatadb.trusted_stg_employee_engagement_year_end where Pitching_Year is null);
-- or Pitched_at__Current_Positioning_ is null

--Observastion 4 --11 records - Pitching details - matching after latest YEC load
select Emp_ID,Pitched_at__Current_Positioning_,Pitching_Year from kgsonedatadb.trusted_stg_employee_engagement_year_end where 
Emp_ID in ('98879','59587','45970','51668','95415','97053','131342','22432','99241','43067','38364')
except
-- union all
select EMPLOYEE_NUMBER,PITCHED_AT__CURRENT_POSITIONING_,YEAR_OF_PITCHING from kgsonedatadb.trusted_hist_compensation_yec where 
-- FILE_DATE = '20240407' and 
EMPLOYEE_NUMBER in ('98879','59587','45970','51668','95415','97053','131342','22432','99241','43067','38364');

-- Observation 5 -- 455 records Date_Since_at_Current_Designation not matching as they are not part of YEC file
select * from kgsonedatadb.trusted_hist_compensation_yec where FILE_DATE = '20240407' and EMPLOYEE_NUMBER in (
select distinct Emp_ID,employee_name,DOJ,BU,Sub_Function,Cost_centre from kgsonedatadb.trusted_stg_employee_engagement_year_end where Date_since_at_current_designation is null or Years_at_current_designation_as_on_30_Sep_Current_FY is null);

-- Observation 6 -- 455 records Total work experience not matching as they are not part of YEC file
select * from kgsonedatadb.trusted_hist_compensation_yec where FILE_DATE = '20240407' and EMPLOYEE_NUMBER in (
select distinct Emp_ID,employee_name,DOJ,BU,Sub_Function,Cost_centre from kgsonedatadb.trusted_stg_employee_engagement_year_end where Total_years_of_work_experience_as_on_30_Sep_Current_FY is null);

-- Observation 7 -- 455 records Prior_Work_Experience not matching as they are not part of YEC file
select * from kgsonedatadb.trusted_hist_compensation_yec where FILE_DATE = '20240407' and EMPLOYEE_NUMBER in (
select distinct Emp_ID,employee_name,DOJ,BU,Sub_Function,Cost_centre from kgsonedatadb.trusted_stg_employee_engagement_year_end where Work_experience_prior_to_KPMG__Years_  is null);

--Observastion 8 -- All the 9 records, the work exp shows > 30 years.. Also the difference is because of YY.MM to years in decimal conversion.
select 'YearEnd' as TableName,Emp_ID,Work_experience_prior_to_KPMG__Years_ from kgsonedatadb.trusted_stg_employee_engagement_year_end where 
Emp_ID in ('85756','130080','134233','130055','137245','139633','139678','141942','139597')
-- except
union all
select 'YEC' as TableName,EMPLOYEE_NUMBER,WORK_EXPERIENCE_PRIOR_TO_KPMG__YY_MM_FORMAT_ from kgsonedatadb.trusted_hist_compensation_yec where 
-- FILE_DATE = '20240407' and 
EMPLOYEE_NUMBER in ('85756','130080','134233','130055','137245','139633','139678','141942','139597');

--Observastion 9 -- 11 colleagues Last_FY_Rating matching but Last_FY_Rating for 3623 ratings are blank because they are not part of the Employee Ratings data shared by the C&B team. Need to check with C&B team
select Emp_ID,Last_FY_Rating from kgsonedatadb.trusted_stg_employee_engagement_year_end where 
Emp_ID in ('97629','98952','105865','109897','109933','116470','116973','118435','119797','130557','56078')
except
-- union all
select Employee_Number, Rating from kgsonedatadb.trusted_hist_compensation_employee_ratings where 
FY = 'FY23' and 
EMPLOYEE_NUMBER in ('97629','98952','105865','109897','109933','116470','116973','118435','119797','130557','56078');

select emp_id, employee_name, sub_function, bu, doj from kgsonedatadb.trusted_stg_employee_engagement_year_end where Last_FY_Rating is null or Last_FY_Rating = '';

select Employee_Number, Rating, FY, file_date from kgsonedatadb.trusted_hist_compensation_employee_ratings where EMPLOYEE_NUMBER in (select distinct Emp_ID from kgsonedatadb.trusted_stg_employee_engagement_year_end where Last_FY_Rating is null or Last_FY_Rating = '');

--Observastion 10 -- 26 colleages, FY'22 ratings are null because we have not received any latest ratings data for FY22 from C&B team
select Emp_ID,previous_FY_rating, employee_name, sub_function, bu, doj from kgsonedatadb.trusted_stg_employee_engagement_year_end where Emp_ID in ('47864','54293','64387','107878','98879','46248','41405','45319','77121','27449','77333','38728','105708','19654','40045','73181','63785','17030','106352','87442','51668','43067','75027','72148','96318');

select Employee_Number, Rating, FY, file_date from kgsonedatadb.trusted_hist_compensation_employee_ratings where EMPLOYEE_NUMBER in ('47864','54293','64387','107878','98879','46248','41405','45319','77121','27449','77333','38728','105708','19654','40045','73181','63785','17030','106352','87442','51668','43067','75027','72148','96318') and FY='FY22';

select * from kgsonedatadb.trusted_hist_compensation_employee_ratings where EMPLOYEE_NUMBER = '40045'

--Observastion 11 -- 16 colleagues Fy'21 ratings are null because we have not received any latest Ratings data for FY21 from C&B team
select Emp_ID,previous_to_previous_FY_rating, employee_name, sub_function, bu, doj from kgsonedatadb.trusted_stg_employee_engagement_year_end where Emp_ID in ('47864','54293','64387','46248','41405','45319','38728','19654','40045','63785','17030','87442','51668','43067','75027','72148');

select Employee_Number, Rating, FY, file_date from kgsonedatadb.trusted_hist_compensation_employee_ratings where EMPLOYEE_NUMBER in ('47864','54293','64387','46248','41405','45319','38728','19654','40045','63785','17030','87442','51668','43067','75027','72148');

-- Observation 12 -- Data is now available after latest file load
select Emp_ID,Distribution_category from kgsonedatadb.trusted_stg_employee_engagement_year_end where Emp_ID in ('86652','143136')

-- Observation 13 -- 17 colleages PG data is matching against the Qualification file shared with us by C&B team. Need to confirm what is the PG info available with Bhumija 
select Emp_ID,Post_Graduation_Highest_Qualification, employee_name, sub_function, bu, doj from kgsonedatadb.trusted_stg_employee_engagement_year_end where Emp_ID in 
('122887','122990','79049','128218','129482','131754','136172','139645','139621','139625','141603','142205','143324','143786','143997','144017','144418')
except
select EMP_ID, Post_Graduation___Highest_Qualification,File_Date from kgsonedatadb.trusted_hist_compensation_non_sensitive_qualification_dump where Emp_ID in 
('122887','122990','79049','128218','129482','131754','136172','139645','139621','139625','141603','142205','143324','143786','143997','144017','144418') order by EMP_ID, File_Date desc;

select EMP_ID, Post_Graduation___Highest_Qualification from kgsonedatadb.trusted_hist_compensation_non_sensitive_qualification_dump where file_date = '20240326' and Emp_ID in 
('122887','122990','79049','128218','129482','131754','136172','139645','139621','139625','141603','142205','143324','143786','143997','144017','144418')
-- union all
except
select Emp_ID,Post_Graduation_Highest_Qualification from kgsonedatadb.trusted_stg_employee_engagement_year_end where Emp_ID in 
('122887','122990','79049','128218','129482','131754','136172','139645','139621','139625','141603','142205','143324','143786','143997','144017','144418');


-- Observation 14 -- 16 colleages UG data is matching against the Qualification file shared with us by C&B team. Need to confirm what is the UG info available with Bhumija 
select Emp_ID,GRADUATION, employee_name, sub_function, bu, doj from kgsonedatadb.trusted_stg_employee_engagement_year_end where Emp_ID in 
('93056','38411','79049','128218','139621','143133','143252','143483','143786','143997','143991','144017','144416','144237','144386','144392')
except
select EMP_ID, GRAD from kgsonedatadb.trusted_hist_compensation_non_sensitive_qualification_dump where Emp_ID in 
('93056','38411','79049','128218','139621','143133','143252','143483','143786','143997','143991','144017','144416','144237','144386','144392');

select EMP_ID, GRAD from kgsonedatadb.trusted_hist_compensation_non_sensitive_qualification_dump where file_date = '20240326' and Emp_ID in 
('93056','38411','79049','128218','139621','143133','143252','143483','143786','143997','143991','144017','144416','144237','144386','144392')
-- union all
except
select Emp_ID,GRADUATION from kgsonedatadb.trusted_stg_employee_engagement_year_end where Emp_ID in 
('93056','38411','79049','128218','139621','143133','143252','143483','143786','143997','143991','144017','144416','144237','144386','144392');

-- Observation 15 -- Some of the records where leave days > 200 records is because this includes sabbatical which are leaves are that are yet to be taken ie., After March'24. Also, adding the leave break up in the column Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_? requires code change. This can be taken up as enhancement/change.

select no_of_days_taken_in_current_year,gender,* from kgsonedatadb.trusted_stg_employee_engagement_year_end where no_of_days_taken_in_current_year > 200;

select emp_id, employee_name, sub_function, bu, doj from kgsonedatadb.trusted_hist_headcount_leave_report where employee_number = '86919' and file_date = '20231231';

-- COMMAND ----------

select File_Date,Dated_On,count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end group by File_Date,Dated_On

select File_Date,Dated_On,count(1) from kgsonedatadb.trusted_hist_compensation_yec group by File_Date,Dated_On

select File_Date,Dated_On,count(1) from kgsonedatadb.raw_hist_compensation_yec group by File_Date,Dated_On

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_compensation_yec
select distinct file_date from kgsonedatadb.trusted_stg_compensation_yec
select distinct file_date from kgsonedatadb.trusted_compensation_yec

select EMP_ID, coalesce(qual_dump.Post_Graduation___Highest_Qualification,qual_dump.GRAD) as GRAD from (
select rank() over(partition by EMP_ID order by File_Date desc, Dated_On desc) as Rank_Grad,* from kgsonedatadb.trusted_hist_compensation_non_sensitive_qualification_dump where EMP_ID is not null
) qual_dump where qual_dump.Rank_Grad = 1

-- COMMAND ----------

-- delete from kgsonedatadb.trusted_hist_compensation_finance_metrics where File_Date = '20230705'; --3440
-- delete from kgsonedatadb.trusted_hist_compensation_finance_metrics_joiners where File_Date = '20230705'; --209
-- delete from kgsonedatadb.trusted_hist_compensation_finance_metrics_leavers where File_Date = '20230705'; --257

-- delete from kgsonedatadb.raw_hist_compensation_finance_metrics where File_Date = '20230705'; --6880
-- delete from kgsonedatadb.raw_hist_compensation_finance_metrics_joiners where File_Date = '20230705'; --418
-- delete from kgsonedatadb.raw_hist_compensation_finance_metrics_leavers where File_Date = '20230705'; --514

-- drop table kgsonedatadb.trusted_hist_compensation_finance_metrics_joiners;
-- drop table kgsonedatadb.trusted_hist_compensation_finance_metrics_leavers;
-- drop table kgsonedatadb.raw_hist_compensation_finance_metrics_joiners;
-- drop table kgsonedatadb.raw_hist_compensation_finance_metrics_leavers;

-- alter table kgsonedatadb.trusted_hist_compensation_finance_metrics add column AGGREGATED_MONTHLY_GROSS_AMOUNT double;

-- ALTER TABLE kgsonedatadb.trusted_hist_compensation_finance_metrics SET TBLPROPERTIES (
--     'delta.minReaderVersion' = '2',
--     'delta.minWriterVersion' = '5',
--     'delta.columnMapping.mode' = 'name'
--   )
-- alter table kgsonedatadb.trusted_hist_compensation_finance_metrics rename column Annual_Aggregated_CTC to Aggregated_Annual_CTC;

-- ALTER TABLE kgsonedatadb.trusted_hist_compensation_finance_metrics DROP COLUMNS (JUL_23__AGGREGATED_HC_, JUL_23__AGGREGATED_MONTHLY_GROSS_AMOUNT_, JUL_23__AGGREGATED_ANNUAL_CTC_)

-- ALTER TABLE kgsonedatadb.trusted_hist_compensation_finance_metrics_joiners SET TBLPROPERTIES (
--     'delta.minReaderVersion' = '2',
--     'delta.minWriterVersion' = '5',
--     'delta.columnMapping.mode' = 'name'
--   )

-- ALTER TABLE kgsonedatadb.trusted_hist_compensation_finance_metrics_joiners DROP COLUMNS (JUL_23__AGGREGATED_HC_, JUL_23__AGGREGATED_MONTHLY_GROSS_AMOUNT_, JUL_23__AGGREGATED_ANNUAL_CTC_)

-- ALTER TABLE kgsonedatadb.trusted_hist_compensation_finance_metrics_leavers SET TBLPROPERTIES (
--     'delta.minReaderVersion' = '2',
--     'delta.minWriterVersion' = '5',
--     'delta.columnMapping.mode' = 'name'
--   )

-- ALTER TABLE kgsonedatadb.trusted_hist_compensation_finance_metrics_leavers DROP COLUMNS (JUN_23__AGGREGATED_HC_, JUN_23__AGGREGATED_MONTHLY_GROSS_AMOUNT_, JUN_23__AGGREGATED_ANNUAL_CTC_)

-- update kgsonedatadb.trusted_hist_compensation_finance_metrics set Aggregated_HC = JUL_23__AGGREGATED_HC_,Annual_Aggregated_CTC = JUL_23__AGGREGATED_ANNUAL_CTC_,AGGREGATED_MONTHLY_GROSS_AMOUNT = JUL_23__AGGREGATED_MONTHLY_GROSS_AMOUNT_ where File_Date = '20230705';

-- describe table kgsonedatadb.trusted_hist_compensation_finance_metrics;

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_compensation_finance_metrics where File_Date = '20230705'; --3440
select * from kgsonedatadb.trusted_hist_compensation_finance_metrics_joiners where File_Date = '20230705'; --209
select * from kgsonedatadb.trusted_hist_compensation_finance_metrics_leavers where File_Date = '20230705'; --257

select * from kgsonedatadb.config_data_type_cast where Type_Cast_To = 'bigint'

-- update kgsonedatadb.config_data_type_cast set Column_Name = 'Aggregated_Annual_CTC' where Process_Name = 'compensation' and Delta_Table_Name like 'finance_metrics%' and Column_Name = 'AGGREGATED_ANNUAL_CTC';