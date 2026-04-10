-- Databricks notebook source
select distinct Emp_ID from kgsonedatadb.trusted_stg_employee_engagement_year_end where Pitched_at__Current_Positioning_ is null or Pitched_at__Current_Positioning_ = ''

select * from kgsonedatadb.trusted_hist_compensation_yec where employee_number in (select distinct Emp_ID from kgsonedatadb.trusted_stg_employee_engagement_year_end where Pitched_at__Current_Positioning_ is null or Pitched_at__Current_Positioning_ = '')

-- COMMAND ----------

select Emp_ID,Employee_Name,`Function`,Sub_Function,Cost_centre,BU,Client_Geo,Operating_unit,Location,Designation,Job_Code,Date_as_of,Pitched_at__Current_Positioning_,Pitching_Year,Date_since_at_current_designation,round(cast(Years_at_current_designation_as_on_30_Sep_Current_FY as float),1) as Years_at_current_designation_as_on_30_Sep_Current_FY,DOJ,round(cast(Years_in_the_Firm_as_on_30_Sep_Current_FY as float),1) as Years_in_the_Firm_as_on_30_Sep_Current_FY,round(cast(Work_experience_prior_to_KPMG__Years_ as float),1) as Work_experience_prior_to_KPMG__Years_,round(cast(Total_years_of_work_experience_as_on_30_Sep_Current_FY as float),1) as Total_years_of_work_experience_as_on_30_Sep_Current_FY,Last_FY_Rating,Previous_FY_Rating,Previous_to_Previous_FY_Rating,Post_Graduation_Highest_Qualification,Graduation,Gender,Email,Emp_Category,Performance_manager_name,Encore_winner,`Include_in_rating_distribution?`,replace(no_of_days_taken_in_current_year,'null','') as no_of_days_taken_in_current_year,Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_,Distribution_category,Team_Band_as_per_CP_document,Proposed_Rating_Current_FY,Rating_Jump_or_Dip,Promotion_recommendation_Yes_or_No_,Promotion_or_Progession,BP_comments,Comments_on_Promotion_Exceptions,PD_comments,HR_head_comments,Promotion_category,Promotion_approved,New_Designation,New_job_code,New_Pitched_at__proposed_positioning,New_Pitching_Year,Remarks,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.trusted_employee_engagement_year_end

select distinct file_date,dated_on from kgsonedatadb.trusted_hist_compensation_yec

-- COMMAND ----------

select EMPLOYEE_NUMBER,PITCHED_AT__CURRENT_POSITIONING_ from kgsonedatadb.trusted_hist_compensation_yec where EMPLOYEE_NUMBER in ('98879','59587','45970','51668','95415','97053','131342','22432','99241','43067','38364') and Dated_On = '2024-02-27T06:20:54.000+00:00'

-- COMMAND ----------

select * from kgsonedatadb.trusted_employee_engagement_year_end where Emp_ID = '143136';
select File_Date,* from kgsonedatadb.trusted_headcount_employee_dump where employee_number = '86652' order by File_Date desc;

select distinct PITCHED_AT__CURRENT_POSITIONING_ from kgsonedatadb.trusted_hist_compensation_yec

select * from kgsonedatadb.trusted_hist_compensation_yec where EMPLOYEE_NUMBER in ('98879','59587','45970','51668','95415','97053','131342','22432','99241','43067','38364');

select Emp_ID,Pitched_at__Current_Positioning_ from kgsonedatadb.trusted_hist_employee_engagement_year_end where Emp_ID in ('98879','59587','45970','51668','95415','97053','131342','22432','99241','43067','38364') and dated_on = '2024-03-28T09:26:13.000+00:00'

select * from kgsonedatadb.trusted_hist_compensation_employee_ratings where EMPLOYEE_NUMBER = '97629';

-- COMMAND ----------

select distinct FY,file_date,dated_on from kgsonedatadb.trusted_hist_compensation_employee_ratings

select * from kgsonedatadb.config_job_code_distribution_category_mapping where processname = 'Employee_Engagement' and tablename = 'Year_End'

-- COMMAND ----------

select employee_number as YEC_EmpNo, YEAR_OF_PITCHING as YEC_Pitching_Year, B.Emp_ID as ye_empno, B.Pitching_Year as ye_pitchingyear from kgsonedatadb.trusted_compensation_yec A
left outer join kgsonedatadb.trusted_employee_engagement_year_end B on A.EMPLOYEE_NUMBER = B.Emp_ID
where A.YEAR_OF_PITCHING != B.Pitching_Year


select * from kgsonedatadb.trusted_compensation_yec
select * from kgsonedatadb.trusted_employee_engagement_year_end

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_employee_engagement_year_end where dated_on = '2024-03-08T14:55:08.000+0000'

-- COMMAND ----------

-- select Emp_ID,Employee_Name,`Function`,Sub_Function,Cost_centre,BU,Client_Geo,Operating_unit,Location,Designation,Job_Code,Date_as_of,Pitched_at__Current_Positioning_,Pitching_Year,Date_since_at_current_designation,Years_at_current_designation_as_on_30_Sep_Current_FY,DOJ,Years_in_the_Firm_as_on_30_Sep_Current_FY,Work_experience_prior_to_KPMG__Years_,Total_years_of_work_experience_as_on_30_Sep_Current_FY,Current_FY_Rating,Last_FY_Rating,Last_to_Last_FY_Rating,Post_Graduation_Highest_Qualification,Graduation,Gender,Email,Emp_Category,Performance_manager_name,Encore_winner,`Include_in_rating_distribution?`,no_of_days_taken_in_current_year,Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_,Distribution_category,Team_Band_as_per_CP_document,Proposed_Rating_Current_FY,Rating_Jump_or_Dip,Promotion_recommendation_Yes_or_No_,Promotion_or_Progession,BP_comments,Comments_on_Promotion_Exceptions,PD_comments,HR_head_comments,Promotion_category,Promotion_approved,New_Designation,New_job_code,New_Pitched_at__proposed_positioning,New_Pitching_Year,Remarks,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.trusted_employee_engagement_year_end limit 1

select * from kgsonedatadb.config_data_type_cast where process_name = 'employee_engagement'

select Emp_ID,Employee_Name,`Function`,Sub_Function,Cost_centre,BU,Client_Geo,Operating_unit,Location,Designation,Job_Code,Date_as_of,Pitched_at__Current_Positioning_,Pitching_Year,Date_since_at_current_designation,Years_at_current_designation_as_on_30_Sep_Current_FY,DOJ,Years_in_the_Firm_as_on_30_Sep_Current_FY,Work_experience_prior_to_KPMG__Years_,Total_years_of_work_experience_as_on_30_Sep_Current_FY,Current_FY_Rating,Last_FY_Rating,Last_to_Last_FY_Rating,Post_Graduation_Highest_Qualification,Graduation,Gender,Email,Emp_Category,Performance_manager_name,Encore_winner,`Include_in_rating_distribution?`,cast(replace(no_of_days_taken_in_current_year,'null','') as int) as no_of_days_taken_in_current_year,Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_,Distribution_category,Team_Band_as_per_CP_document,Proposed_Rating_Current_FY,Rating_Jump_or_Dip,Promotion_recommendation_Yes_or_No_,Promotion_or_Progession,BP_comments,Comments_on_Promotion_Exceptions,PD_comments,HR_head_comments,Promotion_category,Promotion_approved,New_Designation,New_job_code,New_Pitched_at__proposed_positioning,New_Pitching_Year,Remarks,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.trusted_employee_engagement_year_end where Emp_ID in ('97993','125225')


select Emp_ID,Employee_Name,`Function`,Sub_Function,Cost_centre,BU,Client_Geo,Operating_unit,Location,Designation,Job_Code,Date_as_of,Pitched_at__Current_Positioning_,Pitching_Year,Date_since_at_current_designation,Years_at_current_designation_as_on_30_Sep_Current_FY,DOJ,Years_in_the_Firm_as_on_30_Sep_Current_FY,Work_experience_prior_to_KPMG__Years_,Total_years_of_work_experience_as_on_30_Sep_Current_FY,Current_FY_Rating,Last_FY_Rating,Last_to_Last_FY_Rating,Post_Graduation_Highest_Qualification,Graduation,Gender,Email,Emp_Category,Performance_manager_name,Encore_winner,`Include_in_rating_distribution?`,cast(replace(no_of_days_taken_in_current_year,'null','') as int) as no_of_days_taken_in_current_year,Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_,Distribution_category,Team_Band_as_per_CP_document,Proposed_Rating_Current_FY,Rating_Jump_or_Dip,Promotion_recommendation_Yes_or_No_,Promotion_or_Progession,BP_comments,Comments_on_Promotion_Exceptions,PD_comments,HR_head_comments,Promotion_category,Promotion_approved,New_Designation,New_job_code,New_Pitched_at__proposed_positioning,New_Pitching_Year,Remarks,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.raw_curr_employee_engagement_year_end where Emp_ID in ('97993','125225')

-- COMMAND ----------

-- Partner Data in Ratings
select count(1) from kgsonedatadb.trusted_hist_compensation_employee_ratings where EMPLOYEE_NUMBER not in 
(
select distinct employee_number from kgsonedatadb.trusted_hist_headcount_employee_dump where Position like '%Partner%'
)

-- Exited colleagues prior to FY'23
select distinct employee_number from kgsonedatadb.trusted_hist_headcount_termination_dump where termination_date < '2023-10-01'

select count(1) from kgsonedatadb.trusted_hist_compensation_employee_ratings where employee_number not in (select distinct employee_number from kgsonedatadb.trusted_hist_headcount_termination_dump where termination_date < '2023-10-01')

-- Not in ED/TD but in Rating file

select * from kgsonedatadb.trusted_hist_compensation_employee_ratings where employee_number = '139293'

select * from kgsonedatadb.trusted_hist_headcount_employee_dump where employee_number = '139293'

update kgsonedatadb.trusted_hist_compensation_employee_ratings set RATING = 'NA' where employee_number in ('23921','15411','143936','41384','134345','26853','63446','74685','51912','35138','53113','21270','52652','31546','86410','91263','143102','30840','47116','37320','33564','22679','48882','85177','42249','36608','40118','77863','64839','24355','21048','16507','27875')

update kgsonedatadb.raw_hist_compensation_employee_ratings set RATING = 'NA' where employee_number in ('23921','15411','143936','41384','134345','26853','63446','74685','51912','35138','53113','21270','52652','31546','86410','91263','143102','30840','47116','37320','33564','22679','48882','85177','42249','36608','40118','77863','64839','24355','21048','16507','27875')


-- COMMAND ----------

create table kgsonedatadb.trusted_hist_employee_engagement_year_end_bckp_20240307 
select count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end

select count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end_bckp_20240307

-- COMMAND ----------

select FILE_DATE,count(1) from kgsonedatadb_badrecords.trusted_hist_compensation_employee_ratings_bad group by FILE_DATE

-- select * from kgsonedatadb.trusted_hist_compensation_yec where EMPLOYEE_NUMBER = '97993'

select Employee_Number,Name,Leave_Start_Date,Leave_End_Date from (select *, row_number() over (PARTITION BY Employee_Number,Leave_Start_Date,Leave_End_Date,Leave_Type order by date_of_approved desc,dated_on desc) as rn from kgsonedatadb.trusted_hist_headcount_leave_report where Leave_Start_Date>= '2023-10-01' and Leave_End_Date<= '2024-09-30' and employee_number = '97993' and lower(Leave_Type) in ('kgs emergency medical leave','kgs maternity leave','kgs maternity miscarriage leave','kgs leave without pay','kgs emergency medical leave extension','kgs sabbatical leave','kgs extended sick leave','kgs primary care giver (family) leave','kgs primary caregiver leave new parent','kgs eml bank','kgs maternity adoption leave','kgs social sabbatical leave','kgs part timer emergency medical leave')) where rn =1  and (Cancel_Status is null or lower(Cancel_Status) = 'no' ) and lower(Approval_Status) = 'approved'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df_ratings = spark.sql("select emp_rating.Employee_Number as Rating_Employee_Number, emp_rating.FY as Rating_FY, emp_rating.Rating as Emp_Rating from (select rank() over(partition by Employee_Number,FY order by File_Date desc, Dated_On desc) as Rank_Rating,* from kgsonedatadb.trusted_hist_compensation_employee_ratings where Employee_Number is not null) emp_rating where emp_rating.Rank_Rating = 1")
-- MAGIC
-- MAGIC display(df_ratings.where(df_ratings["Rating_Employee_Number"]=='103120'))
-- MAGIC
-- MAGIC df_ratings_pivot = df_ratings.groupBy("Rating_Employee_Number").pivot("Rating_FY").agg(first("Emp_Rating"))
-- MAGIC
-- MAGIC display(df_ratings_pivot.where(df_ratings["Rating_Employee_Number"]=='103120'))

-- COMMAND ----------

select File_Date,Dated_On,count(1) from kgsonedatadb.trusted_hist_employee_engagement_encore_output group by File_Date,Dated_On;

select left(replace(Nomination_Date,"-",""),6),Dated_On,count(1) from kgsonedatadb.trusted_hist_employee_engagement_encore_output where file_date = '20230930' group by left(replace(Nomination_Date,"-",""),6),Dated_On

-- COMMAND ----------

select * from kgsonedatadb.trusted_headcount_employee_details where cost_centre = 'CF-IT One Data COE'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import concat
-- MAGIC month_list = [10,11,12]
-- MAGIC
-- MAGIC # file_date = "20221031"
-- MAGIC # current_month = file_date[4:6]
-- MAGIC # current_year = file_date[0:4]
-- MAGIC # print(current_month)
-- MAGIC # print(current_year)
-- MAGIC
-- MAGIC sampleDF=spark.sql('select Reference_Id,Reward_Status,Reward_Name,Nomination_Date,Reward_Amount_in_Accounting_Currency__INR_,Budget_From,Nominee_s_Name,Nominee_s_Email,Nominee_s_Employee_ID,Nominee_s_Status,Nominee_s_Location,Nominee_s_City,Nominee_s_Unit,Nominee_s_Department,Nominee_s_Designation,Nominee_s_Division,Nominee_s_Function,Nominee_s_Region,Nominee_s_Legal_Entity,Nominee_s_Employee_Band,Team_Name,Team_Members,Nominator_s_Name,Nominator_s_Email,Nominator_s_Employee_ID,Nominator_s_Status,Nominator_s_Location,Nominator_s_City,Nominator_s_Unit,Nominator_s_Department,Nominator_s_Designation,Nominator_s_Division,Nominator_s_Function,Nominator_s_Region,Nominator_s_Legal_Entity,Nominator_s_Employee_Band,Actual_Nominator_s_Name,Actual_Nominator_s_Employee_Id,Actual_Nominator_s_Email,Pending_Approval_With__Name_,Pending_Approval_With__Emp_Id_,Pending_Approval_With__Email_,Reward_Date,First_Level_Approver_Name,First_Level_Approver_Employee_Id,First_Level_Approver_Email,Second_Level_Approver_Name,Second_Level_Approver_Employee_Id,Second_Level_Approver_Email,Reject_Date,Rejected_By__Name_,Rejected_By__Emp_Id_,Rejected_By__Email_,Deleted_Date,Deleted_By__Name_,Deleted_By__Emp_Id_,Deleted_By__Email_,Proxy_By__Name_,Proxy_By__Emp_Id_,Proxy_By__Email_,Reward_Redemption_Type,Reward_Label,Budget_Calendar,Financial_Year,Reward_Attributes,Citation,First_Level_Approval_Remarks,First_Level_Approval_Date,First_Level_Rejection_Date,Second_Level_Approval_Remarks,Second_Level_Approval_Date,Second_Level_Rejection_Date,Nomination_Form_Fields,Dated_On,File_Date,left(replace(Nomination_Date,"-",""),6) as Month_Key from kgsonedatadb.trusted_employee_engagement_thanks_dump')
-- MAGIC
-- MAGIC         # month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
-- MAGIC file_date = sampleDF.select('File_Date').distinct().rdd.map(lambda x:x[0]).first()
-- MAGIC
-- MAGIC current_month = int(file_date[4:6])
-- MAGIC current_year = int(file_date[0:4])
-- MAGIC print(current_month)
-- MAGIC print(current_year)
-- MAGIC
-- MAGIC if int(current_month) in month_list:
-- MAGIC     month_key = str(current_year)+"10"
-- MAGIC     print(month_key)
-- MAGIC else:
-- MAGIC     month_key = str(current_year-1)+"10"
-- MAGIC     print(month_key)

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number = '97993'
select * from kgsonedatadb.trusted_hist_headcount_employee_dump where cost_centre = 'CF-IT One Data COE' 
select * from kgsonedatadb.config_hist_cc_bu_sl where cost_centre = 'CF-IT One Data COE'

-- "Emp_No", - Emp_Dump
-- "Final_Name", - Emp_Dump
-- "Company_Name", - Emp_Dump
-- "Designation", - Emp_Dump
-- "BU", - CC_BU
-- "Email_ID", - Emp_Dump
-- "Level", - Emp_Dump
-- "Gender", - Emp_Dump
-- "Gratuity_Date", - Emp_Dump

select * from kgsonedatadb.trusted_hist_headcount_employee_dump where cost_centre = 'CF-IT One Data COE' 

-- COMMAND ----------

select distinct file_date from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump
-- "Emp_No", - Emp_Dump
-- "Final_Name", - Emp_Dump
-- "Company_Name", - Emp_Dump
-- "Designation", - Emp_Dump
-- "BU", - CC_BU
-- "Category", - Thanks_Dump
-- "Reward_Date", - Thanks_Dump
-- "Nomination_Date", - Thanks_Dump
-- "Reward_Status", - Thanks_Dump
-- "Nominee_s_Unit", - Thanks_Dump
-- "Quarter", - Thanks_Dump
-- "Year", - Thanks_Dump
-- "Email_ID", - Emp_Dump
-- "Level", - Emp_Dump
-- "Gender", - Emp_Dump
-- "Gratuity_Date", - Emp_Dump
-- "Budget_From", - Thanks_Dump
-- "Nominator_s_Employee_ID" - Thanks_Dump

-- COMMAND ----------

-- select distinct Company from kgsonedatadb.trusted_hist_headcount_contingent
select Candidate_Id as Employee_Number,Full_Name,Company as Company_Name,Position,Cost_centre,Official_Email_ID as Email_Address,Job as Job_Name,Gender,'NA' as Gratuity_Date from kgsonedatadb.trusted_hist_headcount_contingent
-- "Emp_No", - contingent - "Candidate_ID"
-- "Final_Name", - contingent - "Full_Name"
-- "Company_Name", - contingent - "Company"
-- "Designation", - contingent - "Position"
-- "Email_ID", - contingent - "Official_Email_ID"
-- "Level", - contingent - "NA"
-- "Gender", - contingent - "Gender"
-- "Gratuity_Date", - contingent - "NA"

-- COMMAND ----------

select * from kgsonedatadb.config_dim_level_wise