-- Databricks notebook source
-- select distinct File_Date from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report

-- SELECT dated_on,COUNT(*) FROM kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report where DateFirstHired >= '+44837-01-01 00:00:00' and DateFirstHired <= '2024-07-29 00:00:00' group by dated_on

SELECT dated_on,File_Date,COUNT(*) FROM kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report group by Dated_On,File_Date

-- COMMAND ----------



-- COMMAND ----------

-- select * from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report
-- select * from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report where File_Date = '20231215'

select count(1) from (select row_number() over(partition by CANDIDATE_EMAIL  order by file_date desc ,dated_on desc) as row_num, * from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report  where to_date(File_Date,'yyyyMMdd') <= '2023-12-18') hist where row_num = 1;

select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Employee_Number = '133188' order by File_Date desc

-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.raw_stg_ta_post_hire_fte group by File_Date;

-- truncate table kgsonedatadb.trusted_hist_ta_post_hire_fte;
-- truncate table kgsonedatadb.trusted_ta_post_hire_fte;
-- truncate table kgsonedatadb.trusted_stg_ta_post_hire_fte;
-- truncate table kgsonedatadb.raw_hist_ta_post_hire_fte;
-- truncate table kgsonedatadb.raw_curr_ta_post_hire_fte;
-- truncate table kgsonedatadb.raw_stg_ta_post_hire_fte;

-- COMMAND ----------

-- MAGIC %run
-- MAGIC /kgsonedata/common_utilities/common_components

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sampleDF = spark.sql('select SNO, EmplNo, FullName, CandidateID, TaleoID, Costcentre, BU, ClientGeography, Location, SubLocation, Position, HCMapping, HClevel, JobName, PeopleGroupName, EmployeeCategory, DateFirstHired, Week, MonthAsPerWeek, FYAsPerWeek, Gender, Candidate_Email, Recruiter, Campus_Lateral, Source_Corrected, NameofSubSource_Corrected, Status, Joined, Drops, ConfirmedStart, ClientGeographyValueActual, PreviousEmployer, PreviousEmployerType, TotalExp, TypeofHire, DropReasons, DropReasons_Cleaned, Performance_Manager_Code, Buddy_Employee_Code, HRBP_Code, People_Champion_Code, Performance_Manager_Name, Buddy_Employee_Name, HRBP_Name, People_Champion_Name, Performance_Manager_Email, Buddy_Employee_Email, HRBP_Email, People_Champion_Email, Candidate_Contact_Number, FYFlagHCCutOff, FYFlagHCCutOffMonth, FYFlagHCCutOffQuarter, Hiring_Manager_Name, HighestEductaionQualification_Cleaned, Education_Bucket, Relocation_Bonus_Flag, Employee_s_Official_Email_ID, Dated_On, File_Date from kgsonedatadb.trusted_talent_acquisition_kgs_joiners_report')
-- MAGIC         
-- MAGIC sampleDF = sampleDF.withColumn("FYFlagHCCutOffMonth_new",from_unixtime(unix_timestamp(col("FYFlagHCCutOffMonth"),'MMM'),'MM'))
-- MAGIC
-- MAGIC sampleDF = sampleDF.withColumn("Month_Key",concat(substring('DateFirstHired',0,2),substring(regexp_replace("FYFlagHCCutOff","FY",""),5,6),'FYFlagHCCutOffMonth_new'))
-- MAGIC
-- MAGIC sampleDF = sampleDF.withColumn("Month_Key",when(sampleDF.FYFlagHCCutOffMonth_new.isin(['10','11','12']),concat(substring('DateFirstHired',0,2),substring("FYFlagHCCutOff",4,2),'FYFlagHCCutOffMonth_new')).otherwise(concat(substring('DateFirstHired',0,2),substring("FYFlagHCCutOff",7,2),'FYFlagHCCutOffMonth_new')))
-- MAGIC
-- MAGIC display(sampleDF)

-- COMMAND ----------

select distinct FYFlagHCCutOff,FYFlagHCCutOffMonth,FYFlagHCCutOffQuarter,Status from kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report where File_Date = '20231221' and status = 'Confirmed Start'

select distinct FYFlagHCCutOff,Status from kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report where File_Date = '20231221'

-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.raw_curr_ta_post_hire_cwk group by File_Date;

-- truncate table kgsonedatadb.trusted_hist_ta_post_hire_cwk;
-- truncate table kgsonedatadb.trusted_ta_post_hire_cwk;
-- truncate table kgsonedatadb.trusted_stg_ta_post_hire_cwk;
-- truncate table kgsonedatadb.raw_hist_ta_post_hire_cwk;
-- truncate table kgsonedatadb.raw_curr_ta_post_hire_cwk;
-- truncate table kgsonedatadb.raw_stg_ta_post_hire_cwk;

-- COMMAND ----------

-- select distinct Costcentre from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report where File_Date = '20231215' and Costcentre not in (select distinct file_date from kgsonedatadb.config_hist_cc_bu_sl);

select distinct Costcentre from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report where File_Date = '20231215' and Costcentre not in (select distinct Cost_centre from kgsonedatadb.config_hist_cc_bu_sl where File_Date = '20231117');

-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.trusted_hist_ta_post_hire_cwk group by File_Date