# Databricks notebook source
# MAGIC %sql
# MAGIC TRUNCATE TABLE kgsfinancedb.trusted_hist_

# COMMAND ----------

# MAGIC %sql
# MAGIC select Employee_Number, Employee_Name, Function, Sub_Function, Sub_Function1, Profit_Centre, Client_Geography, Location, Sub_Location, Positions, Job, Date_First_Hired, Gender, Employee_Category, Entity_Name, Month, BU_Aligned, KGS_Levels, Service_Network, Revised_SL, Termination_Date, Leaving_Reason, Data, Year, Att_Type, Tenure, Vol, Invol, HC, MS_FTS_Emps_tag, Att_match, Tenure_Category, Options, Aligned_Geo, Attrition_Tenure, HC_Tenure, Geo1, Geo2, Level, Quarter, Qualified_Non_Qualified, Highest_Qualification, Education_Group, Prior_Experience, Maternity, Sabbatical, Nationality, Rating, Supervisor_Name, Performance_Manager, IGH_Name, Age_Category, Dated_On, File_Date,DATE_FORMAT(MONTH,'yyyyMM') as Month_Key from kgsonedatadb.trusted_headcount_attrition_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from kgsonedatadb.trusted_headcount_attrition_data where YEAR is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kgsfinancedb.trusted_curr_bu_report_hyperion_dg_hcplan_forecast_pbi_report where DATA!=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_data_type_cast where process_name='gdc' and delta_table_name like '%otp%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_ready_for_production WHERE psid='3855810' order by file_date desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct file_date from kgsonedatadb.trusted_hist_gdc_bcp order by File_Date desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct file_date from kgsonedatadb.trusted_gdc_analytics

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view bcp_vw as (select * from (select *,row_number() over(partition by PSID,EMPLOYEE_NAME order by DOJ desc) as rn from kgsonedatadb.trusted_hist_gdc_bcp) a where rn=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
# MAGIC                USING  bcp_vw b
# MAGIC                ON a.PSID=b.PSID   
# MAGIC                WHEN MATCHED  THEN  
# MAGIC                 UPDATE SET a.BCP="BCP Access re-activated"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rmt_vw where PSID='4167002' --and EMPID='140106'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from   kgsonedatadb.trusted_hist_gdc_onboarding_master  --where RMT_HANDOVER_TO_INDUSTRY is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_contractors --where PS_ID='4180294'

# COMMAND ----------

# MAGIC %sql
# MAGIC Update kgsonedatadb.trusted_hist_gdc_onboarding_master 
# MAGIC             SET CANDIDATES_STATUS= CASE 
# MAGIC                                         WHEN (EMP_NAME is Null or EMP_NAME = "") then ""
# MAGIC                                         WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE='null' or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM='null' or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=CURRENT_DATE())) then "BGV Pending & In Training (Scheduled)"
# MAGIC
# MAGIC                                         WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE='null' or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM='null' or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<CURRENT_DATE())) then "BGV Pending & In Training (Delay)"
# MAGIC                                         
# MAGIC                                         WHEN ((BGV_CLEAR_DATE is not Null or BGV_CLEAR_DATE !="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=GETDATE())) then "In Training (Scheduled)"
# MAGIC                                         
# MAGIC                                         WHEN ((BGV_CLEAR_DATE is not Null or BGV_CLEAR_DATE !="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<GETDATE())) then "In Training (Delay)"
# MAGIC                                         
# MAGIC                                         when ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="")) then "BGV Pending & Risk Team clearance pending"
# MAGIC
# MAGIC                                         when ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (RISK_TEAM_HANDOVER_TO_HRBP is not Null or RISK_TEAM_HANDOVER_TO_HRBP !="")) then "BGV Pending & HRBP clearance pending"
# MAGIC                                         
# MAGIC                                         WHEN ((RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="") and (TARGET_RISK_CLEARANCE_DATE>=CURRENT_DATE()) and ((HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") or  (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY =""))) then "Risk Team clearance pending(Scheduled)"
# MAGIC
# MAGIC                                         WHEN ((RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="") and (TARGET_RISK_CLEARANCE_DATE<CURRENT_DATE()) and ((HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") or  (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY =""))) then "Risk Team clearance pending(Delay)"
# MAGIC
# MAGIC                                         WHEN (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Not Null or RISK_TEAM_HANDOVER_TO_HRBP !="") and (date_add(RISK_TEAM_HANDOVER_TO_HRBP,1)>=GETDATE()) then "With HRBP(Scheduled)"
# MAGIC
# MAGIC                                         WHEN (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Not Null or RISK_TEAM_HANDOVER_TO_HRBP !="") and (date_add(RISK_TEAM_HANDOVER_TO_HRBP,1)<GETDATE()) then "With HRBP(Delay)"
# MAGIC
# MAGIC                                          WHEN (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="") and (HRBP_HANDOVER_TO_RMT_TEAM is Not Null or HRBP_HANDOVER_TO_RMT_TEAM !="") and (date_add(HRBP_HANDOVER_TO_RMT_TEAM,1)>=GETDATE()) then "With RMT(Scheduled)"
# MAGIC                                          
# MAGIC
# MAGIC                                         WHEN (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="") and (HRBP_HANDOVER_TO_RMT_TEAM is Not Null or HRBP_HANDOVER_TO_RMT_TEAM !="") and (date_add(HRBP_HANDOVER_TO_RMT_TEAM,1)<GETDATE()) then "With RMT(Delay)"
# MAGIC
# MAGIC                                         
# MAGIC                                         else ("Handed over to Business")
# MAGIC                                     end
# MAGIC                                 WHERE PSID in ('4182436','4187242','4187270','4187303','4187325')
# MAGIC                

# COMMAND ----------

# MAGIC %sql
# MAGIC Update kgsonedatadb.trusted_hist_gdc_onboarding_master set SERVICE_LINE='GTA' where PSID ='4122783'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view rmt_vw as (select * from (select *,row_number() over(partition by PSID,EMPID order by HANDOVER asc) as rn from kgsonedatadb.trusted_gdc_ready_for_production) a where rn=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rmt_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
# MAGIC                USING  rmt_vw b
# MAGIC                ON a.PSID=b.PSID and a.EMP_ID=b.EMPID and a.SERVICE_LINE not in ('GTA','GTS','Canada Audit')
# MAGIC                WHEN MATCHED  THEN  
# MAGIC                 UPDATE SET a.RMT_HANDOVER_TO_INDUSTRY=
# MAGIC                 date_format(TO_DATE(b.HANDOVER,'yyyy-MM-dd'),'yyyy-MM-dd')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_ready_for_production

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from   kgsonedatadb.trusted_hist_gdc_onboarding_master where PSID='4195447'

# COMMAND ----------

# MAGIC %sql
# MAGIC update kgsonedatadb.trusted_hist_gdc_onboarding_master  set DOJ_BY_MONTH=DATE_FORMAT(DOJ,'MMM-yyyy')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct DOJ_BY_MONTH  from   kgsonedatadb.trusted_hist_gdc_onboarding_master 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from   kgsonedatadb.trusted_hist_gdc_onboarding_master where  SERVICE_LINE in ('Canada Audit','US Core Audit')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct  SERVICE_LINE from   kgsonedatadb.trusted_hist_gdc_onboarding_master 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from   kgsonedatadb.trusted_hist_gdc_onboarding_master where LATERAL_CAMPUS='CA Campus' and SERVICE_LINE in ('US Core Audit')

# COMMAND ----------

# MAGIC %sql
# MAGIC Update kgsonedatadb.trusted_hist_gdc_onboarding_master set LATERAL_CAMPUS='Lateral' where psid in('3369708','3772834','3782409','3826332','3826471','3826554','3830070','3830997','3831052','3834082','3864536','3864653','3864825','3868693','3873779','3874101','3874173','3878874','3878929','3888811','3889354','3891422','3891650','3901605','3901900','3905740','3911981','3912474','3922049','3922172','3927588','3937553','3939034','3939262','3943722','3943944','3945170','3957315','3959674','3965009','3968912','3974125','3975074','3976678','3977727','3978054','3979220','3985788','3992595','3992612','3992628','3992656','3992690','3992773','3992834','3992840','3992884','3992939','3993888','3996880','3997628','4007541','4028179','4080608','4122783','4165860','4166747','4167002','4167096','4167191')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from   kgsonedatadb.trusted_hist_gdc_risk_check  where regexp_replace(PSID,'[^0-9]','')  in('4190970','4188418','4188680')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from   kgsonedatadb.trusted_gdc_risk_check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from   kgsonedatadb.trusted_gdc_ready_for_production  where regexp_replace(PSID,'[^0-9]','') in ('3009421','3278961','3280867')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW hc_vw AS (
# MAGIC     select a.EMP_ID as OnBoarding_EMP_ID,a.PSID,b.KPMG_EMP_ID as Active_Employee_ID,c.EMP_ID as FTS_ID,d.KPMG_ID as Secondee_ID,e.KPMG_ID as Transfers_ID,f.KPMG_EMP_ID as Attrition_Pipeline_ID,g.KPMG_ID as Attrition_ID,h.KPMG_ID as Absconding_ID,i.KPMG_ID as LongLeave_ID,coalesce(b.KPMG_EMP_ID,c.EMP_ID,d.KPMG_ID,e.KPMG_ID,f.KPMG_EMP_ID,g.KPMG_ID,h.KPMG_ID,i.KPMG_ID,z.CANDIDATE__ID) as Final_KPMG_ID,coalesce(b.COST_CENTER,c.NEW_COST_CENTER,z.NEW_COST_CENTER,G.COST_CENTER) as HC_Cost_Center,
# MAGIC     case when (a.PSID=b.PS_ID or a.PSID=c.PS_ID or a.PSID=d.PSID or a.PSID=e.US_ID or a.PSID=z.PS_ID) and (i.PS_ID is null) and (f.PS_ID is null) and (g.PS_ID is null) then 'Active'
# MAGIC     when a.PSID=f.PS_ID then 'Pipeline Attrition'
# MAGIC     when a.PSID=g.PS_ID then 'Attrition'    
# MAGIC     when a.PSID=h.PS_ID then 'Abscondidng'
# MAGIC     when a.PSID=i.PS_ID then 'Long Leave' end as Candidate_Status,b.UG,b.PG,b.HIGHEST_QUALIFICATION,b.KPMG_DOJ,g.APPROVED_LWD as Attrition_LWD,coalesce(f.APPROVED_LWD,i.LEAVE_END_DATE,g.APPROVED_LWD) as LWD
# MAGIC     from kgsonedatadb.trusted_hist_gdc_onboarding_master a left join kgsonedatadb.trusted_gdc_active_hc b on a.PSID=regexp_replace(b.PS_ID,'[^0-9]','')
# MAGIC     left join kgsonedatadb.trusted_gdc_fts c on a.PSID=c.PS_ID 
# MAGIC     left join kgsonedatadb.trusted_gdc_secondee d on a.PSID=d.PSID 
# MAGIC     left join kgsonedatadb.trusted_gdc_transfers e on a.PSID=e.US_ID 
# MAGIC     left join kgsonedatadb.trusted_gdc_contractors z on a.PSID=z.PS_ID 
# MAGIC     left join kgsonedatadb.trusted_gdc_attrition_pipeline f on a.PSID=f.PS_ID
# MAGIC     left join kgsonedatadb.trusted_gdc_attrition g on a.PSID=g.PS_ID
# MAGIC     left join kgsonedatadb.trusted_gdc_absconding h on a.PSID=h.PS_ID
# MAGIC     left join kgsonedatadb.trusted_gdc_long_leave i on a.PSID=i.PS_ID
# MAGIC     where a.PSID!="WIP")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_contractors where regexp_replace(PS_ID,'[^0-9]','') = '4167096'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view hc_unique_vw as select * from ( select PSID,Final_KPMG_ID,HC_Cost_Center,case when Candidate_Status='Attrition' and Attrition_LWD<KPMG_DOJ then 'Active' when Candidate_Status='Attrition' and Attrition_LWD>KPMG_DOJ then 'Attrition' else Candidate_Status end as Candidate_Status,UG,PG,HIGHEST_QUALIFICATION,CAST(LWD as string) as LWD,row_number() over(partition by PSID order by Candidate_Status DESC) as rn from hc_vw) t where rn=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.PSID,a.JOINERS_STATUS,a.EMP_ID,b.* from kgsonedatadb.trusted_hist_gdc_onboarding_master a left join hc_vw b on trim(a.PSID)=trim(b.PSID) where a.Joiners_Status is null
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hc_unique_vw where PSID='3955145'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where EMP_ID='663384'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_ready_for_production where PSID IN ('4015524','4122783')
# MAGIC

# COMMAND ----------


    spark.sql('''create or replace temporary view rmt_vw as (select * from (select *,row_number() over(partition by PSID,EMPID order by HANDOVER desc) as rn from kgsonedatadb.trusted_gdc_ready_for_production) a where rn=1)''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rmt_vw a join kgsonedatadb.trusted_hist_gdc_onboarding_master b on a.PSID=b.PSID and a.EMPID=b.EMP_ID  where b.PSID IN ('4167191','4167002','4165860','4166747','4122783','4015524','3878963','3873179','3867861')

# COMMAND ----------

# MAGIC %sql
# MAGIC update kgsonedatadb.trusted_hist_gdc_onboarding_master set INDUSTRY_GROUP_COST_CENTER='GDC - Central Team' where PSID='4182569'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_analytics where PSID in ('4182569','4183163','4183985','4180577','4178609','4169400','4143294','4146185','4145270')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_hrbp_handover WHERE PSID='4182569'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where BCP='BCP Access Blocked' and LND_HANDOVER_TO_RISK_TEAM is not null and JOINERS_STATUS='Active' order by LND_HANDOVER_TO_RISK_TEAM desc
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


    spark.sql('''create or replace temporary view bcp_vw as (select * from (select *,row_number() over(partition by PSID order by DOJ desc) as rn from kgsonedatadb.trusted_hist_gdc_BCP) a where rn=1)''')

# COMMAND ----------

    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  bcp_vw b
               ON a.PSID=b.PSID   
               WHEN MATCHED  THEN  
                UPDATE SET a.BCP="BCP Access re-activated"''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bcp_vw where PSID='4182620'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_bcp where PSID='4182620'

# COMMAND ----------

# MAGIC %sql
# MAGIC Update kgsonedatadb.trusted_hist_gdc_onboarding_master set BGV_STATUS='No' where PSID= '4133130'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_bgv_check where CANDIDATE_FULL_NAME like '%Twinkle%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct CANDIDATES_STATUS from kgsonedatadb.trusted_hist_gdc_onboarding_master --where PSID='4182569'
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_bgv_check where PSID='4143216'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_on

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where((EMP_NAME IS NULL) or (PSID IS NULL) or (DESIGNATION IS NULL) OR (LOCATION IS NULL) OR (DOJ IS NULL )OR (GENDER IS NULL) OR (INDUSTRY_GROUP_COST_CENTER IS NULL)  OR (INDUSTRY IS NULL) OR (EMP_CATEGORY IS NULL) OR (LATERAL_CAMPUS IS NULL) OR (INDIA_ID IS NULL) OR (VDI_ID IS NULL) OR (CANDIDATE_PERSONAL_EMAIL_ID IS NULL) OR (PM IS NULL) OR (PM_EMAIL_ID IS NULL) OR (BUDDY_NAME IS NULL) OR (BUDDY_EMAIL_ID IS NULL) OR (PM='-') OR (PM_EMAIL_ID='-') OR (BUDDY_NAME='-') OR (BUDDY_EMAIL_ID='-')) and JOINERS_STATUS='Active' and CANDIDATES_STATUS!='Handed over to Business'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where CANDIDATES_STATUS ='BGV Pending & In Training (Scheduled)' and JOINERS_STATUS='Active'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE  kgsonedatadb.trusted_hist_gdc_onboarding_master set CANDIDATES_STATUS ='Risk Team clearance pending(Delay)' where PSID in ('3804649',
# MAGIC '3867861',
# MAGIC '3873179',
# MAGIC '3878963',
# MAGIC '4015524',
# MAGIC '4036568',
# MAGIC '4178671',
# MAGIC '4180577'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where EMP_ID='144580'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_bgv_check where EMPLOYEE_ID='144580'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where  JOINERS_STATUS='Active' and CANDIDATES_STATUS='Handed over to Business'

# COMMAND ----------

# MAGIC %sql
# MAGIC Update kgsonedatadb.trusted_hist_gdc_onboarding_master set CANDIDATES_STATUS='Handed over to Business' where CANDIDATES_STATUS ='With RMT(Delay)' and JOINERS_STATUS='Active'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where PSID='4168434'

# COMMAND ----------

# MAGIC %sql
# MAGIC Update kgsonedatadb.trusted_gdc_contractors
# MAGIC                  SET PS_ID=REGEXP_REPLACE(PS_ID,'[^0-9]','')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_active_hc WHERE PS_ID='4180577'

# COMMAND ----------

# MAGIC %sql
# MAGIC select EMP_NAME,COUNT(Emp_name) from kgsonedatadb.trusted_hist_gdc_onboarding_master group by EMP_NAME having count(EMP_NAME)>2--where EMP_NAME is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master  where EMP_ID is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_bgv_check

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master  where EMP_NAME like '%Rakesh%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master  where TIME_TAKEN_TO_RECEIVE_LND_CLEARANCE_DATE_FROM_LAPTOP_RECEIVED_DATE is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_bgv_check where BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_="No"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  kgsonedatadb.trusted_hist_gdc_onboarding_master  where BCP='BCP Access Blocked'

# COMMAND ----------

# MAGIC %sql
# MAGIC select b.EMP_NAME,b.PSID from kgsonedatadb.trusted_gdc_active_hc a join kgsonedatadb.trusted_hist_gdc_onboarding_master b on a.PS_ID=b.PSID 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_gdc_serviceline_mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into kgsonedatadb.config_gdc_serviceline_mapping values ('18','GDC - Central Team','Central Team','20231214','2024-01-24T15:24:44.000+0000')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_bu_update_table_list where Process_Name='gdc'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_newjoiners_status

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct File_Date from kgsonedatadb.trusted_hist_gdc_bgv_check

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct BGV_STATUS from kgsonedatadb.trusted_hist_gdc_onboarding_master -- where BGV_STATUS='-'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE  kgsonedatadb.trusted_hist_gdc_onboarding_master SET CANDIDATES_STATUS='BGV Pending & In Training (Scheduled)' WHERE ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE='null' or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM='null' or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=CURRENT_DATE())) --AND CANDIDATES_STATUS NOT IN ('Attrition','Absconding','Handed over to Business')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM  kgsonedatadb.trusted_hist_gdc_onboarding_master WHERE BGV_CLEAR_DATE is NULL

# COMMAND ----------



# COMMAND ----------

# IF col( "EMP_NAME") is null then "",
# IF col("BGV_CLEAR_DATE") is null  then "BGV Pending & In Training (Scheduled)",
# IF col("LND_HANDOVER_TO_RISK_TEAM") is null then "In Training (Scheduled)",
# IF col("RISK_TEAM_HANDOVER_TO_HRBP") ="" then "Risk Team clearance pending",
# IF col("HRBP_HANDOVER_TO_RMT_TEAM") is null  then "With HRBP",
# IF col("RMT_HANDOVER_TO_INDUSTRY") ="" then "With RMT"
# else "Handed over to Business"


spark.sql('''Update  test_vw_null
            SET CANDIDATES_STATUS= CASE 
                                        WHEN (EMP_NAME is Null or EMP_NAME = "") then ""
                                        WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=GETDATE())) then "BGV Pending & In Training (Scheduled)"

                                        WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<GETDATE())) then "BGV Pending & In Training (Delay)"
                                        
                                        WHEN ((BGV_CLEAR_DATE is not Null or BGV_CLEAR_DATE !="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=GETDATE())) then "In Training (Scheduled)"
                                        
                                        WHEN ((BGV_CLEAR_DATE is not Null or BGV_CLEAR_DATE !="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<GETDATE())) then "In Training (Delay)"
                                        
                                        when ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="")) then "BGV Pending & Risk Team clearance pending"

                                        when ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (RISK_TEAM_HANDOVER_TO_HRBP is not Null or RISK_TEAM_HANDOVER_TO_HRBP !="")) then "BGV Pending & HRBP clearance pending"
                                        
                                        WHEN ((RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="") and (TARGET_RISK_CLEARANCE_DATE>=CURRENT_DATE()) and ((HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") or  (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY =""))) then "Risk Team clearance pending(Scheduled)"

                                        WHEN ((RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="") and (TARGET_RISK_CLEARANCE_DATE<CURRENT_DATE()) and ((HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") or  (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY =""))) then "Risk Team clearance pending(Delay)"

                                        WHEN (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Not Null or RISK_TEAM_HANDOVER_TO_HRBP !="") and (date_add(RISK_TEAM_HANDOVER_TO_HRBP,1)>=GETDATE()) then "With HRBP(Scheduled)"

                                        WHEN (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Not Null or RISK_TEAM_HANDOVER_TO_HRBP !="") and (date_add(RISK_TEAM_HANDOVER_TO_HRBP,1)<GETDATE()) then "With HRBP(Delay)"

                                         WHEN (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="") and (HRBP_HANDOVER_TO_RMT_TEAM is Not Null or HRBP_HANDOVER_TO_RMT_TEAM !="") and (date_add(HRBP_HANDOVER_TO_RMT_TEAM,1)>=GETDATE()) then "With RMT(Scheduled)"
                                         

                                        WHEN (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="") and (HRBP_HANDOVER_TO_RMT_TEAM is Not Null or HRBP_HANDOVER_TO_RMT_TEAM !="") and (date_add(HRBP_HANDOVER_TO_RMT_TEAM,1)<GETDATE()) then "With RMT(Delay)"

                                        
                                        else ("Handed over to Business")
                                    end
                                WHERE CANDIDATES_STATUS NOT IN ('Attrition','Absconding','Handed over to Business')
                ''')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE  kgsonedatadb.trusted_hist_gdc_onboarding_master SET EMP_ID='122732' where EMP_NAME='Sreelakshmi, Alukunta'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct JOINERS_STATUS from kgsonedatadb.trusted_hist_gdc_onboarding_master --WHERE EMP_ID in ('143608','143821','143835','143882')

# COMMAND ----------


    spark.sql('''CREATE OR REPLACE TEMPORARY VIEW hc_vw AS (
    select a.EMP_ID as OnBoarding_EMP_ID,a.PSID,b.KPMG_EMP_ID as Active_Employee_ID,c.EMP_ID as FTS_ID,d.KPMG_ID as Secondee_ID,e.KPMG_ID as Transfers_ID,f.KPMG_EMP_ID as Attrition_Pipeline_ID,g.KPMG_ID as Attrition_ID,h.KPMG_ID as Absconding_ID,i.KPMG_ID as LongLeave_ID,coalesce(b.KPMG_EMP_ID,c.EMP_ID,d.KPMG_ID,e.KPMG_ID,f.KPMG_EMP_ID,g.KPMG_ID,h.KPMG_ID,i.KPMG_ID) as Final_KPMG_ID,coalesce(b.COST_CENTER,c.NEW_COST_CENTER) as HC_Cost_Center,
    case when (a.PSID=b.PS_ID or a.PSID=c.PS_ID or a.PSID=d.PSID or a.PSID=e.US_ID) and (i.PS_ID is null) and (f.PS_ID is null)  then 'Active'
    when a.PSID=f.PS_ID then 'Pipeline Attrition'
    when a.PSID=g.PS_ID then 'Attrition'    
    when a.PSID=h.PS_ID then 'Abscondidng'
    when a.PSID=i.PS_ID then 'Long Leave' end as Candidate_Status,b.UG,b.PG,b.HIGHEST_QUALIFICATION
    from kgsonedatadb.trusted_hist_gdc_onboarding_master a left join kgsonedatadb.trusted_gdc_active_hc b on a.PSID=b.PS_ID
    left join kgsonedatadb.trusted_gdc_fts c on a.PSID=c.PS_ID 
    left join kgsonedatadb.trusted_gdc_secondee d on a.PSID=d.PSID 
    left join kgsonedatadb.trusted_gdc_transfers e on a.PSID=e.US_ID 
    left join kgsonedatadb.trusted_gdc_attrition_pipeline f on a.PSID=f.PS_ID
    left join kgsonedatadb.trusted_gdc_attrition g on a.PSID=g.PS_ID
    left join kgsonedatadb.trusted_gdc_absconding h on a.PSID=h.PS_ID
    left join kgsonedatadb.trusted_gdc_long_leave i on a.PSID=i.PS_ID
    where a.PSID!="WIP")''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hc_vw

# COMMAND ----------

spark.sql('''create or replace temporary view hc_unique_vw as select * from ( select PSID,Final_KPMG_ID,HC_Cost_Center,Candidate_Status,UG,PG,HIGHEST_QUALIFICATION,row_number() over(partition by PSID order by Candidate_Status DESC) as rn from hc_vw) t where rn=1''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hc_unique_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master  where JOINERS_STATUS is null--WHERE EMP_ID in ('143608','143821','143835','143882')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_active_hc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master WHERE JOINERS_STATUS is null-- ('143608','143821','143835','143882')

# COMMAND ----------

# MAGIC %sql
# MAGIC update kgsonedatadb.trusted_hist_gdc_onboarding_master set CANDIDATES_STATUS='Risk Team clearance pending(Delay)' WHERE EMP_ID in ('143608','143821','143835','143882')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or Replace temporary table abi_vw as select *  from kgsonedatadb.trusted_hist_gdc_onboarding_master --WHERE CANDIDATES_STATUS='Risk Team clearance pending(Scheduled)'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from abi_vw

# COMMAND ----------

# IF col( "EMP_NAME") is null then "",
# IF col("BGV_CLEAR_DATE") is null  then "BGV Pending & In Training (Scheduled)",
# IF col("LND_HANDOVER_TO_RISK_TEAM") is null then "In Training (Scheduled)",
# IF col("RISK_TEAM_HANDOVER_TO_HRBP") ="" then "Risk Team clearance pending",
# IF col("HRBP_HANDOVER_TO_RMT_TEAM") is null  then "With HRBP",
# IF col("RMT_HANDOVER_TO_INDUSTRY") ="" then "With RMT"
# else "Handed over to Business"


spark.sql('''Update abi_vw
            SET CANDIDATES_STATUS= CASE 
                                        WHEN (EMP_NAME is Null or EMP_NAME = "") then ""
                                        WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=GETDATE())) then "BGV Pending & In Training (Scheduled)"

                                        WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<GETDATE())) then "BGV Pending & In Training (Delay)"
                                        
                                        WHEN ((BGV_CLEAR_DATE is not Null or BGV_CLEAR_DATE !="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=GETDATE())) then "In Training (Scheduled)"
                                        
                                        WHEN ((BGV_CLEAR_DATE is not Null or BGV_CLEAR_DATE !="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<GETDATE())) then "In Training (Delay)"
                                        
                                        when ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="")) then "BGV Pending & Risk Team clearance pending"

                                        when ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (RISK_TEAM_HANDOVER_TO_HRBP is not Null or RISK_TEAM_HANDOVER_TO_HRBP !="")) then "BGV Pending & HRBP clearance pending"
                                        
                                        --WHEN ((RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="") and (TARGET_RISK_CLEARANCE_DATE>=CURRENT_DATE()) and (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") or  (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="")) then "Risk Team clearance pending(Scheduled)"

                                        WHEN ((RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="") and (TARGET_RISK_CLEARANCE_DATE<CURRENT_DATE()) and (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") or  (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="")) then "Risk Team clearance pending(Delay)"

                                        WHEN (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Not Null or RISK_TEAM_HANDOVER_TO_HRBP !="") and (date_add(RISK_TEAM_HANDOVER_TO_HRBP,1)>=GETDATE()) then "With HRBP(Scheduled)"

                                        WHEN (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Not Null or RISK_TEAM_HANDOVER_TO_HRBP !="") and (date_add(RISK_TEAM_HANDOVER_TO_HRBP,1)<GETDATE()) then "With HRBP(Delay)"

                                         WHEN (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="") and (HRBP_HANDOVER_TO_RMT_TEAM is Not Null or HRBP_HANDOVER_TO_RMT_TEAM !="") and (date_add(HRBP_HANDOVER_TO_RMT_TEAM,1)>=GETDATE()) then "With RMT(Scheduled)"
                                         

                                        WHEN (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="") and (HRBP_HANDOVER_TO_RMT_TEAM is Not Null or HRBP_HANDOVER_TO_RMT_TEAM !="") and (date_add(HRBP_HANDOVER_TO_RMT_TEAM,1)<GETDATE()) then "With RMT(Delay)"

                                        
                                        else ("Handed over to Business")
                                    end
                                WHERE CANDIDATES_STATUS NOT IN ('Attrition','Absconding','Handed over to Business')
                ''')

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from kgsonedatadb.trusted_hist_gdc_onboarding_master where EMP_NAME in ('Wakodikar, Vidya','Jalan, Palak','Sneha, Janagama','Jain, Megha')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where CANDIDATES_STATUS='Handed over to Business' and NO_OF_COURSES_PENDING!=0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_hist_gdc_onboarding_master where EMP_ID="144282"

# COMMAND ----------

# MAGIC %sql
# MAGIC Update kgsonedatadb.trusted_hist_gdc_onboarding_master set CANDIDATES_STATUS like ''

# COMMAND ----------

# MAGIC %sql
# MAGIC select emp_id,count(PSID) from kgsonedatadb.trusted_hist_gdc_onboarding_master group by emp_id having count(PSID)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from kgsonedatadb.config_data_type_cast where Process_Name='gdc' and Column_Name in ('PS_ID','US_ID')

# COMMAND ----------

# MAGIC %sql
# MAGIC  from kgsonedatadb.trusted_hist_headcount_attrition_data where File_Date='20240201'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb_badrecords.trusted_hist_headcount_attrition_data_bad WHERE File_Date=20240201 AND  upper(Att_Type) like '%VOL%'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.raw_hist_csr_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_csr_budget 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_csr_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_csr_ngo_fund_util

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_csr_ngo_fund_util_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_csr_ngo_fund_budget

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_csr_ngo_fund_budget_kgdc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_csr_indirect_exp_kgs

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_csr_kgsmpl_gl

# COMMAND ----------

