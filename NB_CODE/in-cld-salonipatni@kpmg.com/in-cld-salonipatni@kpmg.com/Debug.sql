-- Databricks notebook source
select distinct File_Date from kgsonedatadb.trusted_hist_impact_air_travel_data order by File_Date desc

-- COMMAND ----------

-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'Audit-GDC' where BU2 is null and BU in ('GDC GTS-Product Development')
-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'Audit-KRC' where BU2 is null and BU in ('ADC-Pun-1-KI','Audit-Tech and DSG Management')
-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'CF-HR' where BU2 is null and BU in ('HR-KI')
-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'CH' where BU2 is null and BU in ('Capability Hubs-Research-Connected Insights','Capability Hubs-Pursuits-GMS')
-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'MC' where BU2 is null and BU in ('MC-GDN-Cloud-Engg-INV','TE-Retail_KI','MC-ASE Trusted SAP','MC-GDN-ESG ST&I','MC-EWT-BPSA','MC-GDN-Cyber-Transformation IAM-INV','MC-GDN-SAP-Tech Dev-INV','MC-TE-Cyber-Strategy-Governance','MC-PMO-ESG COE','Consultant MC-GDN-Integration')

-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'MS' where BU2 is null and BU in ('MS')

-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'CH' where BU2 is null and BU in ('Capability Hubs-Research-Connected Insights','Capability Hubs-Pursuits-GMS')
-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'RC' where BU2 is null and BU in ('RA Hub GRCS-KI','KGS RAS')

-- update trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'Tax' where BU2 is null and BU in  ('Tax-USCT','Tax-USGMS','Tax AM Compliance','Tax-BTS Corp Velocity')


-- COMMAND ----------

-- update  kgsonedatadb.trusted_hist_admin_xport_no_shows_gurgaon set BU2= 'MC' where DEPARTMENT in ('TE-Retail_KI','MC-TE-Cyber-Strategy-Governance','MC-ASE Trusted SAP','MC-GDN-ESG ST&I','MC-GDN-SAP-Tech Dev-INV') and BU2 is null

-- update  kgsonedatadb.trusted_hist_admin_xport_no_shows_gurgaon set BU2= 'Tax' where DEPARTMENT in ('Tax-USCT','Tax-USGMS') and BU2 is null

-- COMMAND ----------



 --update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'MC' where Department in ('MC-GDN-SAP-EPM-INV','MC-AE-AI COE','MC-GDN-SAP-Finance-INV','MC-GDN-SAP-SCM-INV','MC-GDN-Microsoft-AI-INV') and BU is Null

--  update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'DA' where Department in ('DA Core-AAS DPP SG') and BU is Null

-- COMMAND ----------

select distinct Location,file_Date,Dated_on from kgsonedatadb.trusted_hist_admin_xport_no_shows_hyderabad where File_Date ='20240422'

--delete from kgsonedatadb.trusted_hist_admin_xport_no_shows_hyderabad where File_Date ='20240422' and Location ='Gurgaon'





-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_hist_transport_fleet_details --where File_Date ='20240402'

-- COMMAND ----------

-- select distinct FIle_Date from kgsonedatadb.trusted_risk_euda --'20240405'

-- select distinct FIle_Date from kgsonedatadb.trusted_risk_cloud --'20240405'
-- select distinct FIle_Date from kgsonedatadb.trusted_risk_applications --'20240405'

-- delete from kgsonedatadb.trusted_hist_risk_euda where File_Date ='20240405';
-- delete from kgsonedatadb.trusted_hist_risk_cloud where File_Date ='20240405';
-- delete from kgsonedatadb.trusted_hist_risk_applications where File_Date ='20240405';


-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'MC' where Department in ('MC-ASE-SAP GGA') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'CH' where Department in ('Capability Hubs-Research-DA Hub Corporate Clients') and BU is Null


-- COMMAND ----------

select distinct Location from kgsonedatadb.trusted_hist_admin_card_swipe_master_data where File_Date = '20240410' --and Location like '%HYd%' --and PAYROLL_NUM ='122457'

-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'MC' where Department in ('BUSINESS EXCELLENCE-','MC-GATE-EngSup','MC-GDN-Cloud-Architecture-INV','MC-GDN-Cloud-Engg-INV','MC-GDN-Cyber-MDR','MC-GDN-Cyber-Transformation Archer-INV','MC-GDN-SAP-Tech Dev-INV','MC-HCA-SAP','Adv Technology-Product Strategy and Innovation','DT-DTAC','KGS-Adv-Risk Enablement Office','MC-ASE Decarb Hub','MC-ASE DT','MC-ASE Trusted SAP','MC-ASE-I&S','MC-CS-POA-Procurement','MC-EWT-BPSA','MC-EWT-Cloud&AI','MC-GDN-Cyber-Response-INV','MC-GDN-Cyber-Strategy&Gov-INV','MC-GDN-Cyber-Transformation IAM-INV','MC-GDN-ESG ST&I','MC-GDN-Microsoft-CE&Power-INV','MC-GDN-Microsoft-Data-INV','MC-GDN-Microsoft-ERP','MC-GDN-Microsoft-F&O-INV','MC-GDN-SAP-FINANCIAL','MC-PMO-ESG COE','TRANSFORMATION-P&C') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'Tax' where Department in ('Tax KDN GMS PTC','Tax-BTS Corp Velocity','Tax-USCT','Tax-USGMS','Tax AM Compliance') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'DN' where Department in ('Nexus-Business Solutions','Nexus-Architecture & Engineering') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'CH' where Department in ('Capability Hubs-BOI-GBS CI & Reporting','Capability Hubs-BOI-COE F&A','Capability Hubs-BOI-GAMG','Capability Hubs-BOI-KBS Sales Pipeline','Capability Hubs-Pursuits-GMS','CAPABILITY HUBS-RESE','Capability Hubs-Research-Connected Insights') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'CF' where Department in ('CF-IT-DC & COLLABORA') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'Audit-KRC' where Department in ('Audit-Business-Enterprise','AUDIT-DEL-D1','AUDIT-DEL-D3','AUDIT-DEL-D5','Audit-ESH and Change Management','Audit-KDN-Digital Audit','Audit-Quality, ISQM and KDN Management','Audit-Tech 1 Management','Audit-Tech and DSG Management') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'Audit-GDC' where Department in ('GDC TS - Core Data & Analytics') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'MS' where Department in ('FORENSIC-INV') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'RC' where Department in ('FORENSIC-CDD','FRM','GDCAdv-RC-RA-Technology Assurance','RAK-KM-KNOWLEDGE','RS Core Program - GMS') and BU is Null

-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set BU = 'DA' where Department in ('DA Core - Infra Capital Projects & Asset management','DA Core ESG Due Diligence','DA Core-SS-ENR','DA Core-Strategy P&SC') and BU is Null


-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where FIle_Date >= '20240408' and FIle_Date <= '20240412'
-- select count(1) from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where BU is null --471

--select * from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where BU is null and Employee_Id in ('663664','663663')


-- update kgsonedatadb.trusted_hist_admin_booked_and_no_shows set Department ='Capability Hubs-Research-GC&K', BU='CH' where BU is null and Employee_Id in ('663664','663663')


-- COMMAND ----------

--select distinct Cost_Code from kgsonedatadb.trusted_hist_admin_card_swipe_master_data where FIle_Date in ('20240407') --('20240408','20240409')

--delete from kgsonedatadb.trusted_hist_admin_card_swipe_master_data where File_Date in ('20240408','20240409')--
--select distinct File_Date, Dated_On from kgsonedatadb.trusted_hist_admin_card_swipe_master_data where File_Date in ('20240408','20240409')
select * from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where FIle_Date in ('20240408','20240409')

--delete from kgsonedatadb.trusted_hist_admin_card_swipe_master_data where File_Date='20240409' and Dated_On = (select min(Dated_On) from kgsonedatadb.trusted_hist_admin_card_swipe_master_data where File_Date='20240409')


-- COMMAND ----------

select distinct APPROVAL_STATUS from kgsonedatadb.trusted_it_talent_connect_daily_transfer_report

-- COMMAND ----------

-- delete from kgsonedatadb_badrecords.trusted_hist_admin_transaction_report_bad where File_Date = '20240330'
-- delete from  kgsonedatadb.raw_hist_admin_transaction_report where File_Date = '20240330'

select * from  kgsonedatadb_badrecords.trusted_hist_admin_transaction_report_bad where File_Date = '20240330'

-- COMMAND ----------

select *
  from kgsonedatadb.raw_curr_admin_transaction_report --where File_Date ='20240330' and  Transaction_Time is null

-- COMMAND ----------

select distinct Transaction_Time  from kgsonedatadb.raw_hist_admin_transaction_report where File_Date = '20240301' and Dated_On = (select max(Dated_On) from kgsonedatadb.raw_hist_admin_transaction_report where File_Date = '20240301')

-- COMMAND ----------

-- %python
-- from pyspark.sql.functions import *;
-- # currentDf = spark.createDataFrame([("3/1/2024 16:46", 0)], ["ts", "myColumn"])

-- inputData = [("3/1/2024 16:46")]
-- columns=["timestamp_1"]
-- currentDf=spark.createDataFrame(
--         data = inputData,
--         schema = columns)

-- currentDf = currentDf.withColumn("TRANSACTION_TIME", from_unixtime(unix_timestamp("timestamp_1",'MM/dd/yyyy hh:mm'),'MM/dd/yyyy HH:mm:ss'))

-- display(currentDf)

-- COMMAND ----------

select distinct Transaction_Time  from kgsonedatadb.raw_hist_admin_transaction_report where File_Date = '20240301' and Dated_On = (select min(Dated_On) from kgsonedatadb.raw_hist_admin_transaction_report where File_Date = '20240301')

-- COMMAND ----------

select * from kgsonedatadb_badrecords.trusted_hist_admin_transaction_report_bad order by File_Date desc

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where File_Date in ('20240318') and EMployee_ID in ('68491')
--Employee_Name like '%Vaidya%Parul%'

-- select * from kgsonedatadb.trusted_hist_admin_card_swipe_master_data where File_Date in ('20240318') and PAYROLL_Num in ('68491')
-- or  NAME___REG_NUM like '%Vaidya%Parul%'


-- COMMAND ----------

select distinct File_Date,Dated_On from kgsonedatadb.trusted_hist_admin_xport_no_shows_gurgaon order by File_Date

-- COMMAND ----------

select distinct APPROVAL_STATUS,FILE_DATE from kgsonedatadb.trusted_it_talent_connect_daily_transfer_report

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_gdc_onboarding_master

-- COMMAND ----------

select * from kgsonedatadb.config_admin_cc_bu_mapping where Cost_centre like '%Adv Te%'

Adv Technology Service Mgt and Operations - MC
GDC Audit � AMDC - Audit-GDC
Adv Technology Service Mgt and Operations

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals_kochi where BU2 is null 
and
DEPARTMENT like '%GDC%' and 
File_Date='20240222'

--update trusted_hist_admin_xport_planned_vs_actuals set BU2='Audit-GDC' where BU like 'GDC Audit%' and BU2 is null
--update kgsonedatadb.trusted_admin_xport_planned_vs_actuals_kochi set BU2='Audit-GDC' where 
File_Date='20240222' and BU2 is null

-- COMMAND ----------

select * from kgsonedatadb.config_data_type_cast where Process_Name='admin' 
--and Delta_Table_Name like 'lo%'

-- COMMAND ----------



-- COMMAND ----------

-- delete from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where File_Date < '20240215'

select file_Date, count(1) from kgsonedatadb.trusted_hist_admin_booked_and_no_shows group by file_Date

-- COMMAND ----------

select distinct File_Date,Dated_On from kgsonedatadb.trusted_hist_admin_transaction_report where File_Date='20240213'

-- COMMAND ----------

select distinct File_Date,Dated_On,count(*) from kgsonedatadb.trusted_hist_admin_transaction_report group by File_Date,Dated_On;

-- delete from kgsonedatadb.trusted_hist_admin_transaction_report where File_Date = '20240211' and Dated_On != (select max(Dated_On) from kgsonedatadb.trusted_hist_admin_transaction_report where File_Date = '20240211')



-- COMMAND ----------

select location,max(File_Date) from kgsonedatadb.trusted_hist_admin_xport_no_shows group by location

-- COMMAND ----------

select location,max(File_Date) from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals group by location

-- COMMAND ----------

select distinct Location,File_Date,Dated_On from kgsonedatadb.trusted_hist_admin_xport_no_shows where Location ='Bangalore' and File_Date= '20240206' order by File_Date desc;


select distinct Location,File_Date,Dated_On from kgsonedatadb.trusted_hist_admin_xport_no_shows where Location ='Bangalore' and File_Date= '20240206' and Dated_On != (select max(Dated_On) from kgsonedatadb.trusted_hist_admin_xport_no_shows where Location ='Bangalore' and File_Date= '20240206');


--delete from kgsonedatadb.trusted_hist_admin_xport_no_shows where Location ='Bangalore' and File_Date= '20240206' and Dated_On != (select max(Dated_On) from kgsonedatadb.trusted_hist_admin_xport_no_shows where Location ='Bangalore' and File_Date= '20240206')


-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_it_software_database where File_Date ='20240207'

-- COMMAND ----------

select distinct Location,File_Date,Dated_On from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where Location ='Gurgaon' and File_Date= '20240207' order by File_Date desc;


select distinct Location,File_Date,Dated_On from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where Location ='Gurgaon' and File_Date= '20240207' and Dated_On != (select max(Dated_On) from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where Location ='Gurgaon' and File_Date= '20240207');


--delete from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where Location ='Gurgaon' and File_Date= '20240207' and Dated_On != (select max(Dated_On) from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where Location ='Gurgaon' and File_Date= '20240207')


-- COMMAND ----------

--delete from kgsonedatadb.trusted_hist_admin_xport_no_shows where Location='Kolkata' and Date='2024-01-23' and FIle_Date='20240123';


--delete from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where Location='Kolkata' and Date='2024-01-23' and FIle_Date='20240123';


-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_admin_locationwise_holidaylist

-- COMMAND ----------



-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where BU is null order by From_Date

-- COMMAND ----------

--select distinct Date,File_Date,Location from trusted_hist_admin_xport_planned_vs_actuals where Date<'2024-01-15'



--delete from kgsonedatadb.trusted_hist_admin_xport_no_shows where Date<'2024-01-15'



-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2='CF-HR' where Department='HR-KI' and BU2='HR-KI'

--update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set Date='2024-01-17' where Location='Kochi' and File_Date='20240117' and Date='2024-01-16'

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set Date='2024-01-17' where Location='Kochi' and File_Date='20240117' and Date='2024-01-16'


-- COMMAND ----------

select * from kgsonedatadb.config_admin_cc_bu_mapping where Cost_centre='HR-KI'

-- COMMAND ----------


select * from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals --where BU2='HR';

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals  set BU2='HR-KI' where Department='HR-KI' and BU2='HR'

-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2='MS' where BU2 is null and Department='DBO-People Ops - KI';

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2='HR' where BU2 is null and Department='HR-KI';

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set Date ='2024-01-22' where File_Date ='20240122' and Date='2024-01-20' and Location='Pune'


-- COMMAND ----------

select * from kgsonedatadb.config_admin_cc_bu_mapping ;
where BU2 like  'HR'
-- in ('DBO-People Ops - KI','HR-KI')

-- update kgsonedatadb.config_admin_cc_bu_mapping set BU2='CF-HR' where Cost_Centre='HR-KI' and BU2='HR'



-- COMMAND ----------

select * from 
--kgsonedatadb.trusted_hist_admin_xport_no_shows 
test_admin_xport_no_shows

-- COMMAND ----------

-- insert into kgsonedatadb.trusted_hist_admin_xport_no_shows 
-- select * from test_admin_xport_no_shows

-- COMMAND ----------

--drop table test_admin_xport_no_shows

--truncate table kgsonedatadb.trusted_hist_admin_xport_no_shows

-- COMMAND ----------


-- CREATE TEMPORARY VIEW test_admin_xport_no_shows AS 
-- select LOCATION,DATE,EMPLOYEE_NUMBER,NAME,DEPARTMENT,BU2,FILE_DATE,Dated_On
--  from (
--   select *,row_number() over (
--     partition by File_Date,Location,Date,Department,Name,Employee_number,BU2 order by Date
--   ) rn 
--   from kgsonedatadb.trusted_hist_admin_xport_no_shows
-- ) t 
-- where rn=1;

-- COMMAND ----------

select count(1) from (select *,row_number() over (partition by File_Date,Location,Date,Department,Name,Employee_number,BU2 order by Date) rn from kgsonedatadb.trusted_hist_admin_xport_no_shows) where rn=1

-- COMMAND ----------

select * from (select row_number() over (partition by Location,Date,DEPARTMENT,Planned,Actual,NO_SHOWS,BU2,Dated_On,FILE_DATE order by Date) rn,*
                  from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals) where 
                  rn>1 
                  --and
                  -- Location='Bangalore' 
                  --and File_Date ='20240118'
                  -- Date>='2024-01-15'

-- COMMAND ----------

-- delete from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where File_Date in (
--   '20240118'
--   ) and Location='Pune' and Dated_on != (select max(Dated_on) from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where File_Date='20240118' and Location='Pune')

-- COMMAND ----------

-- delete from kgsonedatadb.trusted_hist_admin_xport_no_shows where File_Date in (
--   '20240118'
--   --,'20240103','20240104','20240105','20240108','20240109','20240110','20240111','20240112'
--   ) and Location='Pune' and Dated_on != (select max(Dated_on) from kgsonedatadb.trusted_hist_admin_xport_no_shows where File_Date='20240118' and Location='Pune')

-- COMMAND ----------

select distinct Dated_On from kgsonedatadb.trusted_hist_admin_xport_no_shows where File_Date in (
  '20240102'
  --,'20240103','20240104','20240105','20240108','20240109','20240110','20240111','20240112'
  ) and Location='Bangalore' and Dated_on != (select max(Dated_on) from kgsonedatadb.trusted_hist_admin_xport_no_shows where File_Date='20240102' )

-- COMMAND ----------

select Location,File_Date,Dated_On,count(1) from kgsonedatadb.trusted_hist_admin_xport_no_shows group by Location,File_Date,Dated_On order by Location,File_Date
--where File_Date like '2023%'

--select distinct File_Date from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals --where File_Date like '2023%'

-- COMMAND ----------

select File_Date,Dated_On,Location,count(1) from kgsonedatadb.trusted_hist_admin_xport_no_shows group by File_Date,Dated_On,Location having File_Date>='20240101'

-- COMMAND ----------

select * from (select row_number() over (partition by File_Date,Location,Date,Department,Name,Employee_number,BU2 order by Date) rn,*
                  from kgsonedatadb.trusted_hist_admin_xport_no_shows) where 
                  rn>1 
                  --and
                  -- Location='Bangalore' 
                  --and File_Date ='20240118'
                  -- Date>='2024-01-15'

-- COMMAND ----------

select * from (select row_number() over (partition by File_Date,Location,Date,Department,BU2,Planned,actual,no_shows order by Date) rn,*
                  from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals) where 
                  rn>1 and location !='Gurgaon'
                  --and
                  -- Location='Bangalore' 
                  --and File_Date ='20240118'
                  -- Date>='2024-01-15'

-- COMMAND ----------

select * from (select *, row_number() over (partition by Department order by Date) rn
                  from kgsonedatadb.trusted_hist_admin_xport_no_shows where File_Date ='20240116' and Location='Kochi' and Department ='CF-Facilities & Admin') t where rn=2
                  
                  -- and EMployee_Number ='8658';
                 

-- COMMAND ----------

select * from kgsonedatadb.config_admin_cc_bu_mapping where Cost_centre in ('Audit-KGS')

-- COMMAND ----------

select * from kgsonedatadb.raw_hist_admin_transport_data where Location ='Kochi' and 

-- COMMAND ----------

--UPDATE kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals SET BU2 = 'AUDIT-GDC' WHERE Department IN ('GDC-Canada Audit Support','GDC-US Audit Support') AND BU2 IS NULL;

--UPDATE kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals SET BU2 = 'Tax' WHERE Department IN ('TAX') AND BU2 IS NULL;

--update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 ='MC' where Department ='TE-ES-WORKDAY' and BU2 is null


-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where 
--Department in ('GDC-Canada Audit Support','GDC-US Audit Support') AND BU2 IS NULL
BU2 is null

-- COMMAND ----------

select distinct Location from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where Location like '%Gurgaon%'


--update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set location = 'Gurgaon' where Location like '%Gurgaon%'


-- COMMAND ----------

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'ADMIN' WHERE upper(Department) IN ('ADMIN SUPPORT-KI','ADMINISTRATION','ADMINISTRATION-KI','SUPPORT TELECOM-KI','SUPPORT-OFFICE BOY-KGS','SUPPORT-OFFICE BOY-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'AUDIT-GDC' WHERE upper(Department) IN ('AUDIT- GDC','GDC AMDC','GDC-KGS') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'AUDIT-KRC' WHERE upper(Department) IN ('AUDIT- BSR','AUDIT- KRC','AUDIT-KGS','AUDIT-KI','RISK MANAGEMENT-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'CF' WHERE upper(Department) IN ('CF-HR-KGS','CF-IT-DC & COLLABORATION','CF-IT-IT OPERATIONS','CF-KGS','CF-KI','CORPORATE FUNCTIONS-KGS','CORPORATE FUNCTIONS-KI','CPT-KI','SUPPORT-CSR-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'CF-HR' WHERE upper(Department) IN ('HR-KGS') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'CH' WHERE upper(Department) IN ('CAPABILITY HUBS-KGS','DEDICATED OPS','GSS-MAR-COMM-KGS','RAK-KGS','RESOURCE MANAGEMENT','RPO') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'DA' WHERE upper(Department) IN ('DA','DEAL ADVISORY-KI','ESG (KI)','VALUATIONS-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'DN' WHERE upper(Department) IN ('DIGITAL NEXUS_F','NEXUS-ENGINEERING AND OPS','NEXUS-KGS') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'IT' WHERE upper(Department) IN ('IT-KGS','TECHNOLOGY-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'KI' WHERE upper(Department) IN ('LEGAL-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'MC' WHERE upper(Department) IN ('ADVISORY TECHNOLOGY','DIGITAL LIGHTHOUSE_KI','MC-ADVISORY TECH-ENGG SVCS','MC-CLIMATE DATA & TECH-BUSINESS','MC-CLIMATE DATA AND TECH-ENGG','MC-CLOUD AND DEV OPS','MC-CLOUD-APPLICATIONENGG','MC-CO-CIS-COMMERCIAL CUSTOMER','MC-CO-CIS-COMMERCIAL CUSTOMER-FS','MC-EWT-DPBI','MC-FSS-TECH SERVICES-JAVA-PSQL','MC-GDN-COMMERCIAL CUSTOMER','MC-GDN-SAP-LOGISTICS','MC-GDN-TD-PMO','MC-KGS','MC-PE CSD SERVICE NOW','MC-SPECTRUM-ET-QE','MC-TAX TECH-PROCUREMENT','MC-TE-CYBER-STRATEGY AND GOVERNANCE','PROJECT 1','SAP-KI','TE-ES-GDN-EVOLVE-SAP-SCM','TE-ES-GDN-WORKDAY-EPM AND ANALYTICS','TE-ES-KGS','TE-PLATFORMS-NO CODE ENTERPRISE APPLICATION UNQORK','TRANSFORMATION-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'MS' WHERE upper(Department) IN ('ANALYTICS-KGS','FORENSIC-KI','INSIGHT-LED SALES-KI','MANAGED SERVICES','MS-FS-DIAMOND-KGS','MS-FS-PEARL-KGS','MS-FS-RUBICON-KGS','MS-MANAGEMENT-KGS') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'RC' WHERE upper(Department) IN ('ADV CORPORATE FUNCTIONS','GRCS-KI','RC-KGS','RISK ADVISORY') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'TAX' WHERE upper(Department) IN ('TAX MANAGEMENT','TAX TECH - TTP','TAX TECH-IGNITION','TAX-KGS','TAX-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_PLANNED_VS_ACTUALS SET BU2 = 'AUDIT-GDC' WHERE upper(Department) IN ('GDC -US RISK MANAGEMENT PERSONAL INDEPENDENCE') AND BU2 IS NULL;


-- COMMAND ----------

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'ADMIN' WHERE upper(Department) IN ('ADMIN SUPPORT-KI','ADMINISTRATION','ADMINISTRATION-KI','SUPPORT TELECOM-KI','SUPPORT-OFFICE BOY-KGS','SUPPORT-OFFICE BOY-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'AUDIT-GDC' WHERE upper(Department) IN ('AUDIT- GDC','GDC AMDC','GDC-KGS') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'AUDIT-KRC' WHERE upper(Department) IN ('AUDIT- BSR','AUDIT- KRC','AUDIT-KGS','AUDIT-KI','RISK MANAGEMENT-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'CF' WHERE upper(Department) IN ('CF-HR-KGS','CF-IT-DC & COLLABORATION','CF-IT-IT OPERATIONS','CF-KGS','CF-KI','CORPORATE FUNCTIONS-KGS','CORPORATE FUNCTIONS-KI','CPT-KI','SUPPORT-CSR-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'CF-HR' WHERE upper(Department) IN ('HR-KGS') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'CH' WHERE upper(Department) IN ('CAPABILITY HUBS-KGS','DEDICATED OPS','GSS-MAR-COMM-KGS','RAK-KGS','RESOURCE MANAGEMENT','RPO') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'DA' WHERE upper(Department) IN ('DA','DEAL ADVISORY-KI','ESG (KI)','VALUATIONS-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'DN' WHERE upper(Department) IN ('DIGITAL NEXUS_F','NEXUS-ENGINEERING AND OPS','NEXUS-KGS') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'IT' WHERE upper(Department) IN ('IT-KGS','TECHNOLOGY-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'KI' WHERE upper(Department) IN ('LEGAL-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'MC' WHERE upper(Department) IN ('ADVISORY TECHNOLOGY','DIGITAL LIGHTHOUSE_KI','MC-ADVISORY TECH-ENGG SVCS','MC-CLIMATE DATA & TECH-BUSINESS','MC-CLIMATE DATA AND TECH-ENGG','MC-CLOUD AND DEV OPS','MC-CLOUD-APPLICATIONENGG','MC-CO-CIS-COMMERCIAL CUSTOMER','MC-CO-CIS-COMMERCIAL CUSTOMER-FS','MC-EWT-DPBI','MC-FSS-TECH SERVICES-JAVA-PSQL','MC-GDN-COMMERCIAL CUSTOMER','MC-GDN-SAP-LOGISTICS','MC-GDN-TD-PMO','MC-KGS','MC-PE CSD SERVICE NOW','MC-SPECTRUM-ET-QE','MC-TAX TECH-PROCUREMENT','MC-TE-CYBER-STRATEGY AND GOVERNANCE','PROJECT 1','SAP-KI','TE-ES-GDN-EVOLVE-SAP-SCM','TE-ES-GDN-WORKDAY-EPM AND ANALYTICS','TE-ES-KGS','TE-PLATFORMS-NO CODE ENTERPRISE APPLICATION UNQORK','TRANSFORMATION-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'MS' WHERE upper(Department) IN ('ANALYTICS-KGS','FORENSIC-KI','INSIGHT-LED SALES-KI','MANAGED SERVICES','MS-FS-DIAMOND-KGS','MS-FS-PEARL-KGS','MS-FS-RUBICON-KGS','MS-MANAGEMENT-KGS') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'RC' WHERE upper(Department) IN ('ADV CORPORATE FUNCTIONS','GRCS-KI','RC-KGS','RISK ADVISORY') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'TAX' WHERE upper(Department) IN ('TAX MANAGEMENT','TAX TECH - TTP','TAX TECH-IGNITION','TAX-KGS','TAX-KI') AND BU2 IS NULL;

-- UPDATE kgsonedatadb.TRUSTED_HIST_ADMIN_XPORT_NO_SHOWS SET BU2 = 'AUDIT-GDC' WHERE upper(Department) IN ('GDC -US RISK MANAGEMENT PERSONAL INDEPENDENCE') AND BU2 IS NULL;


-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'Admin' where Department in ('Admin Support-KI','ADMINISTRATION','Administration-KI','Support Telecom-KI','Support-Office Boy-KGS','Support-Office Boy-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'Audit-GDC' where Department in ('Audit- GDC','GDC AMDC','GDC-KGS') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'Audit-KRC' where Department in ('Audit- BSR','Audit- KRC','Audit-KGS','Audit-KI','Risk Management-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'CF' where Department in ('CF-HR-KGS','CF-IT-DC & COLLABORATION','CF-IT-IT Operations','CF-KGS','CF-KI','Corporate Functions-KGS','Corporate Functions-KI','CPT-KI','Support-CSR-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'CF-HR' where Department in ('HR-KGS') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'CH' where Department in ('Capability Hubs-KGS','Dedicated Ops','GSS-Mar-Comm-KGS','RAK-KGS','Resource Management','RPO') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'DA' where Department in ('DA','Deal Advisory-KI','ESG (KI)','Valuations-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'DN' where Department in ('Digital Nexus_F','Nexus-Engineering And Ops','Nexus-KGS') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'IT' where Department in ('IT-KGS','Technology-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'KI' where Department in ('Legal-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'MC' where Department in ('Advisory Technology','Digital Lighthouse_KI','MC-Advisory Tech-Engg Svcs','MC-CLIMATE DATA & TECH-BUSINESS','MC-Climate Data and Tech-Engg','MC-Cloud and Dev Ops','MC-Cloud-ApplicationEngg','MC-CO-CIS-Commercial Customer','MC-CO-CIS-Commercial Customer-FS','MC-EWT-DPBI','MC-FSS-Tech Services-Java-PSQL','MC-GDN-Commercial Customer','MC-GDN-SAP-Logistics','MC-GDN-TD-PMO','MC-KGS','MC-PE CSD SERVICE NOW','MC-Spectrum-ET-QE','MC-Tax Tech-Procurement','MC-TE-Cyber-Strategy And Governance','Project 1','SAP-KI','TE-ES-GDN-Evolve-SAP-SCM','TE-ES-GDN-Workday-EPM And Analytics','TE-ES-KGS','TE-Platforms-No Code Enterprise Application Unqork','Transformation-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'MS' where Department in ('Analytics-KGS','Forensic-KI','Insight-Led Sales-KI','Managed Services','MS-FS-Diamond-KGS','MS-FS-Pearl-KGS','MS-FS-Rubicon-KGS','MS-Management-KGS') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'RC' where Department in ('Adv Corporate Functions','GRCS-KI','RC-KGS','Risk Advisory') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'Tax' where Department in ('Tax Management','Tax Tech - TTP','Tax Tech-Ignition','Tax-KGS','Tax-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2 = 'Audit-GDC' where Department in ('GDC -US Risk Management Personal Independence') and BU2 is null;


-- COMMAND ----------

update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'Tax' where Department in ('Tax-PMO','Tax-BTS-Operating Partnership') and BU2 is null

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals  where BU2 is null

-- COMMAND ----------

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'Admin' where Department in ('Admin Support-KI','ADMINISTRATION','Administration-KI','Support Telecom-KI','Support-Office Boy-KGS','Support-Office Boy-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'Audit-GDC' where Department in ('Audit- GDC','GDC AMDC','GDC-KGS') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'Audit-KRC' where Department in ('Audit- BSR','Audit- KRC','Audit-KGS','Audit-KI','Risk Management-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'CF' where Department in ('CF-HR-KGS','CF-IT-DC & COLLABORATION','CF-IT-IT Operations','CF-KGS','CF-KI','Corporate Functions-KGS','Corporate Functions-KI','CPT-KI','Support-CSR-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'CF-HR' where Department in ('HR-KGS') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'CH' where Department in ('Capability Hubs-KGS','Dedicated Ops','GSS-Mar-Comm-KGS','RAK-KGS','Resource Management','RPO') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'DA' where Department in ('DA','Deal Advisory-KI','ESG (KI)','Valuations-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'DN' where Department in ('Digital Nexus_F','Nexus-Engineering And Ops','Nexus-KGS') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'IT' where Department in ('IT-KGS','Technology-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'KI' where Department in ('Legal-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'MC' where Department in ('Advisory Technology','Digital Lighthouse_KI','MC-Advisory Tech-Engg Svcs','MC-CLIMATE DATA & TECH-BUSINESS','MC-Climate Data and Tech-Engg','MC-Cloud and Dev Ops','MC-Cloud-ApplicationEngg','MC-CO-CIS-Commercial Customer','MC-CO-CIS-Commercial Customer-FS','MC-EWT-DPBI','MC-FSS-Tech Services-Java-PSQL','MC-GDN-Commercial Customer','MC-GDN-SAP-Logistics','MC-GDN-TD-PMO','MC-KGS','MC-PE CSD SERVICE NOW','MC-Spectrum-ET-QE','MC-Tax Tech-Procurement','MC-TE-Cyber-Strategy And Governance','Project 1','SAP-KI','TE-ES-GDN-Evolve-SAP-SCM','TE-ES-GDN-Workday-EPM And Analytics','TE-ES-KGS','TE-Platforms-No Code Enterprise Application Unqork','Transformation-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'MS' where Department in ('Analytics-KGS','Forensic-KI','Insight-Led Sales-KI','Managed Services','MS-FS-Diamond-KGS','MS-FS-Pearl-KGS','MS-FS-Rubicon-KGS','MS-Management-KGS') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'RC' where Department in ('Adv Corporate Functions','GRCS-KI','RC-KGS','Risk Advisory') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'Tax' where Department in ('Tax Management','Tax Tech - TTP','Tax Tech-Ignition','Tax-KGS','Tax-KI') and BU2 is null;

-- update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2 = 'Audit-GDC' where Department in ('GDC -US Risk Management Personal Independence') and BU2 is null;


-- COMMAND ----------

select distinct DEPARTMENT from kgsonedatadb.trusted_hist_admin_xport_no_shows where BU2 is null

-- COMMAND ----------

select distinct trim(Location) from kgsonedatadb.trusted_hist_admin_xport_no_shows

-- COMMAND ----------


--update kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals set BU2='Audit-GDC' where  DEPARTMENT='GDC -US Risk Management Personal Independence' and BU2 is null;


update kgsonedatadb.trusted_hist_admin_xport_no_shows set BU2='Audit-GDC' where  DEPARTMENT='GDC -US Risk Management Personal Independence' and BU2 is null;


-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where BU is null or BU =' ' or BU ='-'

-- select distinct EMPLOYEE_ID,DEPARTMENT from 
-- --kgsonedatadb.trusted_hist_admin_Daily_Attendance_Report
-- kgsonedatadb.trusted_hist_admin_Visited_without_Bookings 
-- where Employee_Id in ('91739','141841','51668') and DEPARTMENT !='-'

-- COMMAND ----------

select * from kgsonedatadb.config_admin_cc_bu_mapping where Cost_centre in 
('GDC -US Risk Management Personal Independence')
--('Audit-Business','Audit-FS-Investment Management','Audit-FS-AQPP','GDC Audit ACH')

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_admin_xport_no_shows where BU2 is null

-- COMMAND ----------

--truncate table kgsonedatadb.trusted_hist_admin_booked_and_no_shows

-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_hist_admin_Visited_without_Bookings where File_Date like '2024%'

-- COMMAND ----------

(select distinct BU,'booked_and_no_shows' as Source from kgsonedatadb.trusted_hist_admin_booked_and_no_shows union
select distinct BU2,'xport_planned_vs_actuals' as Source from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals union
select distinct BU2,'xport_no_shows' as Source from kgsonedatadb.trusted_hist_admin_xport_no_shows) 


-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_hist_admin_booked_and_no_shows where File_Date like '2024%';
select distinct File_Date from kgsonedatadb.trusted_hist_admin_xport_planned_vs_actuals where File_Date like '2024%';
select distinct File_Date from kgsonedatadb.trusted_hist_admin_xport_no_shows where File_Date like '2024%';


-- COMMAND ----------

select * from (select * from (select distinct DEPARTMENT,'Daily_Attendance' as Source from kgsonedatadb.trusted_hist_admin_Daily_Attendance_Report where File_Date like '2024%'
union
select distinct DEPARTMENT,'Visited_without_Bookings' as Source from kgsonedatadb.trusted_hist_admin_Visited_without_Bookings where File_Date like '2024%'
union
select distinct DEPARTMENT,'Transport_Data' as Source from kgsonedatadb.trusted_hist_admin_Transport_Data where File_Date like '2024%'
union
select distinct DEPARTMENT,'No_Shows' as Source from kgsonedatadb.trusted_hist_admin_No_Shows where File_Date like '2024%') t left join kgsonedatadb.config_admin_cc_bu_mapping c on t.DEPARTMENT = c.Cost_centre) where Cost_centre is null --and Department ='MS-Forensic'



-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_admin_Transport_Data where Department ='MS-Forensic'

-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_hist_admin_Daily_Attendance_Report where File_Date like '2024%'; -- 1 Jan
select distinct File_Date from kgsonedatadb.trusted_hist_admin_Visited_without_Bookings where File_Date like '2024%'; --4 Jan
select distinct File_Date from kgsonedatadb.trusted_hist_admin_Transport_Data where File_Date like '2024%'; --2 Jan

select distinct File_Date from kgsonedatadb.trusted_hist_admin_No_Shows where File_Date like '2024%'; --2 Jan


Booked_And_No_Shows - 4 Jan
Xport_Planned_vs_Actuals - 2 Jan
Xport_No_Shows - 2 Jan


-- COMMAND ----------

select distinct File_Date from kgsonedatadb.raw_hist_admin_No_Shows where File_Date like '2024%'; --2 Jan

-- COMMAND ----------

--delete from kgsonedatadb.raw_hist_admin_No_Shows where File_Date like '2024%'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC config_table='config_admin_cc_bu_mapping'
-- MAGIC config_hist_table='config_hist_admin_cc_bu_mapping'
-- MAGIC
-- MAGIC sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+config_table)
-- MAGIC
-- MAGIC month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
-- MAGIC
-- MAGIC query = f"SELECT COUNT(*) FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
-- MAGIC print(query)
-- MAGIC cursor.execute(query)
-- MAGIC result = cursor.fetchone()[0]
-- MAGIC print(result)

-- COMMAND ----------

select distinct FILE_DATE,Dated_On from kgsonedatadb.config_hist_admin_cc_bu_mapping


delete from kgsonedatadb.config_hist_admin_cc_bu_mapping where File_Date = '20230831'

-- COMMAND ----------

select distinct File_Date,Dated_On from kgsonedatadb.config_admin_cc_bu_mapping

-- COMMAND ----------

delete from kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report where File_Date='20240104'; --10735
delete from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report where File_Date='20240104';



-- COMMAND ----------

select count(1) from kgsonedatadb.raw_hist_impact_air_travel_data where File_Date = '20240103'

-- COMMAND ----------

select count(1) from kgsonedatadb.trusted_stg_impact_air_travel_data where File_Date = '20240103'

-- COMMAND ----------

select count(1) from kgsonedatadb_badrecords.trusted_hist_impact_air_travel_data_bad where File_Date = '20240103'

-- COMMAND ----------

select distinct File_DAte, Dated_On from kgsonedatadb.config_admin_location_mapping

-- COMMAND ----------

--delete from kgsonedatadb.raw_hist_impact_admin_data where File_Date = '20231207'

-- COMMAND ----------

--delete from kgsonedatadb.trusted_hist_impact_admin_data where File_Date = '20231207'

-- COMMAND ----------

select count(1) from kgsonedatadb.raw_hist_it_talent_connect_daily_transfer_report where Dated_on = (select max(Dated_on) from kgsonedatadb.raw_hist_it_talent_connect_daily_transfer_report ) --394

select count(1) from kgsonedatadb.raw_hist_it_talent_connect_daily_exit_report where Dated_on = (select max(Dated_on) from kgsonedatadb.raw_hist_it_talent_connect_daily_exit_report )




-- COMMAND ----------

select * from kgsonedatadb_badrecords.trusted_hist_it_talent_connect_daily_transfer_report_bad  where Dated_on = (select max(Dated_on) from kgsonedatadb_badrecords.trusted_hist_it_talent_connect_daily_transfer_report_bad  )

-- COMMAND ----------

select count(1) from kgsonedatadb.raw_hist_headcount_monthly_employee_details where File_Date='20231117'; --22131
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_maternity_cases where File_Date = '20231117'; --227
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_secondee_outward where File_Date = '20231117'; --143
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_academic_trainee where File_Date = '20231117'; --3
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker where File_Date = '20231117'; --127
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker_resigned where File_Date = '20231117'; --15
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_from_ki where File_Date = '20231117'; --285
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_resigned where File_Date = '20231117'; --16
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_resigned_and_left where File_Date = '20231117'; --259
select count(1) from kgsonedatadb.raw_hist_headcount_monthly_sabbatical where File_Date = '20231117'; --25

-- COMMAND ----------

select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_employee_details_bad where File_Date='20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_maternity_cases_bad where File_Date = '20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_secondee_outward_bad where File_Date = '20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_academic_trainee_bad where File_Date = '20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_bad where File_Date = '20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_resigned_bad where File_Date = '20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_from_ki_bad where File_Date = '20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_resigned_bad where File_Date = '20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_resigned_and_left_bad where File_Date = '20231117'; --0
select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_sabbatical_bad where File_Date = '20231117'; --0

-- COMMAND ----------

-- select distinct File_Date,Dated_On from kgsonedatadb.trusted_hist_headcount_contingent

select * from kgsonedatadb.trusted_hist_headcount_contingent where File_Date = '20230919'

-- COMMAND ----------

delete from kgsonedatadb.raw_hist_bgv_progress_sheet where File_Date = '20231119'
delete from kgsonedatadb.trusted_hist_bgv_progress_sheet where File_Date = '20231119'


-- COMMAND ----------


select * from (select distinct a.DESIGNATION,b.Position from kgsonedatadb.trusted_impact_air_travel_data a left join kgsonedatadb.config_dim_level_wise b on a.DESIGNATION=b.Position) where Position is not Null

-- COMMAND ----------


select distinct DESIGNATION,LEVELWISE_MAPPING from kgsonedatadb.trusted_impact_csr_data

-- COMMAND ----------

select count(1) from kgsonedatadb.raw_hist_it_talent_connect_daily_transfer_report where File_Date ='20231103'; --419
select count(1) from kgsonedatadb.trusted_hist_it_talent_connect_daily_transfer_report where File_Date ='20231103'; --384
select count(1) from kgsonedatadb_badrecords.trusted_hist_it_talent_connect_daily_transfer_report_bad where File_Date ='20231103'; --35


select * from kgsonedatadb_badrecords.trusted_hist_it_talent_connect_daily_transfer_report_bad where File_Date ='20231103'; --35


-- COMMAND ----------

select * from kgsonedatadb.config_data_type_cast where Delta_Table_Name='idne_gender_target_vs_actuals'

-- COMMAND ----------

select * from kgsonedatadb.trusted_hist_impact_idne_gender_target_vs_actuals

-- COMMAND ----------

-- update  kgsonedatadb.raw_hist_csr_fund_recevied_in_csr_account
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_indirect_exp_kgs
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_indirect_exp_kgsmpl
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_indirect_exp_krc
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_kgspl_gl
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_kgsmpl_gl
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_krcpl_gl
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_kgdcpl_gl
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_fund_recevied_in_csr_account_kgdc
-- set file_date = '20230831' where file_date = '20240831';

 

-- update  kgsonedatadb.raw_hist_csr_indirect_expense_kgdc
-- set file_date = '20230831' where file_date = '20240831';



-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_hist_csr_indirect_exp_kgs

-- COMMAND ----------

-- delete from kgsonedatadb.raw_hist_csr_budget where Year='2024';
-- delete from kgsonedatadb.raw_hist_csr_budget_kgdc  where Year='2024';
-- delete from kgsonedatadb.raw_hist_csr_ngo_fund_budget where Financial_year ='2024';
-- delete from kgsonedatadb.raw_hist_csr_ngo_fund_budget_kgdc  where Financial_year ='2024';
-- delete from kgsonedatadb.raw_hist_csr_ngo_fund_util  where Financial_year ='2024';
-- delete from kgsonedatadb.raw_hist_csr_ngo_fund_util_kgdc  where Financial_year ='2024'

-- COMMAND ----------

-- delete from kgsonedatadb.raw_hist_csr_budget where File_Date = '20240831';
-- delete from kgsonedatadb.raw_hist_csr_budget_kgdc  where File_Date = '20240831';
-- delete from kgsonedatadb.raw_hist_csr_ngo_fund_budget where File_Date = '20240831';
-- delete from kgsonedatadb.raw_hist_csr_ngo_fund_budget_kgdc  where File_Date = '20240831';
-- delete from kgsonedatadb.raw_hist_csr_ngo_fund_util  where File_Date = '20240831';
-- delete from kgsonedatadb.raw_hist_csr_ngo_fund_util_kgdc  where File_Date = '20240831'

-- COMMAND ----------

select distinct File_Date from kgsonedatadb.trusted_hist_jml_us_mover

-- COMMAND ----------

select * from kgsonedatadb.config_dim_level_wise

-- COMMAND ----------

select count(1) from kgsonedatadb.raw_hist_impact_csr_data --39036

select count(1) from kgsonedatadb.raw_curr_impact_csr_data --39036

select count(1) from kgsonedatadb.trusted_stg_impact_csr_data --41756


-- COMMAND ----------

truncate table kgsonedatadb.trusted_hist_impact_csr_data 

-- COMMAND ----------

select * from kgsonedatadb.trusted_ta_post_hire_fte

-- COMMAND ----------

  truncate table kgsonedatadb.trusted_hist_impact_wlb_attrition_data;
  truncate table kgsonedatadb.trusted_hist_impact_sick_wellbeing_data;
  truncate table kgsonedatadb.trusted_hist_impact_accident_within_office_premises;
  truncate table kgsonedatadb.trusted_hist_impact_fatality_within_office_premises;
  truncate table kgsonedatadb.trusted_hist_impact_one_to_one_status_data

-- COMMAND ----------

truncate table kgsonedatadb.raw_hist_impact_office_safety_compliance; 
truncate table kgsonedatadb.raw_hist_impact_gps_wellbeing; 
-- truncate table kgsonedatadb.trusted_hist_impact_one_to_one_status_data; 
-- truncate table kgsonedatadb.trusted_hist_impact_accident_within_office_premises; 
-- truncate table kgsonedatadb.trusted_hist_impact_sick_wellbeing_data; 
-- truncate table kgsonedatadb.trusted_hist_impact_fatality_within_office_premises; 
-- truncate table kgsonedatadb.trusted_hist_impact_wlb_attrition_data; 


-- COMMAND ----------

select * from kgsonedatadb_badrecords.trusted_hist_it_talent_connect_daily_transfer_report_bad where File_Date ='20230926' and EMPLOYEE_NUMBER like '%81095%'


-- COMMAND ----------

select * from kgsonedatadb.trusted_it_talent_connect_daily_transfer_report where EMPLOYEE_NUMBER ='81095'

-- COMMAND ----------

select * from kgsonedatadb.raw_hist_it_talent_connect_daily_transfer_report where EMPLOYEE_NUMBER ='108157' and File_Date ='20230926'

-- COMMAND ----------


use kgsonedatadb_badrecords;

show tables

-- COMMAND ----------


select * from kgsonedatadb.config_data_type_cast where Delta_Table_Name like'%air%'

-- COMMAND ----------


--select File_Date, Dated_On,count(1) from  kgsonedatadb.raw_hist_jml_nl_mover group by File_Date,Dated_On 
--select File_Date, Dated_On,count(1) from  kgsonedatadb.raw_hist_jml_nl_leaver group by File_Date,Dated_On ;
select File_Date, Dated_On,count(1) from  kgsonedatadb.raw_hist_jml_nl_joiner group by File_Date,Dated_On ;

-- COMMAND ----------

select File_Date, Dated_On,count(1) from  kgsonedatadb.raw_hist_jml_us_mover group by File_Date,Dated_On 
--kgsonedatadb.trusted_hist_headcount_sabbatical

--select * from   kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date is null

--delete from kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date is null

-- COMMAND ----------


select distinct File_Date from kgsonedatadb.trusted_hist_jml_us_mover

-- COMMAND ----------


select * from kgsonedatadb.config_cc_bu_sl where 
--Dated_On = (select max(Dated_On) from kgsonedatadb.config_cc_bu_sl) and 
File_Date ='20230831'

-- COMMAND ----------

SELECT distinct FILE_DATE

  FROM kgsonedatadb.trusted_hist_it_talent_connect_daily_exit_report order by FILE_DATE desc

-- COMMAND ----------

select * from kgsonedatadb_badrecords.it_talent_connect_daily_transfer_report_bad where File_Date = '20230818' order by Dated_On desc

-- COMMAND ----------

select * from kgsonedatadb.trusted_it_talent_connect_daily_transfer_report where Employee_Number in ('108157',
'109071',
'109564',
'115861',
'130702',
'130765',
'30378',
'36818',
'40400',
'50110',
'79046',
'81095',
'86406'
) and FILE_DATE = '20230818'

-- COMMAND ----------

select count(1)from kgsonedatadb.raw_hist_it_talent_connect_daily_transfer_report where FILE_DATE = '20230818'

-- COMMAND ----------

select count(1) from kgsonedatadb.trusted_hist_it_talent_connect_daily_transfer_report where FILE_DATE = '20230818'

-- COMMAND ----------

select * from kgsonedatadb_badrecords.trusted_hist_it_talent_connect_daily_transfer_report_bad where FILE_DATE = '20230818' 
and EMPLOYEE_E_MAIL in ('abhishekmehta3@kpmg.com',
'sachinsaxena@kpmg.com',
'karnikapradeep@kpmg.com',

'priyajim@kpmg.com',

'nileshahir@kpmg.com',

'soorajnair1@kpmg.com',

'muhammeda@kpmg.com',

'rkjampugumpula@kpmg.com',

'nishads@kpmg.com',

'amaldas1@kpmg.com',

'ksrimannarayana1@kpmg.com',

'subhamghosh1@kpmg.com',

'shridac1@kpmg.com'

)

-- COMMAND ----------

select distinct EMPLOYEE_NUMBER,EMPLOYEE_NAME,File_Date from kgsonedatadb.raw_hist_it_talent_connect_daily_transfer_report where Employee_Number in ('108157',
'109071',
'109564',
'115861',
'130702',
'130765',
'30378',
'36818',
'40400',
'50110',
'79046',
'81095',
'86406'
) and APPROVAL_STATUS ='PENDING'

--and FILE_DATE = '20230818'

-- COMMAND ----------

