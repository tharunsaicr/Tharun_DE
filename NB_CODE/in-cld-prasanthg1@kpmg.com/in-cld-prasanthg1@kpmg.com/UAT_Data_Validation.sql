-- Databricks notebook source

-- select count(1) from kgsonedatadb.raw_hist_lnd_glms_details where File_Date >= '20220930';
-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_details where File_Date >= '20220930';
-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'GLMS' and File_Date >= '20220930';

-- select count(1) from kgsonedatadb.raw_hist_lnd_kva_details where File_Date > '20230531';
-- select count(1) from kgsonedatadb.trusted_hist_lnd_kva_details where File_Date > '20230531';
-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'KVA' and File_Date > '20230531';

-- select File_Date, File_Type from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by File_Date,File_Type

-- select * from kgsonedatadb.config_convert_column_to_date where ProcessName = 'lnd';

-- select * from kgsonedatadb.config_data_type_cast where Process_Name = 'lnd';

-- delete from kgsonedatadb.raw_hist_lnd_glms_details where File_Date >= '20220930';
-- delete from kgsonedatadb.trusted_hist_lnd_glms_details where File_Date >= '20220930';
-- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'GLMS' and File_Date >= '20220930';
-- delete from kgsonedatadb_badrecords.trusted_hist_lnd_glms_details_bad;

-- delete from kgsonedatadb.raw_hist_lnd_kva_details where File_Date = '20230630';
-- delete from kgsonedatadb.trusted_hist_lnd_kva_details where File_Date = '20230630';


-- insert into kgsonedatadb.config_data_type_cast values('lnd','glms_details','Item_Revision_Date','date',null,null,null,'20230809',getdate());
-- insert into kgsonedatadb.config_data_type_cast values('lnd','glms_details','Completion_Date','date',null,null,null,'20230809',getdate());
-- insert into kgsonedatadb.config_data_type_cast values('lnd','kva_details','Date_Added','date',null,null,null,'20230809',getdate());
-- insert into kgsonedatadb.config_data_type_cast values('lnd','kva_details','Completion_Date','date',null,null,null,'20230809',getdate());
-- insert into kgsonedatadb.config_data_type_cast values('lnd','glms_kva_details','CPD_CPE_Hours_Awarded','double',null,null,null,'20230809',getdate());

-- delete from kgsonedatadb.config_data_type_cast where Process_Name = 'lnd' and Column_Name in ('Item_Revision_Date','Completion_Date','Date_Added')

-- select * from kgsonedatadb.config_convert_column_to_date where ProcessName = 'lnd'

-- select distinct coalesce(Instructor,Primary_Instructor) as Primary_Instructor,Instructor from kgsonedatadb.trusted_hist_lnd_glms_details where File_Date = '20230530'

-- select * from kgsonedatadb.raw_curr_lnd_glms_details where Local_HR_ID = '36574'

-- select * from kgsonedatadb.config_hist_cc_bu_sl where Cost_centre = 'DA Core-TVP' and Client_Geography = 'UK' and BU = 'DAS' order by File_Date desc,Cost_centre,Client_Geography,BU,Service_Line
--and File_Date = '20230609'

-- select file_date,* from kgsonedatadb.config_cc_bu_sl where Cost_centre in ('Capability Hubs-KM-GDN People Enablement','Audit-FS-Insurance 3','Audit-FS-Insurance 2','Tax-GCMS','Audit-FS-Banking 3','FASTech Data Engineering','Capability Hubs-Research-Risk Management','MC-GDN-Finance','RC-RA-GDN-GRC-Oracle','GDC2-DA Core-Modeling KART','Audit-FS-Banking 4','Audit-FS-Banking 2','MC-GDN-Central-Support','FASTech Data Visualization','FAS Benchmarking','Audit-FS-Investment Management 2','RC-RA-GDN-Technology Risk Management','MC-EWT-Security-KBS','DA Core – Infra S&O','Audit-FS-Private Equity','MC-GDN-Hub-Management','DA Core-ESG & Infra MGT',' ','GDC - GTS ATO Engineering Support Services','Audit-FS-Real Estate','Capability Hubs-AMS-GAMG','MC-ADE-Future Strategy & Roadmap','Audit-FS-Regions 2','KBS-DLMS','MC-Hyperion FT-BI','DA Core Securitization – Tech','MC-GDN-Process Automation','MC-ASE-ERS Core','DA Core-Modeling-Mgmt','MC-OneData-QE','Capability Hubs-Marketing-G&S-Tax support','Capability Hubs-KM-PPC IDE Support')

-- select distinct Employee_Number from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('137041','137067','137102','137092','137048','137070','137039','137019','137060','137084','137059','137049','137037','137017','137057','137065','137072','137015','137096','137100','137046','137079','137064','137071','137061','137091','137103','137026','137047','137077','137036','137101','130573','137056','137107','137024','137076','137073','137097','137051','137042','137075','137094','137063','137058','137081','137080','137016','137089','137055','137040','137086') and File_Date = '20230626' 
-- union

-- select Employee_Number from kgsonedatadb.trusted_hist_headcount_termination_dump where Employee_Number in ('137041','137067','137102','137092','137048','137070','137039','137019','137060','137084','137059','137049','137037','137017','137057','137065','137072','137015','137096','137100','137046','137079','137064','137071','137061','137091','137103','137026','137047','137077','137036','137101','130573','137056','137107','137024','137076','137073','137097','137051','137042','137075','137094','137063','137058','137081','137080','137016','137089','137055','137040','137086') and File_Date = '20230626'

-- select * from kgsonedatadb.config_data_type_cast where process_name = 'lnd' and Delta_Table_Name = 'glms_kva_details'
-- delete from kgsonedatadb.config_data_type_cast where process_name = 'lnd' and Delta_Table_Name = 'glms_kva_details' and Column_Name in ('Total_Hours','Credit_Hours','Scheduled_Offering_Start_Date','Contact_Hours','CPD_CPE_Hours__Item_','Last_Update_Timestamp')
-- update kgsonedatadb.config_data_type_cast set Column_Name = 'Completion_Date' where process_name = 'lnd' and Delta_Table_Name = 'glms_kva_details' and Column_Name = 'Class_End_Date'

-- select left(Completion_Date,7) as Month_Year,BU,sum(CPD_CPE_Hours_Awarded),sum(CPD_CPE_Hours_Awarded)/count(distinct Local_HR_ID),count(distinct A.Local_HR_ID) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where Completion_Date > '2022-09-30' and Completion_Date<'2023-08-31' group by left(Completion_Date,7),BU order by left(Completion_Date,7) desc,A.BU

-- select BU,sum(CPD_CPE_Hours_Awarded),sum(CPD_CPE_Hours_Awarded)/count(distinct Local_HR_ID),count(distinct A.Local_HR_ID) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where Completion_Date > '2022-09-30' and Completion_Date<'2023-08-31' group by BU order by A.BU

-- select left(Completion_Date,7),count(distinct A.Local_HR_ID) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where Completion_Date > '2022-09-30' and Completion_Date<'2023-07-01' group by left(Completion_Date,7)

-- select Local_HR_ID,File_Date,count(1) from (select distinct BU, Local_HR_ID,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and Completion_Date<'2023-07-01') A group by Local_HR_ID,File_Date having count(1)>1

-- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and Completion_Date<'2023-08-01'
-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and Completion_Date<'2023-08-01'
-- select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and Completion_Date<'2023-08-01'

-- select BU,sum(CPD_CPE_Hours_Awarded),count(distinct Local_HR_ID) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' group by BU

-- select distinct Local_HR_ID from kgsonedatadb.trusted_hist_lnd_glms_kva_details where concat(Local_HR_ID,file_date) in 
-- (select concat(Local_HR_ID,A.File_Date) from (select distinct BU,Local_HR_ID,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30' and Completion_Date<'2023-07-01') A group by Local_HR_ID,A.File_Date having count(1)>1)

-- select BU,sum(CPD_CPE_Hours_Awarded),sum(CPD_CPE_Hours_Awarded)/count(distinct Local_HR_ID),count(distinct Local_HR_ID) from (select distinct Local_HR_ID,Item_Type,Item_Type_Category,Item_Title,Completion_Date,BU,CPD_CPE_Hours_Awarded from kgsonedatadb.trusted_hist_lnd_glms_kva_details) where Completion_Date > '2022-09-30' group by BU

-- select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb_badrecords.trusted_hist_lnd_glms_kva_details_bad where Completion_Date > '2022-09-30'

-- select Local_HR_ID,File_Date,count(1) from (select distinct Local_HR_ID,BU,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date > '2022-09-30') group by Local_HR_ID,File_Date having count(1)>1

-- select BU,* from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date = '20230228' and Local_HR_ID ='76749'

-- select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date>'20220930'

-- select Last_Update_Timestamp,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30' and Completion_Date<'2022-11-01' and file_type = 'GLMS' group by Last_Update_Timestamp order by to_date(Last_Update_Timestamp,'d-MMM-yy') desc


-- select local_hr_id,count(local_hr_id) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30' and Completion_Date<'2022-11-01' and file_type = 'GLMS' group by local_hr_id

-- select Local_HR_ID,count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30' and Completion_Date<'2023-08-31' group by Local_HR_ID order by Local_HR_ID

-- select Local_HR_ID,left(Completion_Date,7),count(1),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30' and Completion_Date<'2023-08-01' group by Local_HR_ID,left(Completion_Date,7) order by Local_HR_ID,left(Completion_Date,7)

-- select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30' and Completion_Date<'2022-11-01' and local_hr_id in ('97325','118723','120983','123028','124485','127338','68729','90343','95523','111663','113898','115001','119751','122885') and file_type = 'GLMS'

-- select distinct len(replace(Completion_Date,"-","")) as File_Date, len(left(replace(Completion_Date,"-",""),6)) as Month_Key from kgsonedatadb.trusted_hist_lnd_kva_details where File_Date>'20220930'

-- select left(Completion_Date,7),sum(CPE_Hours) from kgsonedatadb.trusted_hist_lnd_kva_details group by left(Completion_Date,7)
-- select DISTINCT Remarks from kgsonedatadb_badrecords.trusted_hist_lnd_glms_details_bad where file_date = '20221119' and Remarks = 'Employee not present in both the Dumps'

-- select Completion_Date,Last_Update_Timestamp,count(Local_HR_ID) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_ID = '97812' and Completion_Date>'2022-09-30' group by Completion_Date,Last_Update_Timestamp

-- select * from kgsonedatadb.raw_hist_lnd_glms_details where Local_HR_ID = '39296';

--raw glms
--53 
--trusted glms
-- 36

-- raw kva
--292
-- trusted kva
--255

-- select * from kgsonedatadb.raw_hist_lnd_glms_details where Local_HR_ID = '97754' and Completion_Date like '%Jun%' order by item_title; --87
-- select Item_Title,CPD_CPE_Hours_Awarded,count(1),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.raw_hist_lnd_glms_details where Local_HR_ID = '97754' and Completion_Date like '%Jun%' group by Item_Title,CPD_CPE_Hours_Awarded having count(1)>1;

-- select * from kgsonedatadb.trusted_hist_lnd_glms_details where Local_HR_ID = '97754' and Completion_Date like '%-06-%'; --85

-- select * from kgsonedatadb.raw_hist_lnd_kva_details where employee_id = '97754' and Completion_Date like '%-06-%'; --43
-- select * from kgsonedatadb.trusted_hist_lnd_kva_details where employee_id = '97754' and Completion_Date like '%-06-%'; --36

-- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_ID = '97754' and Completion_Date like '%-06-%'; -- 121

-- select * from kgsonedatadb.config_hist_cc_bu_sl where Cost_centre = 'MC-Service Now' and File_Date<'2023-06-01' and File_Date > '2023-04-30';

-- select * from kgsonedatadb.trusted_hist_bgv_offer_release order by offered_created_Date asc;

-- select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.test_trusted_hist_lnd_glms_kva_details;

-- select left(resignationdate,4),count(1) from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report where COMPANY_NAME IN ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited') and upper(requeststatus) in ('PENDING','WITHDRAWAL PENDING WITH PM','TERMINATED','APPROVED','WITHDRAWAL REJECTED','APPROVED BY PM','APPROVED BY PARTNER','LWD APPROVED - RESIGNATION ON BEHALF','PENDING(REASSIGNED)','PENDING FOR APPROVAL FROM PARTNER','PENDING FOR APPROVAL FROM PM') group by left(resignationdate,4) order by left(resignationdate,4) desc;

-- select count(1) from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report;
-- --12262

-- select resignationdate,* from (select row_number() over(partition by employeenumber order by resignationdate desc) as tk_rownum,* from kgsonedatadb.raw_curr_headcount_talent_konnect_resignation_status_report) where tk_rownum >1 order by resignationdate desc;
-- --12248

-- select count(1) from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report;
-- --12248

-- select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report where COMPANY_NAME IN ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited') and coalesce(resignationdate,"NA") != "NA" group by file_date;
-- --

-- select count(1) from kgsonedatadb_badrecords.trusted_hist_headcount_talent_konnect_resignation_status_report_bad;
-- --0

-- select * from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report where EmployeeNumber in ('136456','132825','131995','129063','127719','127584','125083','123401','120140','119735','30501','65509','116681','115953','110801','105361','104794','91146','86459','85305','81446','73265','53264')

-- select count(distinct Cost_centre) from kgsonedatadb.config_hist_cc_bu_sl where cost_centre in (select distinct cost_centre from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where BU is null)
--338

-- select distinct Cost_centre,Month_Key from (
-- select Cost_centre,Client_Geography,BU,Month_Key,count(1) from kgsonedatadb.config_hist_cc_bu_sl_master group by Cost_centre,Client_Geography,BU,Month_Key having count(1)>1);

-- select distinct Cost_centre,Client_Geography,BU,Service_Line,Service_Network,File_Date,Dated_On,Month_Key,Final_BU from kgsonedatadb.config_cc_bu_sl_master;

-- select distinct Cost_centre from kgsonedatadb.config_hist_cc_bu_sl
-- except
-- select distinct Cost_centre from kgsonedatadb.config_hist_cc_bu_sl_master
-- except
-- select distinct Cost_centre from kgsonedatadb.config_hist_cc_bu_sl

-- select *from kgsonedatadb.config_bu_mapping_list order by Final_BU

select File_Date,count(1) from kgsonedatadb.raw_hist_talent_acquisition_requisition_raised group by File_Date order by File_Date desc; --1437
select File_Date,count(1) from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump group by File_Date order by File_Date desc; --1437
select * from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump where file_date = '20230920'

-- COMMAND ----------

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.raw_hist_lnd_glms_details where Local_HR_ID = '39296' and Completion_Date like '%Oct-22%';--and Completion_Date <'20221101'
--27

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Local_HR_ID = '39296' and completion_Date > '2023-07-31' and completion_Date < '2023-09-01';
--13.53

select * from kgsonedatadb.raw_hist_lnd_kva_details where Employee_Id = '39296' and completion_Date > '2022-09-30' and completion_Date < '2022-11-01';
--6 count

select sum(CPE_Hours) from kgsonedatadb.trusted_hist_lnd_kva_details where Employee_Id = '39296' and completion_Date > '2022-09-30' and completion_Date < '2022-11-01';
--5

select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details;

select File_Type,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_Date > '2022-09-30' and ((File_Type = 'GLMS' and Item_Title not like 'KVA%') or (File_Type = 'KVA')) group by File_Type;

select File_Date,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_Date > '2022-09-30' and completion_Date < '2023-09-01' and ((File_Type = 'GLMS' and Item_Title not like 'KVA%') or (File_Type = 'KVA') or (File_Type  is null)) group by File_Date order by File_Date desc;

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_Date > '2022-09-30' and completion_Date < '2023-09-01' group by left(Completion_Date,7);

select left(A.Completion_Date,7),count(1),sum(A.CPD_CPE_Hours_Awarded) from (
select Local_HR_ID,Item_Title,CPD_CPE_Hours_Awarded,Completion_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_Date > '2022-09-30' and completion_Date < '2023-09-01' and CPD_CPE_Hours_Awarded <> 0 and Item_Title <> 'Focus Learning Hours - WBT' group by Local_HR_ID,Item_Title,CPD_CPE_Hours_Awarded,Completion_Date) A group by left(A.Completion_Date,7);

select Dated_On,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by Dated_On;

select 1384683.9403311536+7103.96+74347.91 --onedata --1466135.81
select 1387674.52 + 7104.2+74347.91 --lnd --1469126.63
select File_Type,dated_on,sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_Date > '2023-07-31' and completion_Date < '2023-09-01' and ((File_Type = 'GLMS' and Item_Title not like 'KVA%');

delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Dated_On >='2023-08-17 16:47:44.000' and Dated_On <= '2023-09-14 18:44:33.000';

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_date > '2022-09-30';

-- COMMAND ----------

-- DBTITLE 0,Remove 1 record from ResignedLeft and add to EmployeeDetails as per HRSS request
-- insert into kgsonedatadb.trusted_hist_headcount_monthly_employee_details(Employee_Number,Full_Name,`Function`,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,`Location`,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,Remarks,Current_Base_Location_of_the_Candidate,Office_Location_in_the_KGS_Offer_Letter,WFA_Option__Permanent_12_Months_,File_Date,File_Month,File_Year,Dated_On,Requested_By) values ('112768','Hafiz, Mohammed Ibrahim','Advisory','MC','MC','KGS Non SEZ Bangalore Unit.US','MC-Source-Aris','KGS NON SEZ BANGALORE UNIT','DU','US','Bangalore','Bangalore Two','Associate Consultant','085.Executive','Client Service Staff','Confirmed Staff','2022-02-28','','M','KPMG Global Services Private Limited','Chatra Anand, Sagar','Chatra Anand, Sagar','mhafiz2@kpmg.com','Consulting',NULL,NULL,NULL,NULL,'20230818','August','2023','2023-08-28 18:59:00.000','');
-- select * from kgsonedatadb.trusted_hist_headcount_employee_details
-- insert into kgsonedatadb.trusted_hist_headcount_employee_details(Employee_Number,Full_Name,`Function`,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,Remarks,Current_Base_Location_of_the_Candidate,Office_Location_in_the_KGS_Offer_Letter,WFA_Option__Permanent_12_Months_,Dated_On,File_Date,Business_Category,Requested_By) values ('112768','Hafiz, Mohammed Ibrahim','Advisory','MC','MC','KGS Non SEZ Bangalore Unit.US','MC-Source-Aris','KGS NON SEZ BANGALORE UNIT','DU','US','Bangalore','Bangalore Two','Associate Consultant','085.Executive','Client Service Staff','Confirmed Staff','2022-02-28','','M','KPMG Global Services Private Limited','Chatra Anand, Sagar','Chatra Anand, Sagar','mhafiz2@kpmg.com','Consulting',NULL,NULL,NULL,NULL,'2023-08-28 18:59:00.000','20230818','','');

-- delete from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where Employee_Number = '112768';
-- delete from kgsonedatadb.trusted_hist_headcount_resigned_and_left where Employee_Number = '112768';

select distinct File_Date from kgsonedatadb.trusted_hist_headcount_employee_details order by File_Date desc;

select distinct dated_on from kgsonedatadb.raw_stg_headcount_employee_dump where File_Date = '20230904'
select distinct dated_on from kgsonedatadb.test_raw_stg_headcount_employee_dump where File_Date = '20230904'
--43115
--43343

-- COMMAND ----------

-- DBTITLE 1,Delete and Rerun KVA data FY23
-- delete from kgsonedatadb.raw_hist_lnd_kva_details where File_Date >= '20230904'; --73294
-- delete from kgsonedatadb.trusted_hist_lnd_kva_details where File_Date >= '20230904'; --8925
-- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'KVA' and File_Date >= '20230904'; --8925
-- delete from kgsonedatadb_badrecords.trusted_hist_lnd_kva_details_bad where File_Date >= '20230904'; --0
-- delete from kgsonedatadb_badrecords.trusted_hist_lnd_glms_kva_details_bad where File_Type = 'KVA' and File_Date >= '20230904'; --0

-- delete from kgsonedatadb.raw_hist_lnd_glms_details where File_Date >= '20230904'; --129902
-- delete from kgsonedatadb.trusted_hist_lnd_glms_details where File_Date >= '20230904'; --82918
-- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'GLMS' and File_Date >= '20230904'; --82918
-- delete from kgsonedatadb_badrecords.trusted_hist_lnd_glms_details_bad where File_Date >= '20230904'; --46
-- delete from kgsonedatadb_badrecords.trusted_hist_lnd_glms_kva_details_bad where File_Type = 'GLMS' and File_Date >= '20230904'; --0

-- select min(Completion_Date),max(Completion_Date) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Type = 'KVA'

select left(Completion_Date,7) as Month_Year,BU,sum(CPD_CPE_Hours_Awarded),sum(CPD_CPE_Hours_Awarded)/count(distinct Local_HR_ID),count(distinct A.Local_HR_ID) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where Completion_Date > '2022-09-30' and Completion_Date<'2023-07-01' and A.File_Type = 'KVA' group by left(Completion_Date,7),BU order by left(Completion_Date,7) desc,A.BU

select left(completion_date,7),sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where Completion_Date > '2022-09-30' and Completion_Date<'2023-07-01' and A.File_Type = 'KVA' group by left(completion_date,7)

select sum(CPD_CPE_Hours_Awarded) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where Completion_Date > '2022-09-30' and Completion_Date<'2023-07-01' and A.File_Type = 'KVA'

select left(Completion_Date,7),count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where Completion_Date > '2022-09-30' and Completion_Date<'2023-07-01' group by left(Completion_Date,7)

select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A where File_Date <= '2022-09-30'

select left(Completion_Date,7) as Month_Year,BU,sum(CPE_Hours),sum(CPE_Hours)/count(distinct Employee_ID),count(distinct A.Employee_ID) from kgsonedatadb.trusted_hist_lnd_kva_details A where Completion_Date > '2022-09-30' and Completion_Date<'2023-07-01' and A.File_Type = 'KVA' and left(Completion_Date,7)=left(Date_Added,7) group by left(Completion_Date,7),BU order by left(Completion_Date,7) desc,A.BU

-- COMMAND ----------

-- DBTITLE 1,Update query to fix LND data
-- select distinct A.BU,B.BU from (select Cost_Center,BU,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where BU is null and File_Date>'20220930') A left outer join kgsonedatadb.config_cc_bu_sl B on A.Cost_Center=B.Cost_centre and left(A.File_Date,4)=left(B.File_Date,4) where coalesce(B.BU,'NA') !='NA'

-- MERGE INTO kgsonedatadb.trusted_hist_lnd_glms_kva_details as target
-- USING kgsonedatadb.config_cost_center_business_unit as source 
-- ON target.Cost_Center = source.Cost_centre and target.BU is null
-- WHEN MATCHED THEN 
-- UPDATE SET target.BU = source.BU

-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details A left outer join kgsonedatadb.config_cost_center_business_unit B on A.Cost_Center = B.Cost_centre where A.File_Date>'20220930' and A.BU is null and coalesce(B.BU,'NA')!='NA'
-- A.BU as LND_BU,A.Cost_Center as LND_CC,A.File_Date as LND_File_Date,B.Cost_centre,B.BU,B.File_Date

-- create table kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_202300905 as
-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details
-- select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_202300905
-- 3061580

select file_date,Dated_On,count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30' group by file_date,Dated_On order by file_date desc,Dated_On desc
-- 972920
-- delete from kgsonedatadb.raw_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30' and file_date in ('20230501','20230601','20230701')
-- 2088660

select count(period) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where Completion_Date>'2022-09-30'
--delta - 936846
--file - 936846
--sql - 936846

-- COMMAND ----------

-- DBTITLE 1,To fix Finance Metrics discrepancy
select * from (select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Position in ("Director","Director GDC","Director-CFO-GDC","Technical Director","Associate Partner","Associate Partner - Finance","Associate Partner-KGSnGDC","Associate Partner - COO") and File_Date between '20210930' and '20220601') A left outer join kgsonedatadb.config_cost_center_business_unit B on A.Cost_centre=B.Cost_centre where A.BU is null;

-- select count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Position in ("Director","Director GDC","Director-CFO-GDC","Technical Director","Associate Partner","Associate Partner - Finance","Associate Partner-KGSnGDC","Associate Partner - COO") and File_Date between '20210930' and '20220601' and BU is null

-- select BU,File_Date,count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where File_Date>'20220531' group by BU,File_Date

select distinct left(File_Date,6) from kgsonedatadb.trusted_hist_headcount_monthly_resign where File_Date < '20220601' and BU is null except select distinct left(File_Date,6) from kgsonedatadb.config_hist_cost_center_business_unit where File_Date < '20220601';

select count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where File_Date < '20220601' and BU is null;
--385317

select count(1) from (select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details A where File_Date < '20220601' and BU is null) A inner join (select * from kgsonedatadb.config_hist_cost_center_business_unit where File_Date < '20220601')B on A.Cost_centre=B.Cost_centre and left(A.File_Date,6) = left(B.File_Date,6) where B.BU is null;

select distinct Cost_centre,replace(Cost_centre,"–",'-') from kgsonedatadb.config_hist_cost_center_business_unit --where Cost_centre like '%DA Core%ADO%'
where Cost_centre like '%–%' and File_Date < '20220601';

-- update kgsonedatadb.config_hist_cost_center_business_unit set Cost_centre = replace(Cost_centre,"–",'-') where Cost_centre like '%–%' and File_Date < '20220601'
--after join 

-- MERGE INTO kgsonedatadb.trusted_hist_headcount_monthly_employee_details as target
-- USING kgsonedatadb.config_hist_cost_center_business_unit as source 
-- ON target.Cost_centre = source.Cost_centre and left(target.File_Date,6) = left(source.File_Date,6) and target.BU is null and target.File_Date < '20220601' and source.File_Date < '20220601'
-- WHEN MATCHED THEN 
-- UPDATE SET target.BU = source.BU

--affected rows after update - 384803

--after BU update --514
select distinct Cost_centre,BU from kgsonedatadb.config_hist_cost_center_business_unit where cost_centre in (
select distinct Cost_centre from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where File_Date < '20220601' and BU is null 
and Position in ("Director","Director GDC","Director-CFO-GDC","Technical Director","Associate Partner","Associate Partner - Finance","Associate Partner-KGSnGDC","Associate Partner - COO")
)


select distinct Cost_centre from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where File_Date < '20220601' and BU is null

DA Core – Management SSG-ISA-IVD --DAS
DA Core - Management SSG-ISA-IVD --DAS
GDCAdv-RC-RA-Technology Assurance --RS
Nexus-Infrastructure Mgmt --Digital Nexus
DA Core - Deal Analytics --DAS

-- update kgsonedatadb.trusted_hist_headcount_monthly_employee_details set BU = 'DAS' where File_Date < '20220601' and BU is null and Cost_centre like '%DA%Core%Deal%Analytics%'

--after BU manual update -0

--Resigned and left
--null BU - 8027 before update

-- MERGE INTO kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left as target
-- USING kgsonedatadb.config_hist_cost_center_business_unit as source 
-- ON target.Cost_centre = source.Cost_centre and left(target.File_Date,6) = left(source.File_Date,6) and target.BU is null and target.File_Date < '20220601' and source.File_Date < '20220601'
-- WHEN MATCHED THEN 
-- UPDATE SET target.BU = source.BU

--affected after BU update - 7913

-- update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Advisory' where File_Date < '20220601' and BU is null and Cost_centre = 'KGS-Adv-Ops-Dedicated Support'; --1
-- update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='CF' where File_Date < '20220601' and BU is null and Cost_centre = 'CF-HR Central C&B'; --1
-- update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='CF' where File_Date < '20220601' and BU is null and Cost_centre = 'KBS-KICS Support';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='CF' where File_Date < '20220601' and BU is null and Cost_centre = 'CF-HR REC MS';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='CF' where File_Date < '20220601' and BU is null and Cost_centre = 'CF-HR REC DN';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='CF' where File_Date < '20220601' and BU is null and Cost_centre = 'CF-HR L&D RC';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='CF' where File_Date < '20220601' and BU is null and Cost_centre = 'CF-HR REC Campus';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'RAK-GSM-Research';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-Research-REG SGI';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-KM-Digital Marketing Publishing';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-MS-Creative Prop';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-Pursuits-SN';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-KM-GCM-Relationship Management';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-Benchmarking-Consulting-PBS';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-AMS-CSMS';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-Benchmarking-Consulting-FT';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-MS-RC-Mar-Comm';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Cap-Hubs' where File_Date < '20220601' and BU is null and Cost_centre = 'Capability Hubs-Research-GCM Platinum Support';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MS-Microsoft-POD';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-EWT-SIAM IT Support';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-CS-Procurement';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'GPTS - RTEC';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-Business Services & Outsourcing Adv';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'GPTS -  IPG';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-IT Advisory-NLBI';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-IT Advisory-EIM';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-GDN-D&A';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-KDN-GCMS';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-TE-Cyber-Transformation';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'TE-ES-Workday';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-GDN-DataScience';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'SGI-D&A';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'C&O-Customer Solutions';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-Management-TA&Others';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'C&O-Product Operations & Procurement';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-Powered Maintenance-CIOA';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-PMO Pooling IES';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Consulting' where File_Date < '20220601' and BU is null and Cost_centre = 'MC-GDN-Microsoft';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='DAS' where File_Date < '20220601' and BU is null and Cost_centre = 'DA Core-SS-FS';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='DAS' where File_Date < '20220601' and BU is null and Cost_centre = 'DA Core – ADO';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='DAS' where File_Date < '20220601' and BU is null and Cost_centre = 'DA Core-TS-FDD Management';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='DAS' where File_Date < '20220601' and BU is null and Cost_centre = 'DA Core-Infra DATA';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='DAS' where File_Date < '20220601' and BU is null and Cost_centre = 'CH –KBS – Modelling People Analytics';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Digital Nexus' where File_Date < '20220601' and BU is null and Cost_centre = 'Nexus-Cloud COE & Platform';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='KRC' where File_Date < '20220601' and BU is null and Cost_centre = 'Audit-Payroll';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='KRC' where File_Date < '20220601' and BU is null and Cost_centre = 'Audit-FS-Regions';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='MS' where File_Date < '20220601' and BU is null and Cost_centre = 'MS-Liberty';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='MS' where File_Date < '20220601' and BU is null and Cost_centre = 'MS-Astrus';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='MS' where File_Date < '20220601' and BU is null and Cost_centre = 'MS-FS Fin Services-AML&KYC';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='MS' where File_Date < '20220601' and BU is null and Cost_centre = 'MS-FS Fin Services-CDD';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='MS' where File_Date < '20220601' and BU is null and Cost_centre = 'MS-FS-Liberty';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'RC-RA-Technology Assurance';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'RC-Secondments';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'GDCAdv-RC-RS&C-Risk Analytics';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'GDCAdv-RC-RA-Internal Audit & Enterprise Risk';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'RC-FORENSIC-ASTRUS';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'RC-RA-Technology Risk Management (KPE)';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'RC-RS&C-Spectrum';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'RC-RS&C-Operations & Compliance Risk';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'RC-TR-Cyber';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'RC-RA-Securitization';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='RS' where File_Date < '20220601' and BU is null and Cost_centre = 'GDCAdv-RC-RA-Technology Assurance';update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Tax' where File_Date < '20220601' and BU is null and Cost_centre = 'Tax & Dedicated Ops Mgmt';
--update kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left set BU='Digital Nexus' where File_Date < '20220601' and BU is null and Cost_centre = '%Nexus%Infrastructure%Mgmt%';


--affected after manual BU update

select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_Date is null and file_Date > '20221001'
--2088411

select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where file_Date < '20221001'
--2088411

select distinct Period from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_Date is null and file_Date < '20221001'


-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details group by File_Date;
20230818 21681
20230719 21680

select File_Date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_details group by File_Date;
-- 20230904 21639
-- 20230831 21648
-- 20230828 21654
-- 20230821 21657
-- 20230818 21681
-- 20230814 21684

select file_date,count(1) from kgsonedatadb_badrecords.trusted_hist_talent_acquisition_kgs_joiners_report_bad group by File_Date;

select File_Date,count(1) from kgsonedatadb_badrecords.trusted_hist_talent_acquisition_requisition_dump_bad group by File_Date;

select File_Date,count(1) from kgsonedatadb_badrecords.trusted_hist_talent_acquisition_requisition_raised_bad group by File_Date;

-- COMMAND ----------

-- select count(distinct Employee_Number) from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('137041','137067','137102','137092','137048','137070','137039','137019','137060','137084','137059','137049','137037','137017','137057','137065','137072','137015','137096','137100','137046','137079','137064','137071','137061','137091','137103','137026','137047','137077','137036','137101','130573','137056','137107','137024','137076','137073','137097','137051','137042','137075','137094','137063','137058','137081','137080','137016','137089','137055','137040','137086') and file_date <= '20230710' 
-- order by Employee_Number,File_Date desc

-- select count(distinct employee_number) from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_employee_dump A where Employee_Number in ('137041','137067','137102','137092','137048','137070','137039','137019','137060','137084','137059','137049','137037','137017','137057','137065','137072','137015','137096','137100','137046','137079','137064','137071','137061','137091','137103','137026','137047','137077','137036','137101','130573','137056','137107','137024','137076','137073','137097','137051','137042','137075','137094','137063','137058','137081','137080','137016','137089','137055','137040','137086') and Entity = 'KGS' and file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_employee_dump B where to_date(B.File_Date,'yyyyMMdd') <= '2023-07-10')) ed_hist where rank = 1

-- select * from (select rank() over(partition by employee_number,file_date order by dated_on desc) as rank, * from kgsonedatadb.trusted_hist_headcount_termination_dump where Employee_Number in ('137041','137067','137102','137092','137048','137070','137039','137019','137060','137084','137059','137049','137037','137017','137057','137065','137072','137015','137096','137100','137046','137079','137064','137071','137061','137091','137103','137026','137047','137077','137036','137101','130573','137056','137107','137024','137076','137073','137097','137051','137042','137075','137094','137063','137058','137081','137080','137016','137089','137055','137040','137086') and  file_date = (select max(file_date) from kgsonedatadb.trusted_hist_headcount_termination_dump where to_date(File_Date,'yyyyMMdd') <= '2023-07-10')) td_hist where rank = 1

-- select Employee_Number,Date_First_Hired,Cost_centre,File_Date from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('133540','137041','126910','117754','87023','52655','122297','72372','133320','113431','133071','117737','136759','113425','131692','131591') and File_Date = '20230626'

-- select count(1) from kgsonedatadb.raw_hist_headcount_monthly_employee_details A where A.File_Date>'2021-09-30' and A.File_Date<'2022-06-30'
--168717
select distinct Employee_Number,Cost_centre,BU,left(File_Date,4) as Year from kgsonedatadb.raw_hist_headcount_monthly_resigned_and_left A where A.File_Date>'2021-09-30' and A.File_Date<'2022-06-30' and A.BU is null
--155793
-- select count(1) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details A where A.File_Date>'2021-09-30' and A.File_Date<'2022-06-30' and A.BU is null
--113

-- select count(1) from kgsonedatadb.raw_hist_headcount_monthly_employee_details A left outer join kgsonedatadb.trusted_hist_headcount_monthly_employee_details B on A.Employee_Number = B.Employee_Number
-- where A.BU <> B.BU and A.File_Date>'2021-09-30' and A.File_Date<'2022-06-30'

-- COMMAND ----------

-- select File_Date,count(1) from kgsonedatadb.config_hist_cc_bu_sl group by File_Date

-- select distinct Cost_centre from kgsonedatadb.config_hist_cc_bu_sl where File_Date = '20230609'

-- select distinct Cost_centre,file_date from kgsonedatadb.config_hist_cc_bu_sl where Cost_centre in ('Capability Hubs-KM-PPC IDE Support','DA Core-Modeling-Mgmt','MC-GDN-Finance','MC-GDN-Oracle EPM','MC-GDN-Process Automation','Nexus-Ingestion & Data Engineering','Tax-HNI','KGS Capability Hubs','Risk Services')

-- select count(1) from (select distinct cost_centre,BU from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where File_Date= '20230517' union select distinct cost_centre,BU from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where File_Date= '20230517')

-- select distinct cost_centre,BU from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Employee_Number = '121849'

-- select * from kgsonedatadb.config_cc_bu_sl where Cost_centre = 'KBS-KICS Support' order by File_Date

select distinct Cost_centre from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where left(File_Date,6)="202204"

-- select distinct Cost_centre from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left where left(File_Date,6)="202204"

-- COMMAND ----------



-- COMMAND ----------



select count(distinct Employee_Number) from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where File_Date = '20230517' and Employee_Category not in ("Secondee-Inward-With Pay","Secondee-Inward-Without Pay","Secondee-Outward-With Pay","Secondee-Outward-Without Pay")

-- insert into kgsonedatadb.trusted_hist_bgv_upcoming_joiners
-- select * from temp_upc_jnts

-- select file_date,dated_on,count(1) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners group by file_date,dated_on
-- select file_date,count(1) from kgsonedatadb.trusted_hist_bgv_kcheck  group by file_date
-- select file_date,count(1) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners group by file_date

-- select File_Date,count(1) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners group by File_Date
--
-- select `Candidate_Email/Candidate_Email_Personal_Email`,count(1) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners group by `Candidate_Email/Candidate_Email_Personal_Email` having count(1)>1

-- select `Candidate_Email/Candidate_Email_Personal_Email`,count(1) from (
-- select Candidate_Identifier,BU,Candidate_Name,`Candidate_Email/Candidate_Email_Personal_Email`,Candidate_Alternate_Email_Candiddate_Official_Email,
-- CASE when (trim(Start_Date) == '' or trim(Start_Date) == '-')  then NULL else Start_Date END as Start_Date,
-- Recruiter_Name,BGV_Reference_No,
-- CASE when (trim(BGV_Initiation_Date) == '' or trim(BGV_Initiation_Date) == '-')  then NULL else BGV_Initiation_Date END as BGV_Initiation_Date,
-- BGV_Status,Report_Colour,Mandatory_Checks_Completed,All_Checks_Completed,Can_be_Onboarded_Yes_or_No,Waiver_Applicable,Waiver_Status_Closed_Yes_or_No,Insuff_Remarks,UTV_Remarks,Discrepancy_remarks,Found_in_Suspicious_List,Work_Location,Cost_Center,Client_Geography,Designation,CRI1,CRI2,CRI3,CRI4,CRI5,DTB_1,DTB_2,DTB_3,DTB_4,File_Date,Dated_On,concat(year(Start_Date),date_format(Start_Date,"MM")) as Month_Key from (select rank() over(partition by `Candidate_Email/Candidate_Email_Personal_Email` order by File_Date desc, dated_on desc) as rank, * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners ) hist where rank = 1) A group by `Candidate_Email/Candidate_Email_Personal_Email` having count(1)>1

-- COMMAND ----------

-- DBTITLE 1,Total Hist count
select 'trusted_hist_bgv_kcheck' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_bgv_kcheck union
select 'trusted_hist_bgv_offer_release' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_bgv_offer_release union
select 'trusted_hist_bgv_progress_sheet' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_bgv_progress_sheet union
select 'trusted_hist_bgv_upcoming_joiners' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_bgv_upcoming_joiners union
select 'trusted_hist_bgv_waiver_tracker' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_bgv_waiver_tracker union
select 'trusted_hist_compensation_additional_pay' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_compensation_additional_pay union
select 'trusted_hist_compensation_finance_metrics' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_compensation_finance_metrics union
select 'trusted_hist_compensation_paysheet' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_compensation_paysheet union
select 'trusted_hist_compensation_yec' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_compensation_yec union
select 'trusted_hist_employee_engagement_encore_output' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_encore_output union
select 'trusted_hist_employee_engagement_gps' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_gps union
select 'trusted_hist_employee_engagement_rock' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_rock union
select 'trusted_hist_employee_engagement_thanks_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump union
select 'trusted_hist_employee_engagement_year_end' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_year_end union
select 'trusted_hist_global_mobility_l1_visa_tracker' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_l1_visa_tracker union
select 'trusted_hist_global_mobility_non_us_tracker' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_non_us_tracker union
select 'trusted_hist_global_mobility_opportunity_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_opportunity_dump union
select 'trusted_hist_global_mobility_secondment_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_secondment_details union
select 'trusted_hist_global_mobility_secondment_tracker' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_secondment_tracker union
select 'trusted_hist_headcount_academic_trainee' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_academic_trainee union
select 'trusted_hist_headcount_contingent' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent union
select 'trusted_hist_headcount_contingent_worker' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker union
select 'trusted_hist_headcount_contingent_worker_resigned' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned union
select 'trusted_hist_headcount_employee_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_employee_details union
select 'trusted_hist_headcount_employee_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_employee_dump union
select 'trusted_hist_headcount_leave_report' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_leave_report union
select 'trusted_hist_headcount_loaned_staff_from_ki' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_from_ki union
select 'trusted_hist_headcount_loaned_staff_resigned' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned union
select 'trusted_hist_headcount_maternity_cases' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_maternity_cases union
select 'trusted_hist_headcount_monthly_academic_trainee' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee union
select 'trusted_hist_headcount_monthly_contingent_worker' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker union
select 'trusted_hist_headcount_monthly_contingent_worker_resigned' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned union
select 'trusted_hist_headcount_monthly_employee_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_employee_details union
select 'trusted_hist_headcount_monthly_loaned_staff_from_ki' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki union
select 'trusted_hist_headcount_monthly_loaned_staff_resigned' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned union
select 'trusted_hist_headcount_monthly_maternity_cases' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases union
select 'trusted_hist_headcount_monthly_resigned_and_left' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left union
select 'trusted_hist_headcount_monthly_sabbatical' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical union
select 'trusted_hist_headcount_monthly_secondee_outward' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward union
select 'trusted_hist_headcount_resigned_and_left' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_resigned_and_left union
select 'trusted_hist_headcount_sabbatical' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_sabbatical union
select 'trusted_hist_headcount_sabbatical_test' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_sabbatical_test union
select 'trusted_hist_headcount_secondee_outward' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_secondee_outward union
select 'trusted_hist_headcount_talent_konnect_resignation_status_report' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report union
select 'trusted_hist_headcount_termination_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_headcount_termination_dump union
select 'trusted_hist_i_and_d_gl_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_i_and_d_gl_dump union
select 'trusted_hist_jml_germany_joiner' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_germany_joiner union
select 'trusted_hist_jml_germany_leaver' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_germany_leaver union
select 'trusted_hist_jml_germany_mover' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_germany_mover union
select 'trusted_hist_jml_nl_joiner' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_nl_joiner union
select 'trusted_hist_jml_nl_leaver' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_nl_leaver union
select 'trusted_hist_jml_nl_mover' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_nl_mover union
select 'trusted_hist_jml_uk_joiner' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_uk_joiner union
select 'trusted_hist_jml_uk_leaver' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_uk_leaver union
select 'trusted_hist_jml_uk_mover' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_uk_mover union
select 'trusted_hist_jml_us_joiner' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_us_joiner union
select 'trusted_hist_jml_us_leaver' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_us_leaver union
select 'trusted_hist_jml_us_mover' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_jml_us_mover union
select 'trusted_hist_lnd_glms_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_lnd_glms_details union
select 'trusted_hist_lnd_glms_kva_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_lnd_glms_kva_details union
select 'trusted_hist_lnd_kva_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_lnd_kva_details union
select 'trusted_hist_talent_acquisition_kgs_joiners_report' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report union
select 'trusted_hist_talent_acquisition_requisition_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump

-- COMMAND ----------

-- DBTITLE 1,Total trusted bad count
select 'trusted_hist_compensation_finance_metrics_bad' as Delta_Table_Name,count(1) as count from kgsonedatadb_badrecords.trusted_hist_compensation_finance_metrics_bad
union select 'trusted_hist_headcount_monthly_contingent_worker_bad' as Delta_Table_Name,count(1) as count from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_bad
union select 'trusted_hist_headcount_monthly_contingent_worker_resigned_bad' as Delta_Table_Name,count(1) as count from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_contingent_worker_resigned_bad
union select 'trusted_hist_headcount_monthly_employee_details_bad' as Delta_Table_Name,count(1) as count from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_employee_details_bad
union select 'trusted_hist_headcount_monthly_loaned_staff_from_ki_bad' as Delta_Table_Name,count(1) as count from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_from_ki_bad
union select 'trusted_hist_headcount_monthly_loaned_staff_resigned_bad' as Delta_Table_Name,count(1) as count from kgsonedatadb_badrecords.trusted_hist_headcount_monthly_loaned_staff_resigned_bad
union select 'trusted_hist_lnd_glms_details_bad' as Delta_Table_Name,count(1) as count from kgsonedatadb_badrecords.trusted_hist_lnd_glms_details_bad
union select 'trusted_hist_lnd_kva_details_bad' as Delta_Table_Name,count(1) as count from kgsonedatadb_badrecords.trusted_hist_lnd_kva_details_bad

-- COMMAND ----------

select 'raw_hist_headcount_employee_dump' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_employee_dump group by File_Date,Dated_on union
select 'raw_hist_headcount_talent_konnect_resignation_status_report' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report group by File_Date,Dated_on union 
select 'raw_hist_headcount_contingent' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_contingent group by File_Date,Dated_on union 
select 'raw_hist_headcount_termination_dump' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_termination_dump group by File_Date,Dated_on union
select 'raw_hist_headcount_leave_report' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_leave_report group by File_Date,Dated_on union 
select 'raw_hist_headcount_employee_details' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_employee_details group by File_Date,Dated_on union 
select 'raw_hist_headcount_academic_trainee' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_academic_trainee group by File_Date,Dated_on union 
select 'raw_hist_headcount_contingent_worker' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_contingent_worker group by File_Date,Dated_on union 
select 'raw_hist_headcount_contingent_worker_resigned' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_contingent_worker_resigned group by File_Date,Dated_on union 
select 'raw_hist_headcount_loaned_staff_resigned' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_loaned_staff_resigned group by File_Date,Dated_on union 
select 'raw_hist_headcount_sabbatical' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_sabbatical group by File_Date,Dated_on union 
select 'raw_hist_headcount_maternity_cases' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_maternity_cases group by File_Date,Dated_on union 
select 'raw_hist_headcount_resigned_and_left' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_resigned_and_left group by File_Date,Dated_on union 
select 'raw_hist_headcount_secondee_outward' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_secondee_outward group by File_Date,Dated_on union 
select 'raw_hist_headcount_loaned_staff_from_ki' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_loaned_staff_from_ki group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_employee_details' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_employee_details group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_academic_trainee' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_academic_trainee group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_contingent_worker' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_contingent_worker_resigned' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker_resigned group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_loaned_staff_resigned' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_resigned group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_sabbatical' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_sabbatical group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_maternity_cases' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_maternity_cases group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_resigned_and_left' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_resigned_and_left group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_secondee_outward' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_secondee_outward group by File_Date,Dated_on union 
select 'raw_hist_headcount_monthly_loaned_staff_from_ki' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_from_ki group by File_Date,Dated_on union 
select 'raw_hist_compensation_additional_pay' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_compensation_additional_pay group by File_Date,Dated_on union 
select 'raw_hist_compensation_paysheet' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_compensation_paysheet group by File_Date,Dated_on union 
select 'raw_hist_compensation_yec' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_compensation_yec group by File_Date,Dated_on union 
select 'raw_hist_compensation_finance_metrics' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_compensation_finance_metrics group by File_Date,Dated_on union 
select 'raw_hist_employee_engagement_thanks_dump' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_employee_engagement_thanks_dump group by File_Date,Dated_on union 
select 'raw_hist_employee_engagement_encore_output' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_employee_engagement_encore_output group by File_Date,Dated_on union 
select 'raw_hist_employee_engagement_rock' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_employee_engagement_rock group by File_Date,Dated_on union 
select 'raw_hist_employee_engagement_gps' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_employee_engagement_gps group by File_Date,Dated_on union 
select 'raw_hist_employee_engagement_year_end' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_employee_engagement_year_end group by File_Date,Dated_on union 
select 'raw_hist_lnd_glms_details' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_lnd_glms_details group by File_Date,Dated_on union 
select 'raw_hist_lnd_kva_details' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_lnd_kva_details group by File_Date,Dated_on union 
select 'raw_hist_lnd_glms_kva_details' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_lnd_glms_kva_details group by File_Date,Dated_on union
select 'raw_hist_i_and_d_gl_dump' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_i_and_d_gl_dump group by File_Date,Dated_on union
select 'raw_hist_jml_germany_joiner' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_germany_joiner group by File_Date,Dated_on union
select 'raw_hist_jml_germany_leaver' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_germany_leaver group by File_Date,Dated_on union
select 'raw_hist_jml_germany_mover' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_germany_mover group by File_Date,Dated_on union
select 'raw_hist_jml_nl_joiner' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_nl_joiner group by File_Date,Dated_on union
select 'raw_hist_jml_nl_leaver' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_nl_leaver group by File_Date,Dated_on union
select 'raw_hist_jml_nl_mover' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_nl_mover group by File_Date,Dated_on union
select 'raw_hist_jml_uk_joiner' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_uk_joiner group by File_Date,Dated_on union
select 'raw_hist_jml_uk_leaver' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_uk_leaver group by File_Date,Dated_on union
select 'raw_hist_jml_uk_mover' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_uk_mover group by File_Date,Dated_on union
select 'raw_hist_jml_us_joiner' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_us_joiner group by File_Date,Dated_on union
select 'raw_hist_jml_us_leaver' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_us_leaver group by File_Date,Dated_on union
select 'raw_hist_jml_us_mover' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_jml_us_mover group by File_Date,Dated_on union
select 'raw_hist_talent_acquisition_kgs_joiners_report' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report group by File_Date,Dated_on union
select 'raw_hist_talent_acquisition_requisition_dump' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_talent_acquisition_requisition_dump group by File_Date,Dated_on union 
select 'raw_hist_global_mobility_l1_visa_tracker' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_global_mobility_l1_visa_tracker group by File_Date,Dated_on union
select 'raw_hist_global_mobility_non_us_tracker' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_global_mobility_non_us_tracker group by File_Date,Dated_on union
select 'raw_hist_global_mobility_secondment_tracker' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_global_mobility_secondment_tracker group by File_Date,Dated_on union
select 'raw_hist_global_mobility_secondment_details' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_global_mobility_secondment_details group by File_Date,Dated_on union
select 'raw_hist_global_mobility_opportunity_dump' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.raw_hist_global_mobility_opportunity_dump group by File_Date,Dated_on

order by Delta_Table_Name,File_Date,Dated_on;

-- COMMAND ----------

select 'trusted_hist_headcount_employee_details' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_employee_details where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_academic_trainee' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_academic_trainee where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_contingent_worker' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_contingent_worker_resigned' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_loaned_staff_resigned' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_sabbatical' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_sabbatical where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_maternity_cases' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_maternity_cases where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_resigned_and_left' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_resigned_and_left where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_secondee_outward' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_secondee_outward where File_Date = '20230517' group by File_Date,Dated_on union 
select 'trusted_hist_headcount_loaned_staff_from_ki' as Delta_Table_Name, File_Date,Dated_on,count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_from_ki where File_Date = '20230517' group by File_Date,Dated_on 

-- COMMAND ----------

delete from kgsonedatadb.raw_hist_headcount_loaned_staff_from_ki where File_Date = '20230517' and Dated_On != (select max(Dated_On) from kgsonedatadb.raw_hist_headcount_loaned_staff_from_ki where File_Date = '20230517')

-- COMMAND ----------

select file_date,Dated_On,count(1) from kgsonedatadb.trusted_hist_headcount_resigned_and_left group by File_Date,Dated_On

select file_date,Dated_On,count(1) from kgsonedatadb.raw_hist_headcount_leave_report group by File_Date,Dated_On

-- update kgsonedatadb.trusted_hist_headcount_leave_report set file_date = '20230517' where file_date = '20230526'

select * from kgsonedatadb.trusted_headcount_resigned_and_left

-- COMMAND ----------

-- DBTITLE 1,Total Raw hist count
select 'raw_hist_bgv_kcheck' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_bgv_kcheck union
select 'raw_hist_bgv_offer_release' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_bgv_offer_release union
select 'raw_hist_bgv_progress_sheet' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_bgv_progress_sheet union
select 'raw_hist_bgv_waiver_tracker' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_bgv_waiver_tracker union
select 'raw_hist_compensation_additional_pay' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_compensation_additional_pay union
select 'raw_hist_compensation_paysheet' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_compensation_paysheet union
select 'raw_hist_compensation_yec' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_compensation_yec union
select 'raw_hist_employee_engagement_encore_output' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_employee_engagement_encore_output union
select 'raw_hist_employee_engagement_thanks_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_employee_engagement_thanks_dump union
select 'raw_hist_global_mobility_l1_visa_tracker' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_global_mobility_l1_visa_tracker union
select 'raw_hist_global_mobility_non_us_tracker' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_global_mobility_non_us_tracker union
select 'raw_hist_global_mobility_opportunity_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_global_mobility_opportunity_dump union
select 'raw_hist_global_mobility_secondment_tracker' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_global_mobility_secondment_tracker union
select 'raw_hist_headcount_academic_trainee' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_academic_trainee union
select 'raw_hist_headcount_contingent' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_contingent union
select 'raw_hist_headcount_contingent_worker' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_contingent_worker union
select 'raw_hist_headcount_contingent_worker_resigned' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_contingent_worker_resigned union
select 'raw_hist_headcount_employee_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_employee_details union
select 'raw_hist_headcount_employee_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_employee_dump union
select 'raw_hist_headcount_leave_report' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_leave_report union
select 'raw_hist_headcount_loaned_staff_from_ki' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_loaned_staff_from_ki union
select 'raw_hist_headcount_loaned_staff_resigned' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_loaned_staff_resigned union
select 'raw_hist_headcount_maternity_cases' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_maternity_cases union
select 'raw_hist_headcount_monthly_academic_trainee' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_academic_trainee union
select 'raw_hist_headcount_monthly_contingent_worker' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker union
select 'raw_hist_headcount_monthly_contingent_worker_resigned' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker_resigned union
select 'raw_hist_headcount_monthly_employee_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_employee_details union
select 'raw_hist_headcount_monthly_loaned_staff_from_ki' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_from_ki union
select 'raw_hist_headcount_monthly_loaned_staff_resigned' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_loaned_staff_resigned union
select 'raw_hist_headcount_monthly_maternity_cases' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_maternity_cases union
select 'raw_hist_headcount_monthly_resigned_and_left' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_resigned_and_left union
select 'raw_hist_headcount_monthly_sabbatical' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_sabbatical union
select 'raw_hist_headcount_monthly_secondee_outward' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_monthly_secondee_outward union
select 'raw_hist_headcount_resigned_and_left' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_resigned_and_left union
select 'raw_hist_headcount_sabbatical' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_sabbatical union
select 'raw_hist_headcount_secondee_outward' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_secondee_outward union
select 'raw_hist_headcount_talent_konnect_resignation_status_report' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report union
select 'raw_hist_headcount_termination_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_headcount_termination_dump union
select 'raw_hist_i_and_d_gl_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_i_and_d_gl_dump union
select 'raw_hist_jml_germany_joiner' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_germany_joiner union
select 'raw_hist_jml_germany_leaver' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_germany_leaver union
select 'raw_hist_jml_germany_mover' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_germany_mover union
select 'raw_hist_jml_nl_joiner' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_nl_joiner union
select 'raw_hist_jml_nl_leaver' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_nl_leaver union
select 'raw_hist_jml_nl_mover' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_nl_mover union
select 'raw_hist_jml_uk_joiner' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_uk_joiner union
select 'raw_hist_jml_uk_leaver' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_uk_leaver union
select 'raw_hist_jml_uk_mover' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_uk_mover union
select 'raw_hist_jml_us_joiner' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_us_joiner union
select 'raw_hist_jml_us_leaver' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_us_leaver union
select 'raw_hist_jml_us_mover' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_jml_us_mover union
select 'raw_hist_lnd_glms_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_lnd_glms_details union
select 'raw_hist_lnd_glms_kva_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_lnd_glms_kva_details union
select 'raw_hist_lnd_kva_details' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_lnd_kva_details union
select 'raw_hist_talent_acquisition_kgs_joiners_report' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_talent_acquisition_kgs_joiners_report union
select 'raw_hist_talent_acquisition_requisition_dump' as Delta_Table_Name,count(1) as count from kgsonedatadb.raw_hist_talent_acquisition_requisition_dump

-- COMMAND ----------

select 'trusted_hist_headcount_employee_dump' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_employee_dump group by File_Date union
select 'trusted_hist_headcount_talent_konnect_resignation_status_report' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report group by File_Date union 
select 'trusted_hist_headcount_contingent' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent group by File_Date union 
select 'trusted_hist_headcount_termination_dump' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_termination_dump group by File_Date union
select 'trusted_hist_headcount_leave_report' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_leave_report group by File_Date union 
select 'trusted_hist_headcount_employee_dump' as Delta_Table_Name, File_Date,Dated_On,count(1) as count from kgsonedatadb.trusted_hist_headcount_employee_dump group by File_Date,Dated_On union 
select 'trusted_hist_headcount_sabbatical' as Delta_Table_Name, File_Date,Dated_On,count(1) as count from kgsonedatadb.trusted_hist_headcount_sabbatical group by File_Date,Dated_On union 
select 'trusted_hist_headcount_maternity_cases' as Delta_Table_Name, File_Date,Dated_On,count(1) as count from kgsonedatadb.trusted_hist_headcount_maternity_cases group by File_Date,Dated_On union
select 'trusted_hist_headcount_academic_trainee' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_academic_trainee group by File_Date union 
select 'trusted_hist_headcount_contingent_worker' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker group by File_Date union 
select 'trusted_hist_headcount_contingent_worker_resigned' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned group by File_Date union 
select 'trusted_hist_headcount_loaned_staff_resigned' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned group by File_Date union  
select 'trusted_hist_headcount_resigned_and_left' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_resigned_and_left group by File_Date union 
select 'trusted_hist_headcount_secondee_outward' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_secondee_outward group by File_Date union 
select 'trusted_hist_headcount_loaned_staff_from_ki' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_from_ki group by File_Date union 
select 'trusted_hist_headcount_monthly_employee_details' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_employee_details group by File_Date union 
select 'trusted_hist_headcount_monthly_academic_trainee' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee group by File_Date union 
select 'trusted_hist_headcount_monthly_contingent_worker' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker group by File_Date union 
select 'trusted_hist_headcount_monthly_contingent_worker_resigned' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned group by File_Date union 
select 'trusted_hist_headcount_monthly_loaned_staff_resigned' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned group by File_Date union 
select 'trusted_hist_headcount_monthly_sabbatical' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical group by File_Date union 
select 'trusted_hist_headcount_monthly_maternity_cases' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases group by File_Date union 
select 'trusted_hist_headcount_monthly_resigned_and_left' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left group by File_Date union 
select 'trusted_hist_headcount_monthly_secondee_outward' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward group by File_Date union 
select 'trusted_hist_headcount_monthly_loaned_staff_from_ki' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki group by File_Date union 
select 'trusted_hist_compensation_additional_pay' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_compensation_additional_pay group by File_Date union 
select 'trusted_hist_compensation_paysheet' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_compensation_paysheet group by File_Date union 
select 'trusted_hist_compensation_yec' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_compensation_yec group by File_Date union 
select 'trusted_hist_compensation_finance_metrics' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_compensation_finance_metrics group by File_Date union 
select 'trusted_hist_employee_engagement_thanks_dump' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump group by File_Date union 
select 'trusted_hist_employee_engagement_encore_output' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_encore_output group by File_Date union 
select 'trusted_hist_employee_engagement_rock' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_rock group by File_Date union 
select 'trusted_hist_employee_engagement_gps' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_gps group by File_Date union 
select 'trusted_hist_employee_engagement_year_end' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_year_end group by File_Date union 
select 'trusted_hist_lnd_glms_details' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_lnd_glms_details group by File_Date union 
select 'trusted_hist_lnd_kva_details' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_lnd_kva_details group by File_Date union 
select 'trusted_hist_lnd_glms_kva_details' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_lnd_glms_kva_details group by File_Date union
select 'trusted_hist_i_and_d_gl_dump' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_i_and_d_gl_dump group by File_Date union
select 'trusted_hist_jml_germany_joiner' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_germany_joiner group by File_Date union
select 'trusted_hist_jml_germany_leaver' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_germany_leaver group by File_Date union
select 'trusted_hist_jml_germany_mover' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_germany_mover group by File_Date union
select 'trusted_hist_jml_nl_joiner' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_nl_joiner group by File_Date union
select 'trusted_hist_jml_nl_leaver' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_nl_leaver group by File_Date union
select 'trusted_hist_jml_nl_mover' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_nl_mover group by File_Date union
select 'trusted_hist_jml_uk_joiner' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_uk_joiner group by File_Date union
select 'trusted_hist_jml_uk_leaver' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_uk_leaver group by File_Date union
select 'trusted_hist_jml_uk_mover' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_uk_mover group by File_Date union
select 'trusted_hist_jml_us_joiner' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_us_joiner group by File_Date union
select 'trusted_hist_jml_us_leaver' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_us_leaver group by File_Date union
select 'trusted_hist_jml_us_mover' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_jml_us_mover group by File_Date union
select 'trusted_hist_talent_acquisition_kgs_joiners_report' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report group by File_Date union
select 'trusted_hist_talent_acquisition_requisition_dump' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump group by File_Date union 
select 'trusted_hist_global_mobility_l1_visa_tracker' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_l1_visa_tracker group by File_Date union
select 'trusted_hist_global_mobility_non_us_tracker' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_non_us_tracker group by File_Date union
select 'trusted_hist_global_mobility_secondment_tracker' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_secondment_tracker group by File_Date union
select 'trusted_hist_global_mobility_secondment_details' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_secondment_details group by File_Date union
select 'trusted_hist_global_mobility_opportunity_dump' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.trusted_hist_global_mobility_opportunity_dump group by File_Date

order by Delta_Table_Name,File_Date;

-- COMMAND ----------

-- select * from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump where File_Date = '20230430'

select distinct file_date,Dated_On from kgsonedatadb.trusted_hist_headcount_employee_dump order by file_date desc,dated_on desc

select file_date,dated_on,count(1) from kgsonedatadb.trusted_hist_headcount_sabbatical group by file_date,dated_on order by file_date desc;

update kgsonedatadb.trusted_hist_headcount_sabbatical set file_date = '20230522' where dated_on = (select max(dated_on) from kgsonedatadb.trusted_hist_headcount_sabbatical where file_date = '20230517')

select file_date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_details group by file_date order by file_date desc;

-- COMMAND ----------

show tables in kgsonedatadb like 'trusted_hist*'

-- COMMAND ----------

select 'trusted_hist_headcount_employee_dump' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_employee_dump union
select 'trusted_hist_headcount_talent_konnect_resignation_status_report' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report union 
select 'trusted_hist_headcount_contingent' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent union 
select 'trusted_hist_headcount_termination_dump' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_termination_dump union
select 'trusted_hist_headcount_leave_report' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_leave_report union 
select 'trusted_hist_headcount_employee_details' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_employee_details union 
select 'trusted_hist_headcount_academic_trainee' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_academic_trainee union 
select 'trusted_hist_headcount_contingent_worker' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker union 
select 'trusted_hist_headcount_contingent_worker_resigned' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned union 
select 'trusted_hist_headcount_loaned_staff_resigned' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned union 
select 'trusted_hist_headcount_sabbatical' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_sabbatical union 
select 'trusted_hist_headcount_maternity_cases' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_maternity_cases union 
select 'trusted_hist_headcount_resigned_and_left' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_resigned_and_left union 
select 'trusted_hist_headcount_secondee_outward' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_secondee_outward union 
select 'trusted_hist_headcount_loaned_staff_from_ki' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_loaned_staff_from_ki union 
select 'trusted_hist_headcount_monthly_employee_details' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_employee_details union 
select 'trusted_hist_headcount_monthly_academic_trainee' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee union 
select 'trusted_hist_headcount_monthly_contingent_worker' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker union 
select 'trusted_hist_headcount_monthly_contingent_worker_resigned' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned union 
select 'trusted_hist_headcount_monthly_loaned_staff_resigned' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned union 
select 'trusted_hist_headcount_monthly_sabbatical' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical union 
select 'trusted_hist_headcount_monthly_maternity_cases' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases union 
select 'trusted_hist_headcount_monthly_resigned_and_left' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left union 
select 'trusted_hist_headcount_monthly_secondee_outward' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward union 
select 'trusted_hist_headcount_monthly_loaned_staff_from_ki' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki union 
select 'trusted_hist_compensation_additional_pay' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_compensation_additional_pay union 
select 'trusted_hist_compensation_paysheet' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_compensation_paysheet union 
select 'trusted_hist_compensation_yec' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_compensation_yec union 
select 'trusted_hist_compensation_finance_metrics' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_compensation_finance_metrics union 
select 'trusted_hist_employee_engagement_thanks_dump' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump union 
select 'trusted_hist_employee_engagement_encore_output' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_encore_output union 
select 'trusted_hist_employee_engagement_rock' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_rock union 
select 'trusted_hist_employee_engagement_gps' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_gps union 
select 'trusted_hist_employee_engagement_year_end' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_employee_engagement_year_end union 
select 'trusted_hist_lnd_glms_details' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_lnd_glms_details union 
select 'trusted_hist_lnd_kva_details' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_lnd_kva_details union 
select 'trusted_hist_lnd_glms_kva_details' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_lnd_glms_kva_details union
select 'trusted_hist_i_and_d_gl_dump' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_i_and_d_gl_dump union
select 'trusted_hist_talent_acquisition_kgs_joiners_report' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report union
select 'trusted_hist_talent_acquisition_requisition_dump' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump union 
select 'trusted_hist_global_mobility_l1_visa_tracker' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_global_mobility_l1_visa_tracker union
select 'trusted_hist_global_mobility_non_us_tracker' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_global_mobility_non_us_tracker union
select 'trusted_hist_global_mobility_secondment_tracker' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_global_mobility_secondment_tracker union
select 'trusted_hist_global_mobility_secondment_details' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_global_mobility_secondment_details union
select 'trusted_hist_global_mobility_opportunity_dump' as Delta_Table_Name, count(1) as count from kgsonedatadb.trusted_hist_global_mobility_opportunity_dump

order by Delta_Table_Name;

-- COMMAND ----------

select 'trusted_hist_headcount_employee_dump' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_employee_dump union
select 'trusted_hist_headcount_talent_konnect_resignation_status_report' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report union 
select 'trusted_hist_headcount_contingent' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_contingent union 
select 'trusted_hist_headcount_termination_dump' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_termination_dump union
select 'trusted_hist_headcount_leave_report' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_leave_report union 
select 'trusted_hist_headcount_employee_details' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_employee_details union 
select 'trusted_hist_headcount_academic_trainee' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_academic_trainee union 
select 'trusted_hist_headcount_contingent_worker' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_contingent_worker union 
select 'trusted_hist_headcount_contingent_worker_resigned' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_contingent_worker_resigned union 
select 'trusted_hist_headcount_loaned_staff_resigned' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned union 
select 'trusted_hist_headcount_sabbatical' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_sabbatical union 
select 'trusted_hist_headcount_maternity_cases' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_maternity_cases union 
select 'trusted_hist_headcount_resigned_and_left' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_resigned_and_left union 
select 'trusted_hist_headcount_secondee_outward' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_secondee_outward union 
select 'trusted_hist_headcount_loaned_staff_from_ki' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_loaned_staff_from_ki union 
select 'trusted_hist_headcount_monthly_employee_details' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_employee_details union 
select 'trusted_hist_headcount_monthly_academic_trainee' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee union 
select 'trusted_hist_headcount_monthly_contingent_worker' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker union 
select 'trusted_hist_headcount_monthly_contingent_worker_resigned' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned union 
select 'trusted_hist_headcount_monthly_loaned_staff_resigned' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned union 
select 'trusted_hist_headcount_monthly_sabbatical' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical union 
select 'trusted_hist_headcount_monthly_maternity_cases' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases union 
select 'trusted_hist_headcount_monthly_resigned_and_left' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left union 
select 'trusted_hist_headcount_monthly_secondee_outward' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward union 
select 'trusted_hist_headcount_monthly_loaned_staff_from_ki' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki union 
select 'trusted_hist_compensation_additional_pay' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_compensation_additional_pay union 
select 'trusted_hist_compensation_paysheet' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_compensation_paysheet union 
select 'trusted_hist_compensation_yec' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_compensation_yec union 
select 'trusted_hist_compensation_finance_metrics' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_compensation_finance_metrics union 
select 'trusted_hist_employee_engagement_thanks_dump' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump union 
select 'trusted_hist_employee_engagement_encore_output' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_employee_engagement_encore_output union 
select 'trusted_hist_employee_engagement_rock' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_employee_engagement_rock union 
select 'trusted_hist_employee_engagement_gps' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_employee_engagement_gps union 
select 'trusted_hist_employee_engagement_year_end' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_employee_engagement_year_end union 
select 'trusted_hist_lnd_glms_details' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_lnd_glms_details union 
select 'trusted_hist_lnd_kva_details' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_lnd_kva_details union 
select 'trusted_hist_lnd_glms_kva_details' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_lnd_glms_kva_details union
select 'trusted_hist_i_and_d_gl_dump' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_i_and_d_gl_dump union
select 'trusted_hist_talent_acquisition_kgs_joiners_report' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report union
select 'trusted_hist_talent_acquisition_requisition_dump' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_talent_acquisition_requisition_dump union 
select 'trusted_hist_global_mobility_l1_visa_tracker' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_global_mobility_l1_visa_tracker union
select 'trusted_hist_global_mobility_non_us_tracker' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_global_mobility_non_us_tracker union
select 'trusted_hist_global_mobility_secondment_tracker' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_global_mobility_secondment_tracker union
select 'trusted_hist_global_mobility_secondment_details' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_global_mobility_secondment_details union
select 'trusted_hist_global_mobility_opportunity_dump' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_global_mobility_opportunity_dump union
select 'trusted_hist_bgv_upcoming_joiners' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_bgv_upcoming_joiners union
select 'trusted_hist_jml_us_joiner' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_us_joiner union
select 'trusted_hist_jml_us_mover' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_us_mover union
select 'trusted_hist_jml_us_leaver' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_us_leaver union
select 'trusted_hist_jml_uk_joiner' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_uk_joiner union
select 'trusted_hist_jml_uk_mover' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_uk_mover union
select 'trusted_hist_jml_uk_leaver' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_uk_leaver union
select 'trusted_hist_jml_germany_joiner' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_germany_joiner union
select 'trusted_hist_jml_germany_mover' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_germany_mover union
select 'trusted_hist_jml_germany_leaver' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_germany_leaver union
select 'trusted_hist_jml_netherland_joiner' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_nl_joiner union
select 'trusted_hist_jml_netherland_mover' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_nl_mover union
select 'trusted_hist_jml_netherland_leaver' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_jml_nl_leaver union
select 'trusted_hist_lnd_glms_kva_details' as Delta_Table_Name, concat(Min(to_date(File_Date,'yyyyMMdd')),' to ',Max(to_date(File_Date,'yyyyMMdd'))) as History_Date_Range from kgsonedatadb.trusted_hist_lnd_glms_kva_details
-- group by File_Date

-- order by Delta_Table_Name,File_Date;

-- COMMAND ----------

select min(Planned_Start_Date),max(Planned_Start_Date) from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report

-- COMMAND ----------

-- DBTITLE 1,Headcount Dumps
select 'employee_dump' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.raw_hist_headcount_employee_dump group by File_Date union
select 'talent_konnect_resignation_status_report' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report group by File_Date union 
select 'contingent' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.raw_hist_headcount_contingent group by File_Date union 
select 'termination_dump' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.raw_hist_headcount_termination_dump group by File_Date union
select 'leave_report' as Delta_Table_Name, File_Date,count(1) as count from kgsonedatadb.raw_hist_headcount_leave_report group by File_Date;


-- COMMAND ----------

select file_date, count(1) from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left group by File_Date

-- COMMAND ----------

-- select File_Date,count(1) from kgsonedatadb.trusted_hist_employee_engagement_year_end group by File_Date

-- select * from kgsonedatadb.trusted_hist_employee_engagement_rock where Employee_Number = '30840'


select 'trusted_hist_headcount_employee_dump' as table_name, file_date,count(1) from kgsonedatadb.trusted_hist_headcount_employee_dump group by file_date union
select 'trusted_hist_headcount_termination_dump' as table_name, file_date,count(1) from kgsonedatadb.trusted_hist_headcount_termination_dump group by file_date union 
select 'trusted_hist_headcount_contingent' as table_name, file_date,count(1) from kgsonedatadb.trusted_hist_headcount_contingent group by file_date union 
select 'trusted_hist_headcount_leave_report' as table_name, file_date,count(1) from kgsonedatadb.trusted_hist_headcount_leave_report group by file_date union 
select 'trusted_hist_headcount_talent_konnect_resignation_status_report' as table_name, file_date,count(1) from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report group by file_date



-- COMMAND ----------


-- select A.File_Date,count(1) from (select * from kgsonedatadb.trusted_hist_employee_engagement_thanks_dump where upper (Reward_Status) in ('APPROVED','APPROVAL NOT REQUIRED')) A LEFT JOIN kgsonedatadb.trusted_hist_headcount_employee_dump B on A.Nominee_s_Employee_ID = B.Employee_Number where B.Entity = 'KGS' and month(A.File_Date) = month(B.File_Date) group by A.File_Date

-- select Employee_Number,count(1) from kgsonedatadb.trusted_hist_employee_engagement_rock group by Employee_Number having count(1)>1
-- select * from kgsonedatadb.trusted_hist_employee_engagement_rock where Employee_Number = '15205' and left(File_Date,6) = '202212'

-- delete from kgsonedatadb.raw_hist_headcount_employee_dump where file_date = '20230118';
-- delete from kgsonedatadb.raw_hist_headcount_termination_dump where file_date = '20230118';
-- delete from kgsonedatadb.raw_hist_headcount_contingent where file_date = '20230118';
-- delete from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report where file_date = '20230118';
-- delete from kgsonedatadb.raw_hist_headcount_leave_report where file_date = '20230112';

-- select distinct Employee_Number,Person_Type_Flag,Full_Name,`Function`,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,`Location`,Sub_Location,`Position`,Job_Name,People_Group_Name,Supervisor_Employee_Number,Supervisor_Name,Employee_Category,PML_Number,PML,PM_Employee_Number,Performance_Manager,Partner_Employee_Number,Reporting_Partner,Gratuity_Date,Date_First_Hired,End_Date,Residential_Status,Gender,Date_of_Birth,Employee_Tenure,Email_Address,Total_Work_Experience,Previous_Employer,Company_Name,Normal_Hours,Legacy_Number,Start_Date_Of_Current_Employer,Principal_Name,Display_Name,UK_Employee_Number,US_Employee_Number,Desired_LWD,Approved_LWD,RM_Employee_Number,RM_Employee_Name,ICAI_Status,Source_of_Hire,Source_Of_Hire_Details,Referral_Employee_Number,Referral_Employee_Name,GPID,NATIONALITY,Status,Entity,TK_Status,Termination_Date,x,Remarks,Dated_On,File_Date into kgsonedatadb.temp_ed from kgsonedatadb.raw_hist_headcount_employee_dump where file_date = '20230118'

select file_date,Dated_On,count(1) from kgsonedatadb.trusted_hist_employee_engagement_gps group by file_date,Dated_On order by File_Date

-- COMMAND ----------

-- DBTITLE 1,Headcount_Monthly
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_employee_details group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_academic_trainee group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_contingent_worker_resigned group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_resigned group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_sabbatical group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_maternity_cases group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_resigned_and_left group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward group by File_Date;
-- select File_Date,count(1) as count from kgsonedatadb.trusted_hist_headcount_monthly_loaned_staff_from_ki group by File_Date;