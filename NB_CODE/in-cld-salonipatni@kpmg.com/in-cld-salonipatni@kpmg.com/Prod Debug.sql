-- Databricks notebook source
-- select * from kgsonedatadb.raw_hist_headcount_employee_dump where File_Date = '20230606' and Employee_Number = '103983';
select * from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date = '20230606' and Employee_Number = '103983'

-- select * from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report where File_Date = '20230605' and EMPLOYEENUMBER = '103983'

-- select * from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report where File_Date = '20230612' and EMPLOYEENUMBER = '103983'


-- TK File - 20230605 - LWD_PENDING_WITH_PARTNER_NAME and REQUESTSTATUS

-- COMMAND ----------

select EMPLOYEENUMBER,File_Date from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report group by EMPLOYEENUMBER,File_Date having count(1)>1

-- COMMAND ----------

select distinct file_date from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report

-- COMMAND ----------

select * from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report where EMPLOYEENUMBER IN ('103983',
'131940',
'132156',
'28284',
'89468',
'124396',
'103983',
'118397',
'131908',
'111593',
'118289',
'95892',
'126821',
'124407',
'116053',
'126413',
'109433',
'118665',
'131911',
'108713',
'131912',
'131905',
'131972',
'131902',
'131910',
'101269',
'133269',
'127708',
'84739',
'108499',
'134231',
'128451',
'116073',
'118866',
'93550',
'108907',
'125010',
'105277') AND File_Date in ('20230605','20230612','20230529','20230428','20230522') order by File_date,EMPLOYEENUMBER desc


-- COMMAND ----------

select * from kgsonedatadb.raw_hist_headcount_talent_konnect_resignation_status_report  WHERE eMPLOYEENUMBER IN /*('116073',
'118866',
'93550',
'108907',
'125010',
'105277'*/
('128451') AND fILE_dATE = '20230428'

-- COMMAND ----------

select * from kgsonedatadb.TRUSTED_hist_headcount_EMPLOYEE_DUMP  WHERE eMPLOYEE_NUMBER IN ('116073',
'118866',
'93550',
'108907',
'125010',
'105277'
) AND fILE_dATE = '20230518'

-- COMMAND ----------

 select * from kgsonedatadb.TRUSTED_hist_headcount_EMPLOYEE_DUMP WHERE EMPLOYEE_NUMBER IN ('128451'

) AND fILE_dATE = '20230522'

-- COMMAND ----------

