-- Databricks notebook source
-- DBTITLE 1,ED - EDW vs HRSS - Validation
select trim(`Employee Number`),trim(`Person Type Flag`),trim(`Full Name`),trim(`Function`),trim(`Sub Function`),trim(`Service Line`)
-- ,upper(trim(`Organization Name`))
,trim(`Cost centre`)
,trim(`Operating Unit`)
,trim(`User Type`)
,trim(`Client Geography`)
,trim(`Sub Location`)
,trim(`Position`)
,trim(`Job Name`)
,trim(`People Group Name`)
-- ,trim(`Supervisor Employee Number`)
-- ,trim(`Supervisor Name`)
,trim(`Employee Category`)
,trim(`PML Number`)
,trim(`PML`)
-- ,trim(`PM Employee Number`)
-- ,upper(trim(`Performance Manager`))
-- ,trim(`Partner Employee Number`)
-- ,trim(`Reporting Partner`)
-- ,trim(`Gratuity Date`)
,trim(`Date First Hired`)
,trim(`End Date`)
,trim(`Residential Status`)
,trim(`Gender`)
,trim(`Date of Birth`)
,trim(`Employee Tenure`)
,trim(`Email Address`)
,trim(`Total Work Experience`)
,trim(round(`Previous Employer`,0))
,trim(`Company Name`)
,trim(`Normal Hours`)
,trim(`Legacy Number`)
-- ,trim(`Start Date Of Current Employer`)
-- ,upper(trim(`Display Name`))
,trim(`UK Employee Number`)
,trim(`US Employee Number`)
,trim(`Desired LWD`)
,trim(`Approved LWD`)
,trim(`RM Employee Number`)
,trim(`RM Employee Name`)
,trim(`ICAI Status`)
,trim(`Source of Hire`)
-- ,trim(`Referral Employee Number`)
-- ,trim(`Referral Employee Name`)
,trim(`GPID`)
,trim(`NATIONALITY`)
from default.ed_edw_01232024
where `Employee Number` <> '39990' 
-- and `Employee Number` in ('142049','30840','31024','48882','135348')
-- and `Employee Number` in ('130694')

except
-- union all

select trim(`Employee Number`),trim(`Person Type Flag`),trim(`Full Name`),trim(`Function`),trim(`Sub Function`)
,trim(`Service Line`)
-- ,upper(trim(`Organization Name`))
,trim(`Cost centre`)
,trim(`Operating Unit`)
,trim(`User Type`)
,trim(`Client Geography`)
,trim(`Sub Location`)
,trim(`Position`)
,trim(`Job Name`)
,trim(`People Group Name`)
-- ,trim(`Supervisor Employee Number`)
-- ,trim(`Supervisor Name`)
,trim(`Employee Category`)
,trim(`PML Number`)
,trim(`PML`)
-- ,trim(`PM Employee Number`)
-- ,upper(trim(`Performance Manager`))
-- ,trim(`Partner Employee Number`)
-- ,trim(`Reporting Partner`)
-- ,trim(`Gratuity Date`)
,trim(`Date First Hired`)
,trim(`End Date`)
,trim(`Residential Status`)
,trim(`Gender`)
,trim(`Date of Birth`)
,trim(`Employee Tenure`)
,trim(`Email Address`)
,trim(`Total Work Experience`)
,trim(round(`Previous Employer`,0))
,trim(`Company Name`)
,trim(`Normal Hours`)
,trim(`Legacy Number`)
-- ,trim(`Start Date Of Current Employer`)
-- ,upper(trim(`Display Name`))
,trim(`UK Employee Number`)
,trim(`US Employee Number`)
,date_format(to_date(trim(`Desired LWD`), 'd-MMM-yy'),'yyyy-MM-dd')
,date_format(to_date(trim(`Approved LWD`), 'd-MMM-yy'),'yyyy-MM-dd')
,trim(`RM Employee Number`)
,trim(`RM Employee Name`)
,trim(`ICAI Status`)
,trim(`Source of Hire`)
-- ,trim(`Referral Employee Number`)
-- ,trim(`Referral Employee Name`)
,trim(`GPID`)
,trim(`NATIONALITY`) 
from default.ed_hrss_01232024
where `Employee Number` <> '39990'
-- and `Employee Number` in ('142049','30840','31024','48882','135348')
-- and `Employee Number` in ('130694')

-- COMMAND ----------

-- DBTITLE 1,ED - EDW vs HRSS - Observations
-- ,`BU Mapping` where `Employee Number` = '141831'
-- ,`Business Category` where `Employee Number` = '141831'
-- ,`Source Of Hire Details` where `Employee Number` = '141831'
-- ,`Location` where `Employee Number` = '80687'
-- ,`Principal Name` where `Employee Number` = '80687'
-- ,`Organization Name` where `Employee Number` in ('115677','142580','143502','74400') - because Description not available in LOOKUP_HR_CLIENT_GEOGRAPHY
-- `Supervisor Employee Number` is null where `Employee Number` in ('142049','30840','31024','48882','135348')
-- `Supervisor Name` is null where `Employee Number` in ('142049','30840','31024','48882','135348')
-- `PM Employee Number` is null in EDW but present in HRSS where `Employee Number` in ('142049','30840','31024','48882','135348')
-- `Performance Manager` is null in EDW but present in HRSS where `Employee Number` in ('142049','30840','31024','48882','135348')
-- `Partner Employee Number`  is null in EDW but present in HRSS where `Employee Number` in ('135268','48882')
-- `Reporting Partner`  is null in EDW but present in HRSS where `Employee Number` in ('135268','48882')
-- For 330 records, Gratuity Date is before the Date First Hired, specifically whose `Date First Hired` is in ('2014-03-01','2013-07-15').
-- For 330 records, `Start Date Of Current Employer` does not match between HRSS vs EDW. - `Start Date Of Current Employer` is before the Date First Hired, specifically whose `Date First Hired` is in ('2014-03-01','2013-07-15')
-- `Display Name` not matching for many records 
-- `Referral Employee Number` - Not matching for 82 records. Value in EDW, NULL in HRSS
-- `Referral Employee Name` - Not matching for 82 records. Value in EDW, NULL in HRSS

-- COMMAND ----------

-- DBTITLE 1,TD - EDW vs HRSS - Validation
-- select count(1) from (
select 
trim(`Employee Number`)
-- ,upper(trim(`Person Type Flag`))
-- ,trim(`Employee Name`)
,trim(`Function`)
,trim(`Sub Function`)
-- ,trim(`Sub Function1`)
,trim(`Profit Centre`)
-- ,trim(`BU Mapping`)
-- ,trim(`Business Category`)
,trim(`User Type `)
,trim(`Client Geography`)
,trim(`Position`)
-- ,trim(`Location`)
,trim(`Sub Location`)
,trim(`Job`)
,trim(`Date First Hired`)
,trim(`Termination Date`)
,trim(`Date of Birth`)
,trim(`Gender`)
-- ,trim(`Employee Category `)
,trim(`Company Name`)
,trim(`People Group`)
-- ,trim(`Supervisor Name`)
-- ,trim(`Employee Tenure`)
,trim(`Leaving Reason`)
,trim(`email Address`)
,trim(`Assignment End Date`)
-- ,trim(`Gratuity Date`)
-- ,trim(`Previous Employer`)
-- ,trim(`Start Date Of Current Employer`)
,trim(`Last Update Date`)
-- ,trim(`Reporting Partner Name`)
,trim(`Source of Hire Details`)
,trim(`Source Of Hire Details`)
-- ,trim(`Referrer Emp No`)
-- ,trim(`Referrer Emp Name`)
,trim(`RM Employee Name`)
,trim(`KGS Business Unit`)
-- ,trim(`KGS Service Line`)
,trim(`KGS Service Network`)
,trim(`GPID`)
,trim(`Entity Name`)
,trim(`Latest  Available Rating`)
from default.td_edw_01232024
where `Employee Number` not in ('18651','45538','45539','45537','45579')
-- and `Employee Number` in ('29722')

except
-- union all

select 
trim(`Employee Number`)
-- ,upper(trim(`Person Type Flag`))
-- ,trim(`Employee Name`)
,trim(`Function`)
,trim(`Sub Function`)
-- ,trim(`Sub Function1`)
,trim(`Profit Centre`)
-- ,trim(`BU Mapping`)
-- ,trim(`Business Category`)
,trim(`User Type `)
,trim(`Client Geography`)
,trim(`Position`)
-- ,trim(`Location`)
,trim(`Sub Location`)
,trim(`Job`)
,trim(`Date First Hired`)
,trim(`Termination Date`)
,trim(`Date of Birth`)
,trim(`Gender`)
-- ,trim(`Employee Category `)
,trim(`Company Name`)
,trim(`People Group`)
-- ,trim(`Supervisor Name`)
-- ,trim(`Employee Tenure`)
,trim(`Leaving Reason`)
,trim(`email Address`)
,trim(`Assignment End Date`)
-- ,trim(`Gratuity Date`)
-- ,trim(`Previous Employer`)
-- ,trim(`Start Date Of Current Employer`)
,trim(`Last Update Date`)
-- ,trim(`Reporting Partner Name`)
,trim(`Source of Hire Details`)
,trim(`Source Of Hire Details`)
-- ,trim(`Referrer Emp No`)
-- ,trim(`Referrer Emp Name`)
,trim(`RM Employee Name`)
,trim(`KGS Business Unit`)
-- ,trim(`KGS Service Line`)
,trim(`KGS Service Network`)
,trim(`GPID`)
,trim(`Entity Name`)
,trim(`Latest  Available Rating`)
from default.td_hrss_01232024
where `Employee Number` not in ('18651','45538','45539','45537','45579')
-- and `Employee Number` in ('29722')
-- ) A

-- COMMAND ----------

-- DBTITLE 1,TD - EDW vs HRSS - Observations
-- Person Type Flag - 35747, 36351, 47969, 55385 - EMPLOYEE in EDW, Contractual Employee in HRSS
-- Employee Name - 44422, 44600, 45538, 45049 - Special character issue
-- Function - 1 record mismatch, Employee Number = 18651
-- Sub Function - 1 record mismatch, Employee Number = 18651
-- Sub Function1 - 10543 records - Mismatch
-- Profit Centre - 1 record mismatch, Employee Number = 18651
-- BU Mapping - 18152 records mismatch
-- Business Category - 23783 records mismatch
-- User Type - 1 record mismatch, Employee Number = 18651
-- Client Geography - 1 record mismatch, Employee Number = 18651
-- Position - 1 record mismatch, Employee Number = 18651
-- Location - 9484 records mismatch
-- `Employee Category ` - 20891 records mismatch - Eg., `Employee Number` in ('12776') - "Serving Notice Period" in EDW but "Confirmed Staff" in HRSS file
-- `Supervisor Name` - 29 records mismatch
-- `Employee Tenure` - 24824 records mismatch - Eg., - `Employee Number` in ('23436') - 12.33 in EDW and 3.05 in HRSS
-- `email Address` - 3 records mismatch- `Employee Number` in ('45538','45539','45537') - Special character issue
-- `Gratuity Date` - 1544 records mismatch - Grauity Date is less than Date First Hired
-- `Previous Employer` - 34 records mismatch
-- `Start Date Of Current Employer` - 1554 records mismatch
-- `Reporting Partner Name` - 34 records mismatch
-- `Referrer Emp No` - 37 records mismatch
-- `Referrer Emp Name` - 37 records mismatch
-- `KGS Service Line` - 24546 records mismatch
-- `GPID` - 1 record - Employee Number = '45579'
-- `Latest  Available Rating` - 54 records mismatch

-- COMMAND ----------

select File_Date,count(1) from kgsonedatadb.trusted_hist_ta_post_hire_fte group by File_Date order by File_Date

-- COMMAND ----------

select File_Date,* from kgsonedatadb.trusted_hist_talent_acquisition_kgs_joiners_report where Candidate_Email in ('mevishal110@gmail.com'
,'ritasharochlani@gmail.com'
,'adarshtank@yahoo.in');

select File_Date,APPROVED_LWD,count(1) from kgsonedatadb.trusted_hist_headcount_talent_konnect_resignation_status_report group by File_Date,APPROVED_LWD order by APPROVED_LWD,File_Date desc