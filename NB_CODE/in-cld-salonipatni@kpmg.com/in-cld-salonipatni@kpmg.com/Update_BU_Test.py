# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_data_type_cast

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners
# MAGIC
# MAGIC
# MAGIC -- Employee Dump
# MAGIC -- Requisition Dump
# MAGIC -- KGS_New_Joiners_Report
# MAGIC -- Resignation_status_report

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select `Candidate_Email/Candidate_Email_Personal_Email`,count(1) from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where File_Date = '20230530' group by `Candidate_Email/Candidate_Email_Personal_Email`
# MAGIC
# MAGIC
# MAGIC select `Candidate_Email/Candidate_Email_Personal_Email`,Dated_On,count(1) from (select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where File_Date = '20230530') group by `Candidate_Email/Candidate_Email_Personal_Email`,Dated_On having count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Email,Initiated_On, External_Status,* from kgsonedatadb.trusted_bgv_kcheck where Email in (select `Candidate_Email/Candidate_Email_Personal_Email` from (select * from kgsonedatadb.trusted_hist_bgv_upcoming_joiners where File_Date = '20230530') group by `Candidate_Email/Candidate_Email_Personal_Email`,Dated_On having count(1)>1) order by Email,Initiated_On desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Candidate_Email,Start_Date, * from kgsonedatadb.trusted_hist_bgv_offer_release where Candidate_Email in (select Candidate_Email from (select * from kgsonedatadb.trusted_hist_bgv_offer_release where File_Date = '20230530') group by Candidate_Email,Start_Date,Dated_On,File_Date having count(1)>1) order by Candidate_Email,Start_Date desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Personal_Email_ID,count(1),File_Date from (select * from kgsonedatadb.trusted_hist_bgv_progress_sheet where File_Date = '20230530') group by Personal_Email_ID,Dated_On,File_Date having count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Candidate_Email_ID,count(1),File_Date from (select * from kgsonedatadb.trusted_hist_bgv_waiver_tracker where File_Date = '20230530') group by Candidate_Email_ID,Dated_On,File_Date having count(1)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.raw_hist_bgv_kcheck where File_Date = '20230530' and Email = 'aarushitalwar94@gmail.com'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct FIle_date,Dated_On from kgsonedatadb.trusted_hist_headcount_contingent_worker order by File_Date desc
# MAGIC select distinct FIle_date,Dated_On from kgsonedatadb.trusted_hist_headcount_academic_trainee order by File_Date desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- create table kgsonedatadb.glms_kva_details_test
# MAGIC -- select * from (select *,Row_Number() over(partition by Employee_Id,Co,Item_Type 
# MAGIC -- order by CPD_CPE_Hours_Awarded) as rowvalue  
# MAGIC -- from kgsonedatadb.trusted_hist_lnd_kva_details 
# MAGIC -- where file_date in ('20230228') and file_type='KVA')a where rowvalue=1
# MAGIC
# MAGIC -- select count(1) from kgsonedatadb.glms_kva_details_test --9527
# MAGIC
# MAGIC -- ALTER TABLE kgsonedatadb.glms_kva_details_test  SET TBLPROPERTIES (
# MAGIC --    'delta.columnMapping.mode' = 'name',
# MAGIC --    'delta.minReaderVersion' = '2',
# MAGIC --    'delta.minWriterVersion' = '5');
# MAGIC -- alter table kgsonedatadb.glms_kva_details_test drop column rowvalue;
# MAGIC
# MAGIC
# MAGIC -- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where file_date in ('20230228') and file_type='KVA' --19053
# MAGIC
# MAGIC
# MAGIC -- insert into kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC -- select * from kgsonedatadb.glms_kva_details_test
# MAGIC
# MAGIC
# MAGIC -- select count(1) from kgsonedatadb.trusted_hist_lnd_kva_details where file_date in ('20230228') and file_type='KVA' --19053
# MAGIC
# MAGIC -- drop table kgsonedatadb.glms_kva_details_test

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select Employee_Number,count(1) from kgsonedatadb.trusted_headcount_sabbatical group by Employee_Number

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- delete from kgsonedatadb.trusted_hist_bgv_kcheck where Dated_On = (select min(Dated_On) from kgsonedatadb.trusted_hist_bgv_kcheck where File_Date = '20230529')

# COMMAND ----------

# MAGIC %sql
# MAGIC select File_Date,count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20230201','20230301','20230401') group by File_Date

# COMMAND ----------

# jdbcHostname = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseHostName")  
# jdbcDatabase = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseName")  
# jdbcPort = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databasePort")  
# username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNameUserName")  
# password = password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNamePassword")  
# jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)  
# connectionProperties = {  
#   "user" : username,  
#   "password" : password,  
#   "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"  
# }  

# table = "trusted_hist_lnd_glms_kva_details"
# sampleDF=spark.sql('select Item_ID,Training_Category,Item_Title,KBS_Classification_,Item_Type,Assignment_Type,Security_Domain__Item_,CPD_CPE_Hours__Item_,Item_Revision_Date,Class_ID,Scheduled_Offering_Start_Date,SO_Item_Start_Time,Class_End_Date,Scheduled_Offering_End_Time,Completion_Date,Period,Completion_Time,Completion_Status_ID,Completion_Status_Description,Total_Hours,Credit_Hours,Contact_Hours,CPD_CPE_Hours_Awarded,Primary_Instructor,Last_Update_User_ID,Last_Update_Timestamp,Last_Update_Time,Comments,Local_HR_ID,First_Name,Last_Name,Middle_Name,Email_Address__User_,User_ID,Account_ID,User_Status,Job_Code_ID,Job_Code_Description,Job_Title,Position,Job_Name,Level_Wise,Country_Region,Office_Location_ID,Department,Global_Function,Organisation_Description,CC,BU,Subfunction,Employee_Class_Description,Local_Job_Level,Is_Client_Facing,Manager_Email_Address,Last_Hire_Date,Local_Function,Local_Service_Line,Cost_Center,ORG_ID,Geo,SL,Location,Item_Type_Category,Full_Name,Quarter,Original_Levelwise_Mapping,File_Type,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

# sampleDF.write \
# .format("jdbc") \
# .mode("append") \
# .option("url",jdbcUrl) \
# .option("dbtable",table) \
# .option("user", username) \
# .option("password", password) \
# .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from kgsonedatadb.trusted_hist_lnd_glms_kva_details

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select Cost_centre,BU,Client_Geography,Final_BU,count(1) from kgsonedatadb.config_cc_bu_sl group by Cost_centre,Client_Geography,BU,Final_BU having count(1)>1
# MAGIC select *  from kgsonedatadb.config_cc_bu_sl where Cost_centre in ('DA Core-CF-M&A-East','DA-Management-CF')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Local_HR_ID,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and SL is null and Cost_Center = 'Capability Hubs-KM-Shared Services'

# COMMAND ----------

# MAGIC %sql
# MAGIC select Cost_centre,Employee_Subfunction,Employee_Subfunction_1,Client_Geography,File_Date from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Employee_Number = '127820'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from  kgsonedatadb.config_bu_mapping_list
# MAGIC
# MAGIC select distinct Cost_Center,Geo,BU,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and SL is null and Cost_Center ='Capability Hubs-KM-Shared Services'
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 set SL = 'Knowledge Management' where Cost_Center = 'Capability Hubs-KM-Shared Services' and SL is null
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Cost_Center,Geo,BU,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and SL is null and Local_HR_ID='127820'
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set
# MAGIC -- Geo = 'ROW' , BU = 'Cap-Hubs' , SL ='Knowledge Management' where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and SL is null and Local_HR_ID='127820'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct Local_Service_Line,File_Type from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') --and (Cost_Center is not null and trim(Cost_Center)!='') and File_Type='KVA'
# MAGIC
# MAGIC
# MAGIC -- select distinct Local_Service_Line,SL,File_Type  from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and Local_Service_Line is null and File_Type='KVA'
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set Local_Service_Line = SL where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and Local_Service_Line is null and File_Type='KVA'
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Employee_Number,Location,File_Date from kgsonedatadb.trusted_hist_headcount_termination_dump where Employee_Number in ('131716',
# MAGIC -- '131809')
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set Location = 'Gurugram' where local_hr_id ='131809' and Location is null and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set Global_Function = 'Tax' where BU = 'Tax' and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- delete from kgsonedatadb.trusted_hist_lnd_glms_details where File_Date in ('20221001') and Local_HR_Id  ='118732'
# MAGIC
# MAGIC
# MAGIC -- delete from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20221001') and Local_HR_Id  ='118732'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20221130','20230228') and Local_HR_Id in ('KPMG20308943','KPMG66011029') and Email_Address__User_ in ('dharshikgopan@kpmg.com','mandadapuhimab@kpmg.com','kpmgindiaadmin@skillton.degreed.com') and 
# MAGIC -- CC is null
# MAGIC
# MAGIC select Email_Address__User_,* from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20221130','20230228') and Local_HR_Id in ('KPMG66011029')
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set
# MAGIC -- location = 'Bangalore',
# MAGIC -- Geo  = 'UK',
# MAGIC -- Position = 'Analyst',
# MAGIC -- Job_Name = 'Analyst',
# MAGIC -- Job_Title = 'Analyst',
# MAGIC -- Level_wise = 'Analysts to Seniors/Team Leaders ',
# MAGIC -- Original_Levelwise_Mapping ='Analysts to Seniors/Team Leaders ',
# MAGIC -- CC = 'Tax Tech-TTP',
# MAGIC -- BU = 'Tax',
# MAGIC -- SL = 'Tax Technology',
# MAGIC -- Global_Function = 'Tax' where File_Date in ('20221130') and Local_HR_Id in ('KPMG20308943')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_headcount_employee_dump where File_Date = '20221018' and email_address = 'mandadapuhimab@kpmg.com'
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details set
# MAGIC -- location = 'Bangalore',
# MAGIC -- Geo  = 'US',
# MAGIC -- Position = 'Consultant',
# MAGIC -- Job_Name = 'Senior',
# MAGIC -- Job_Title = 'Senior',
# MAGIC -- Level_wise = 'Analysts to Seniors/Team Leaders ',
# MAGIC -- Original_Levelwise_Mapping ='Analysts to Seniors/Team Leaders ',
# MAGIC -- CC = 'TE-DL-SoftwareEngg',
# MAGIC -- BU = 'Consulting',
# MAGIC -- SL = 'Lighthouse',
# MAGIC -- Global_Function = 'Advisory' where File_Date in ('20221130','20230228') and Local_HR_Id in ('KPMG66011029')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb_badrecords.trusted_hist_lnd_kva_details_bad

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select File_Date,* from kgsonedatadb.trusted_hist_lnd_glms_kva_details where  File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and FIle_Type = 'GLMS' and Local_HR_Id in ('133781','133540') and File_Date in ('20230301')
# MAGIC
# MAGIC
# MAGIC -- select File_Date,* from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('45319','105675')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Local_HR_Id,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details where SL is null and (Cost_Center is null or trim(Cost_Center) ='') and File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') AND File_Type='GLMS';
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Employee_Number in ('45319','105675')
# MAGIC -- select * from kgsonedatadb.trusted_hist_headcount_employee_dump where Employee_Number in ('45319','105675')
# MAGIC
# MAGIC select distinct FIle_Date from 
# MAGIC -- kgsonedatadb.trusted_hist_headcount_employee_dump
# MAGIC kgsonedatadb.trusted_hist_headcount_termination_dump 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Local_HR_Id,Email_Address__User_,Position,Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_kva_details where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') 
# MAGIC and (Position is null or trim(Position) = '')

# COMMAND ----------

# %sql

# update kgsonedatadb.trusted_hist_lnd_glms_kva_details set Training_Category = 'Technology/ Techno-functional' where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and (Training_Category is null or trim(Training_Category) = '')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from  kgsonedatadb.config_bu_mapping_list
# MAGIC
# MAGIC select distinct Training_Category from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and SL is null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into kgsonedatadb.trusted_hist_lnd_glms_kva_details
# MAGIC select * from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select distinct Level_Wise from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430')
# MAGIC
# MAGIC
# MAGIC select distinct Local_Hr_Id,Cost_Center,CC,BU,File_Type,File_Date from kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') 
# MAGIC and BU is null
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 set Level_Wise = 'Analysts to Seniors/Team Leaders ' , Original_Levelwise_Mapping = 'Analysts to Seniors/Team Leaders '  where 
# MAGIC -- File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') and Level_Wise ='Analysts to Seniors/Team Leaders' and Original_Levelwise_Mapping ='Analysts to Seniors/Team Leaders'
# MAGIC
# MAGIC -- DA Core-SS Infra Research - DAS
# MAGIC -- TE-ES-SAP-EPM & Analytics - Consulting
# MAGIC -- Tax-T&L Ops - Tax
# MAGIC
# MAGIC
# MAGIC -- update kgsonedatadb.trusted_hist_lnd_glms_kva_details_bckp_05262023 set BU = 'Consulting' where File_Date in ('20221001','20221031','20221101','20221130','20221201','20221231','20230101','20230131','20230201','20230228','20230301','20230331','20230401','20230430') 
# MAGIC -- and BU is null and File_Type = 'GLMS' and Cost_Center = 'TE-ES-SAP-EPM & Analytics'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.trusted_hist_headcount_monthly_employee_details where Employee_Number in ('133540','133781')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Mapping from kgsonedatadb.config_dim_level_wise

# COMMAND ----------

processName = "talent_acquisition"
table = "trusted_talent_acquisition_kgs_joiners_report"
print(table)
if table == "trusted_talent_acquisition_kgs_joiners_report":
    sampleDF=spark.sql('select Candidate_Identifier,Requisition_Number,Candidate_Full_Name,Candidate_First_Name,Candidate_Middle_Names,Candidate_Last_Name,Display_Name,Candidate_Personal_Email,Candidate_Creation_Date_Time,Candidate_Creation_Date,Candidate_Last_Modified_Date_Time,Candidate_Last_Modified_Date,Created_By_User,_Gender,PWD___Category,Special_Diversity,Veteran,Date_of_Birth17,Date_of_Birth18,Nationality,Contact_Number,Complete_Address,Candidate_Current_Status,Business_Unit,Department,Function1,Sub_Function1,Sub_Function2,Service_Line,Sub_Service_Line___Service_Network,Location,Sub_Location,Client_Geography,Offer_Role_Date_Time,Offer_Role_Date,First_Acceptance_Date_Time,First_Acceptance_Date,Latest_Acceptance_Date_Time,Latest_Acceptance_Date,Planned_Start_Date_Time,Planned_Start_Date,Contract_End_Date_Time,Contract_End_Date,Employee_Category,Candidate_Source_Name_,Candidate_Sub_Source__,Candidate_Source_email_,Recruiter_Source_Type, Recruiter_Sub_Source_,Recruiter_Source_Name_,Recruiter_Source_Email,Referrer_Employee_ID,Application_Source,Application_Sub_Source,Application_Agency_Name,India_Project_Code,Reason_for_Hire,Previous_Employer,Previous_Employer_Country,Total_Work_Experience,Graduation,Graduation_College,Graduation_Date_Time,Graduation_Date,Graduation_City,Post_Graduation,Post_Graduation_College,Post_Graduation_Date_Time,Post_Graduation_Date,Post_Graduation_City,Recruiter_Name,Recruiter_Email,Hiring_Manager_Number,Grade,Job___Position_Code,Skill_Type,Skill_Family,Primary_Skill,Entity,Business_Category,Operating_Unit,User_Type__For_KGS_,Employee_s_Official_Email_ID,Employee_Number,PAN_Card,Buddy_Employee_ID,People_Champion_Employee_ID,HRBP_Employee_ID,Performance_Manager_Name,Performance_Manager_Code,Reporting_Partner_Employee_ID,Offer_Created_By,Offer_Creation_Date_Time,Offer_Creation_Date,Working_Hours,Frequency,Retainer_Time_Commitment,Designation,Relocation_Bonus_Flag,Joining_Bonus_Flag,Current_Referral_Status,Dated_On,File_Date,concat(year(planned_start_date),date_format(planned_start_date,"MM")) as Month_Key from kgsonedatadb.'+table)


    display(sampleDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_date from kgsonedatadb.trusted_hist_employee_engagement_year_end

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct file_date from kgsonedatadb.trusted_hist_headcount_leave_report order by File_Date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct File_Date from kgsonedatadb.trusted_hist_employee_engagement_encore_output order by File_Date

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from kgsonedatadb.config_data_type_cast where Delta_Table_Name='encore_output'
# MAGIC
# MAGIC -- update kgsonedatadb.config_data_type_cast set  Type_Cast_To = 'string' where Delta_Table_Name='encore_output' and column_Name ='Year'

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward
# MAGIC  SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5');
# MAGIC
# MAGIC ALTER TABLE kgsonedatadb.trusted_hist_headcount_monthly_secondee_outward
# MAGIC  DROP COLUMN DoubleConversion;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct File_Date from kgsonedatadb.trusted_hist_headcount_termination_dump

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import functions
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window


currentDf = spark.sql("select distinct LWD from kgsonedatadb.raw_hist_headcount_monthly_contingent_worker_resigned where File_Date = '20200219'")
columnName = 'LWD'
columnName_New = 'UPdated_LWD'
# currentDf=currentDf.withColumn(columnName_New,\
#             when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MM-yy'),'dd-MM-yyyy'))\
#             .when(to_date(currentDf[columnName], 'dd-MMM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MMM-yy'),'dd-MM-yyyy'))\
#             .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd MMM yy'),'dd-MM-yyyy'))\
#             .otherwise(currentDf[columnName]))

currentDf=currentDf.withColumn(columnName_New,\
            when(to_date(currentDf[columnName], 'dd-MM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MM-yy'),'dd-MM-yyyy'))\
            .when(to_date(currentDf[columnName], 'dd-MMM-yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd-MMM-yy'),'dd-MM-yyyy'))\
            .when(to_date(currentDf[columnName], 'dd MMM yy').isNotNull(),from_unixtime(unix_timestamp(currentDf[columnName], 'dd MMM yy'),'dd-MM-yyyy'))\
            .otherwise(currentDf[columnName]))

currentDf.display()



# # Create a DataFrame with the date column
# data = [("20-07-20",)]
# df = spark.createDataFrame(data, ["date_string"])

# # Convert the string column to a TimestampType column
# df = df.withColumn("timestamp", unix_timestamp("date_string", "dd-MM-yy"))

# # Convert the TimestampType column to a string column with the desired format
# df = df.withColumn("formatted_date", from_unixtime("timestamp", "yyyy-MM-dd"))

# # Show the resulting DataFrame
# df.show()



# COMMAND ----------

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(tableName)
print(processName)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit, upper, when

currentdatetime= datetime.now()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_bu_update_table_list 

# COMMAND ----------

buList = spark.sql("Select Actual_BU,Final_BU from kgsonedatadb.config_bu_mapping_list")
buTableList = spark.sql("select * from kgsonedatadb.config_bu_update_table_list where Process_Name = '"+processName+"' and Table_Name = '"+tableName+"'")


display(buList)
display(buTableList.orderBy(col('Process_Name')))

# COMMAND ----------

columnList = buTableList.select("Column_Name").rdd.flatMap(lambda x: x).collect()
print("List ", columnList)

# COMMAND ----------

currentDf = spark.sql("select * from kgsonedatadb.trusted_hist_"+processName + "_" + tableName)

currentDf=currentDf.withColumn("Dated_On", lit(currentdatetime))

display(currentDf)

# COMMAND ----------

for columnName in currentDf.columns:
    if (columnName in columnList):
        print(columnName)


        joinDf = currentDf.join(buList,upper(currentDf[columnName]) == upper(buList['Actual_BU']),"left" )

        joinDf = joinDf.withColumn(columnName,when(joinDf.Final_BU.isNotNull(),col('Final_BU'))\
        .otherwise(joinDf[columnName]))
        
        joinDf = joinDf.drop(*buList.columns)

        display(joinDf.select(columnName).distinct())

# display(joinDf)

# COMMAND ----------

currentDf = joinDf

# COMMAND ----------

# Final BU List
# CF
# Tax
# DAS
# Consulting
# KRC
# Cap-Hubs
# RAS
# GDC
# MS
# Digital Nexus

# COMMAND ----------

# secondment_tracker has different BU like DA - DAS(correct), CH - Cap-Hubs(correct)
# Data issue in kgs_joiners_report - Business_Unit has different values other than BU itself
# l1_visa_tracker BU - DA&S - DAS(correct)
# nl_leaver BU - CH - Cap-Hubs (correct)
# nl_joiner BU - RC,MC - NEED TO CHECK

# RC: RAS
# MC: Consulting
# RAK : Cap-Hubs

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.config_bu_update_table_list
# MAGIC -- kgsonedatadb.config_bu_mapping_list

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct BU from kgsonedatadb.config_cost_center_business_unit
# MAGIC --kgsonedatadb.trusted_hist_lnd_kva_details

# COMMAND ----------

