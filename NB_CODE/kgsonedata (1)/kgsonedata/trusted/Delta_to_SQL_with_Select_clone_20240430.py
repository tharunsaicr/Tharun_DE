# Databricks notebook source
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

print(processName)
print(tableName)

if processName == 'config':
    config_table = processName+"_"+tableName
    config_hist_table = processName+"_hist_"+tableName
    print(config_table)
    print(config_hist_table)
else:
    table = "trusted_"+processName+"_"+tableName
    hist_table = "trusted_hist_"+processName+"_"+tableName
    print(table)
    print(hist_table)

# COMMAND ----------

# DBTITLE 1,Delta Table Details
# %sql
# USE kgsonedatadb;
# SHOW TABLES;

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Database Connection Conifguration
jdbcHostname = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseHostName")
jdbcDatabase = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseName")
jdbcPort = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databasePort")

username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNameUserName")
password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNamePassword")

# Using service principal

# username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="applicationId")
# password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="appregkgs1datasecret")
  
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {  
  "user" : username,  
  "password" : password,  
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"  
}

# COMMAND ----------

# DBTITLE 1,Sh script - create ODBC driver to use cursor for Update/Delete queries ADBR to Az SQL DB
## Commented the below code as Msodbcsql 17 is installed on cluster level - 06/09/2023

# %sh

# curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# sudo apt-get update

# sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

# DBTITLE 1,Create Cursor to execute update/delete queries from ADBR to Az SQL DB
driver = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect(
f'DRIVER={driver};SERVER={jdbcHostname};DATABASE={jdbcDatabase};UID={username};PWD={password}'
)
cursor = conn.cursor()

# COMMAND ----------

# sampleDF=spark.sql('select Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,BU,Service_Line,Service_Network,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'+config_hist_table)
# sampleDF.write \
# .format("jdbc") \
# .mode("overwrite") \
# .option("url",jdbcUrl) \
# .option("dbtable",config_hist_table) \
# .option("user", username) \
# .option("password", password) \
# .save()

# COMMAND ----------

# DBTITLE 1,Config
if processName == "config":

    if tableName == "cc_bu_sl":
        sampleDF=spark.sql('select `Function`,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,BU,Service_Line,Service_Network,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'+config_table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(config_hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",config_hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ config_hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",config_hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ config_hist_table)
        
    else:
        sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+config_table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{config_hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(config_hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",config_hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ config_hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",config_hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ config_hist_table)
        

# COMMAND ----------

# if processName == "config":
#     config_table = processName+"_"+tableName
#     print(config_table)

#     if tableName == "cc_bu_sl":
#         sampleDF=spark.sql('select Function,Employee_Subfunction,Employee_Subfunction_1,Cost_centre,Client_Geography,BU,Service_Line,Dated_On from kgsonedatadb.'+config_table)
#         sampleDF.write \
#         .format("jdbc") \
#         .mode("overwrite") \
#         .option("truncate", "true") \
#         .option("url",jdbcUrl) \
#         .option("dbtable",config_table) \
#         .option("user", username) \
#         .option("password", password) \
#         .save()

#     elif tableName == "admin_cc_bu_sl":
#         sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key  from kgsonedatadb.'+config_table)
#         sampleDF.write \
#         .format("jdbc") \
#         .mode("overwrite") \
#         .option("truncate", "true") \
#         .option("url",jdbcUrl) \
#         .option("dbtable",config_hist_table) \
#         .option("user", username) \
#         .option("password", password) \
#         .save()
#     else:
#         sampleDF=spark.sql('select * from kgsonedatadb.'+config_table)
#         sampleDF.write \
#         .format("jdbc") \
#         .mode("overwrite") \
#         .option("truncate", "true") \
#         .option("url",jdbcUrl) \
#         .option("dbtable",config_table) \
#         .option("user", username) \
#         .option("password", password) \
#         .save()
        

# COMMAND ----------

# DBTITLE 1,JML Table
if processName == "jml":

    # dbutils.notebook.exit(0)
    if table == "trusted_jml_germany_joiner":
        sampleDF=spark.sql('select SAP_ID,Name_of_Joiner,Function,Request_Received_on,Form_sent_to_Germany_Team,SAP_ID_Created_On,No_of_days_taken_to_complete_the_process,Initiator_of_request_from_JML_Team,Initiator_of_request_from_HR_Business_SPOC,Effective_Onboarding_Date,Remarks,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_germany_leaver":
        sampleDF=spark.sql('select SAP_ID,Name_of_employee,Function,LWD_of_the_employee,Request_Received_on,Form_sent_to_Germany_Team,SAP_ID_Terminated_On,No_of_days_taken_to_complete_the_process,Initiator_of_request_from_JML_Team,Initiator_of_request_from_HR_Business_SPOC,Remarks,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_germany_mover":
        sampleDF=spark.sql('select SAP_ID,Name_of_employee,Function,Type_of_request,Request_Received_on,Form_sent_to_Germany_Team,Request_completed_on,No_of_days_taken_to_complete_the_process,Initiator_of_request_from_JML_Team,Initiator_of_request_from_HR_Business_SPOC,Remarks,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_nl_joiner":
        sampleDF=spark.sql('select India_Emp_ID,NL_ID,BU,First_Name,Last_Name,Start_Date,Initiator_of_the_request,`Date:__BU_HR_to_JML_`,`Date:__JML_to_NL_`,`Date:__NL_to_JML_`,Total_No__of_days_taken_to_complete_the_process,Request_Status,Reason_for_delay,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_nl_leaver":
        sampleDF=spark.sql('select KGS_Emp_Id,NL_ID,Employee_Name,BU,Initiator_of_the_request,LWD_of_the_user,`Date:__BU_HR_to_JML_`,`Date:__JML_to_NL_Team_`,`Date:__NL_to_JML_Team_`,Total_No__of_days_taken_to_complete_the_process,Network_Days_from_LWD_till_request_was_sent_to_JML,Reason_for_Delay,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_nl_mover":
        sampleDF=spark.sql('select India_Emp_ID,NL_ID,BU,Employee_Name,Initiator_of_the_request,`Date:__BU_HR_to_JML_`,`Date:__JML_to_NL_`,`Date:__NL_to_JML_`,Total_No__of_days_taken_to_complete_the_process,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_uk_joiner":
        sampleDF=spark.sql('select ID,Priority,Employee_ID,Candidate_India_ID,Start_Date,First_Name,Last_Name,Pay_Grade,Employee_Class,Standard_Weekly_Hours,Contract_Activity,Job_Title,Capability,Cost_centre,Cost_Centre_Description,Retain_Capability,Retain_Service_Line_Code,Retain_Service_Line_Description,Location,Employee_Status,Previous_staff_number,Gender,Date_of_Birth,PML_Staff_Number,PML_Name,IT_Sub_function,Type_of_Employee,Profile_Type,Additional_Applications,Status,GPID,UK_Staff_ID,Login_ID,Password,Notes,Email,Client,VDI_Link,UK_MI_Resource,Comments,Created,Created_By,HR_Date,JML_Date,People_Center_Date,ITS_Date,MI_Date,Position_ID,Dated_On,File_Date,ED_Employee_Number,ED_Full_Name,ED_Function,ED_Employee_Subfunction,ED_Employee_Subfunction_1,ED_Cost_centre,ED_Client_Geography,ED_Position,ED_GPID,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_uk_leaver":
        sampleDF=spark.sql('select ID,Priority,UK_Staff_ID as Title,First_Name,Last_Name,Leave_Date,UK_Email_Address,Reason_For_Leaving,`Action_Required?`,UK_People_Center,Status,Comments,HR_Date,JML_Date,People_Center_Date,Created_By,Position_ID,Dated_On,File_Date,ED_Employee_Number,ED_Full_Name,ED_Function,ED_Employee_Subfunction,ED_Employee_Subfunction_1,ED_Cost_centre,ED_Client_Geography,ED_Position,ED_UK_Employee_Number,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_uk_mover":
        sampleDF=spark.sql('select ID,Process_Type,Priority,Effective_Date,UK_Staff_ID,First_Name,Last_Name,Request_Type,New_Capability,New_Cost_Centre,Absence_End_Date,Absence_Type,New_Retain_Capability,New_Retain_Service_Line_Code,New_Retain_Service_Line_Description,ITS_Sub_Function_to_be_revoked,ITS_Sub_Function_to_be_provided,DMS_environment_worksite_has_been_removed,Profile_Type,New_Pay_Grade,New_Employee_Class,New_Standard_Weekly_Hours,New_Work_Contract,New_Location,Job_Title,PML_Staff_Number,Secondment_End_Date,Additional_Applications,Status,PML_Staff_Name,UK_People_Center,UK_Resourcing_MI,Global_Mobility_team,HR_Date,JML_Date,People_Center_Date,MI_Date,ITS_Date,GM_Date,Created_By,Modified,Existing_Position_ID,New_Position_ID,Existing_Cost_Centre,Capability,Dated_On,File_Date,ED_Employee_Number,ED_Full_Name,ED_Function,ED_Employee_Subfunction,ED_Employee_Subfunction_1,ED_Cost_centre,ED_Client_Geography,ED_Position,ED_UK_Employee_Number,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_us_joiner":
        sampleDF=spark.sql('select Magnet_ID,Fusion_Ticket,Fusion_Ticket_Create_Date,Personal_KGS_email_ID,User_5_Access,Position,Function,`Date:__BU_HR_to_JML_`,`Date:__JML_to_US_`,`Date:__US_to_JML_`,Remarks,Initiator_of_request_from_JML_Team,Requester_Name,Requester_Email_ID,India_Emp_ID,First_name,Last_name,Start_date,PSID,IID,US_E_mail_Address,DSID,SecurID_Activation_Code,Product_Code,Total_No__of_days_taken_to_complete_the_process,US_Login_details_date,Date_sent_to_Reconnect_team,Dated_On,File_Date,ED_Employee_Number,ED_Full_Name,ED_Function,ED_Employee_Subfunction,ED_Employee_Subfunction_1,ED_Cost_centre,ED_Client_Geography,ED_Position,ED_US_Employee_Number,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_jml_us_leaver":
        sampleDF=spark.sql('select Emp_Id,PS_ID,Employee_Name,Function,Initiator_of_the_request,Initiator_of_request_from_JML_Team,LWD_of_the_user,`Date:__BU_HR_to_JML_`,`Date:__JML_to_US_`,Confirmation_date_for_Pro_Team,Confirmation_date_for_Kteck__Team,Total_No__of_days_taken_to_complete_the_process,Days_from_LWD_to_date_when_termination_request_is_initiated,Reason_for_delay,Date_sent_to_Reconnect_team,S_No,Wand_Req_ID,Engagement_ID,Fusion_Ticket,Dated_On,File_Date,ED_Employee_Number,ED_Full_Name,ED_Function,ED_Employee_Subfunction,ED_Employee_Subfunction_1,ED_Cost_centre,ED_Client_Geography,ED_Position,ED_US_Employee_Number,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        
    if table == "trusted_jml_us_mover":
        sampleDF=spark.sql('select Fushion_Ticket,PS_ID,Employee_Name,Type_of_Request,Description,Initiator_of_the_request,Initiator_of_request_from_JML_Team,`Date:__BU_HR_to_JML_`,`Date:__JML_to_US_`,`Date:__US_to_JML_`,Total_No__of_days_taken_to_complete_the_process,Count,Dated_On,File_Date,ED_Employee_Number,ED_Full_Name,ED_Function,ED_Employee_Subfunction,ED_Employee_Subfunction_1,ED_Cost_centre,ED_Client_Geography,ED_Position,ED_US_Employee_Number,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
# else:
# sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

# COMMAND ----------

# DBTITLE 1,lnd
if processName == "lnd":
    import datetime
    from pyspark.sql.functions import concat

    month_list = [11,12]
    current_date = datetime.datetime.now()
    current_month = current_date.month
    current_year = current_date.year

    # dbutils.notebook.exit(0)

    if table == "trusted_lnd_glms_kva_details":
        # sampleDF=spark.sql('select Item_ID,Training_Category,Item_Title,KBS_Classification_,Item_Type,Assignment_Type,Security_Domain__Item_,cast(CPD_CPE_Hours__Item_ as float) as CPD_CPE_Hours__Item_,Item_Revision_Date,Class_ID,Scheduled_Offering_Start_Date,SO_Item_Start_Time,Class_End_Date,Scheduled_Offering_End_Time,Completion_Date,Period,Completion_Time,Completion_Status_ID,Completion_Status_Description,cast(Total_Hours as float) as Total_Hours,cast(Credit_Hours as float) as Credit_Hours,cast(Contact_Hours as float) as Contact_Hours,cast(CPD_CPE_Hours_Awarded as float) as CPD_CPE_Hours_Awarded,Primary_Instructor,Last_Update_User_ID,Last_Update_Timestamp,Last_Update_Time,Comments,Local_HR_ID,First_Name,Last_Name,Middle_Name,Email_Address__User_,User_ID,Account_ID,User_Status,Job_Code_ID,Job_Code_Description,Job_Title,Position,Job_Name,Level_Wise,Country_Region,Office_Location_ID,Department,Global_Function,Organisation_Description,CC,BU,Subfunction,Employee_Class_Description,Local_Job_Level,Is_Client_Facing,Manager_Email_Address,Last_Hire_Date,Local_Function,Local_Service_Line,Cost_Center,ORG_ID,Geo,SL,Location,Item_Type_Category,Full_Name,Quarter,Original_Levelwise_Mapping,File_Type,Dated_On,replace(Completion_Date,"-","") as File_Date,left(replace(Completion_Date,"-",""),6) as Month_Key from kgsonedatadb.'+table)

        sampleDF=spark.sql('select Item_ID,Training_Category,Item_Title,KBS_Classification_,Item_Type,Assignment_Type,Security_Domain__Item_,cast(CPD_CPE_Hours__Item_ as float) as CPD_CPE_Hours__Item_,Item_Revision_Date,Class_ID,Scheduled_Offering_Start_Date,SO_Item_Start_Time,Class_End_Date,Scheduled_Offering_End_Time,Completion_Date,Period,Completion_Time,Completion_Status_ID,Completion_Status_Description,cast(Total_Hours as float) as Total_Hours,cast(Credit_Hours as float) as Credit_Hours,cast(Contact_Hours as float) as Contact_Hours,cast(CPD_CPE_Hours_Awarded as float) as CPD_CPE_Hours_Awarded,Primary_Instructor,Last_Update_User_ID,Last_Update_Timestamp,Last_Update_Time,Comments,Local_HR_ID,First_Name,Last_Name,Middle_Name,Email_Address__User_,User_ID,Account_ID,User_Status,Job_Code_ID,Job_Code_Description,Job_Title,Position,Job_Name,Level_Wise,Country_Region,Office_Location_ID,Department,Global_Function,Organisation_Description,CC,BU,Subfunction,Employee_Class_Description,Local_Job_Level,Is_Client_Facing,Manager_Email_Address,Last_Hire_Date,Local_Function,Local_Service_Line,Cost_Center,ORG_ID,Geo,SL,Location,Item_Type_Category,Full_Name,Quarter,Original_Levelwise_Mapping,File_Type,Dated_On,replace(Completion_Date,"-","") as File_Date,left(replace(Completion_Date,"-",""),6) as Month_Key from kgsonedatadb.trusted_hist_lnd_glms_kva_details where completion_date >"2023-09-30" and left(Dated_On,10) ="2024-04-15"')
        
        sampleDF = sampleDF.dropDuplicates()

        # month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        if current_month in month_list:
            month_key = str(current_year)+"10"
            print(month_key)
        else:
            month_key = str(current_year-1)+"10"
            print(month_key)

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key >= '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        print(result)

        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key >= '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    # else:
    #     sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

# COMMAND ----------

# DBTITLE 1,Global Mobility
if processName == "global_mobility":

    if table == "trusted_global_mobility_l1_visa_tracker" :
        sampleDF=spark.sql('select S_No,Emp_ID,PSID,Travel_Model,Employee_Name,Visa_Category,SPOC,Business_Unit,Program,Entity,Case_Status,Remarks,Final_Case_Status,Email_ID,KGS_Title,Home_City,KPMG_Experience,Prev_Work_Experience,Total_Experience,US_Title,Expected_Start_Date,Expected_End_Date,Expected_Duration_of_Travel,Business_Nomination_Date,Pre_assessment__Form_Recd_Date,IAI_sent_to_secondee_date,IAI_received_for_review_date,IAI_reviewed_and_replied_date,Time_taken_to_review_the_IAI,IAI_final_version_received_date,Time_taken_by_the_secondee_to_submit_the_final_IAI_version,IAI_sent_to_US_by_India_GM_date,Time_taken_by_IN_GM_to_send_the_IAI_to_US_GM,Overall_time_from_IAI_sent_to_secondee_to_US,US_Offer_received_date,Time_taken_by_the_US_GM_to_share_the_offer_letter,Offer_letter_shared_to_secondee,Offer_accepted_Date,Time_taken_by_secondee_to_accept_Offer,Received_Fragomen_Questionniare,Time_taken_by__Fragomen_to_send__questionnaire,Completed_Fragomen_Questionnaire,Time_taken_by__secondee_to_submit_the_questionnaire,DS_160_Instructions_recd_from_Fragomen,DS_160_submitted_online,Time_taken_by__secondee_to_submit_DS_160,Petition_received_on,Time_taken_by__Fragomen_to_send_Petition,Interview_Date,Interview_Outcome,Visa_Start_Date,Visa_End_Date,Petition_End_Date,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_global_mobility_non_us_tracker" :
        sampleDF=spark.sql('select S_No,Emp_ID,Travel_Model,Employee_Name_PS,Country,City,Visa_Category,Business_Unit,Service_Line,Entity,Case_Status,Remarks,Final_Case_Status,Email_ID,Home_City,Start_Date,End_Date,Duration_of_Travel__Days_,Duration_of_Travel__Months_,Initiation_Date,Offer_Received_Date,Offer_Acceptance_Date,Completion_Date,Visa_Start_Date,Visa_End_Date,RP_Start_Date,RP_End_Date,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_global_mobility_opportunity_dump" :
        sampleDF=spark.sql('select Job_Req_ID,Country,Number_of_Positions,Host_Global_Mobility_Professional_First_name,Host_Global_Mobility_Professional_Last_Name,Recruiter_Host_HR_Last_Name,Recruiter_Host_HR_First_Name,GM_Point_of_Contact__Primary_GMP__Last_Name,GM_Point_of_Contact__Primary_GMP__First_name,Anticipated_Assignment_Start_Date,City,Job_Title,Type_of_Package__If_known_,Host_Function,Host_Service_Line,Global_Job_Level,Local_Job_Level,Market_Sector,Level_Of_Accountability,Host_Cost_Center,Duration,Region,Sub_Region,Status,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_global_mobility_secondment_tracker" :
        sampleDF=spark.sql('select S_No,Secondment_Status,Travel_Model,Employee_No,Employee_Name,Inbound___Outbound,Employee_Category_updated,Business_Unit,India_Entity,Home_Country,Home_City,Host_Country,Host_City,cast(Assignment_Start_Date as date) as Assignment_Start_Date,cast(Assignment_End_Date as date) as Assignment_End_Date,Remarks,Additional_Comments,HRMS_Designation_,India_Email_ID,Host_Email_ID,Personal_Email_ID,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_global_mobility_secondment_details" :
        #  Added 3 more columns Personal_Email_ID,Host_Email_ID,India_Email_ID
        sampleDF=spark.sql('select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Business_Category,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job,People_Group,Employee_Category,cast(Date_First_Hired as date) as Date_First_Hired, cast(End_Date_for_FTS as date) as End_Date_for_FTS,Gender,Company,Supervisor_Name,Performance_Manager,Email,cast(Start_Date as date) as Start_Date, cast(End_Date as date) as End_Date,Home_Country,Host_Country,Secondment_Type_Long_or_Short,Inbound___Outbound,Active_or_concluded,Comments_Mobility,File_Date,Dated_On,left(File_Date,6) as Month_Key,Personal_Email_ID,Host_Email_ID,India_Email_ID from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    # else:
    #     sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

# COMMAND ----------

# DBTITLE 1,TA_New
pushdown_query = "(select * from [dbo].[HRSS_Payroll_cutoff_date])HRSS_Payroll_cutoff_date"
HRSS_Payroll_cutoff_Date_df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)

HRSS_Payroll_cutoff_Date_df = spark.sql('''select *,concat(year(Cutoff_Date),date_format(Cutoff_Date,"MM")) as Month_Name_new from kgsonedatadb.trusted_curr_talent_acquisition_hrss_payroll_cutoff_date''')

if processName == "talent_acquisition":
    # FYFlagHCCutOff, FYFlagHCCutOffMonth

    print(table)
    if table == "trusted_talent_acquisition_kgs_joiners_report":
        sampleDF = spark.sql('select SNO, EmplNo, FullName, CandidateID, TaleoID, Costcentre, BU, ClientGeography, Location, SubLocation, Position, HCMapping, HClevel, JobName, PeopleGroupName, EmployeeCategory, DateFirstHired, Week, MonthAsPerWeek, FYAsPerWeek, Gender, Candidate_Email, Recruiter, Campus_Lateral, Source_Corrected, NameofSubSource_Corrected, Status, Joined, Drops, ConfirmedStart, ClientGeographyValueActual, PreviousEmployer, PreviousEmployerType, TotalExp, TypeofHire, DropReasons, DropReasons_Cleaned, Performance_Manager_Code, Buddy_Employee_Code, HRBP_Code, People_Champion_Code, Performance_Manager_Name, Buddy_Employee_Name, HRBP_Name, People_Champion_Name, Performance_Manager_Email, Buddy_Employee_Email, HRBP_Email, People_Champion_Email, Candidate_Contact_Number, FYFlagHCCutOff, FYFlagHCCutOffMonth, FYFlagHCCutOffQuarter, Hiring_Manager_Name, HighestEductaionQualification_Cleaned, Education_Bucket, Relocation_Bonus_Flag, Employee_s_Official_Email_ID, Offer_Role_Date as Offer_Roll_Date, Dated_On, File_Date from kgsonedatadb.'+table)

        sampleDF = sampleDF.withColumn("FYFlagHCCutOffMonth_new",from_unixtime(unix_timestamp(col("FYFlagHCCutOffMonth"),'MMM'),'MM'))

        sampleDF = sampleDF.withColumn("Month_Key",concat(substring('DateFirstHired',0,2),substring(regexp_replace("FYFlagHCCutOff","FY",""),5,6),'FYFlagHCCutOffMonth_new'))

        sampleDF = sampleDF.withColumn("Month_Key",when(sampleDF.FYFlagHCCutOffMonth_new.isin(['10','11','12']),concat(substring('DateFirstHired',0,2),substring("FYFlagHCCutOff",4,2),'FYFlagHCCutOffMonth_new')).otherwise(concat(substring('DateFirstHired',0,2),substring("FYFlagHCCutOff",7,2),'FYFlagHCCutOffMonth_new'))
        )

        sampleDF = sampleDF.drop('FYFlagHCCutOffMonth_new')

        # To delete and load
        df_min_max_datefirst_hired = spark.sql('select min(DateFirstHired) as Min_DateFirstHired, max(DateFirstHired) as Max_DateFirstHired from kgsonedatadb.'+table)

        min_datefirsthired = df_min_max_datefirst_hired.select('Min_DateFirstHired').distinct().rdd.map(lambda x:x[0]).first()
        max_datefirsthired = df_min_max_datefirst_hired.select('Max_DateFirstHired').distinct().rdd.map(lambda x:x[0]).first()
        
        print(min_datefirsthired,max_datefirsthired)

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where DateFirstHired >= '{min_datefirsthired}' and DateFirstHired <= '{max_datefirsthired}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        print(result)

        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where DateFirstHired >= '{min_datefirsthired}' and DateFirstHired <= '{max_datefirsthired}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted data successfully between ' + min_datefirsthired + ' and ' + max_datefirsthired +' in SQL Database')

            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries reloaded successfully between ' + min_datefirsthired + ' and ' + max_datefirsthired +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully between ' + min_datefirsthired + ' and ' + max_datefirsthired +' in '+ hist_table)
        
        # sampleDF.display()
        # sampleDF.write \
        # .format("jdbc") \
        # .mode("overwrite") \
        # .option("truncate", "true") \
        # .option("url",jdbcUrl) \
        # .option("dbtable",hist_table) \
        # .option("user", username) \
        # .option("password", password) \
        # .save()

    if table == "trusted_talent_acquisition_requisition_dump":
        sampleDF=spark.sql('''select ReqIdentifier, Open_Positions, DepartmentName, BU, TemplateJobCode, HCLevel, LevelCategory, ReqTitle, CityName, CurrentStatus, RecruiterName, HiringManagerName, LatestFullyApprovedDate, ClientGeographyValue, Justification, StaffingType, SkillType, SkillFamily, PrimarySkill, Is_Posted_Externally, Is_Posted_Internally,  Dated_On,File_Date ,concat(year(LatestFullyApprovedDate),date_format(LatestFullyApprovedDate,"MM")) as new_Date from kgsonedatadb.'''+table)
        # sampleDF.display()
        sampleDF = sampleDF.withColumn('LatestFullyApprovedDate', to_date(col('LatestFullyApprovedDate'),'yyyy-MM-dd'))
        sampleDF = sampleDF.join(HRSS_Payroll_cutoff_Date_df,sampleDF.new_Date == HRSS_Payroll_cutoff_Date_df.Month_Name_new,"left")
        sampleDF = sampleDF.withColumn("add_month_key",when((sampleDF.LatestFullyApprovedDate > sampleDF.Cutoff_Date), add_months(sampleDF.LatestFullyApprovedDate,1)) \
                                                        .otherwise(sampleDF.LatestFullyApprovedDate))
                                                        
        sampleDF = sampleDF.withColumn('Month_Key',date_format(sampleDF['add_month_key'],"yyyyMM"))
        # columns_to_drop = ['add_month_key']
        # sampleDF.select('LatestFullyApprovedDate','Cutoff_Date','new_Date','month_key')
        sampleDF = sampleDF.drop('add_month_key','new_Date','Month_Name','Cutoff_Date','Month_Name_new')

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_talent_acquisition_requisition_raised":
        sampleDF=spark.sql('select ReqIdentifier, Total_Positions, DepartmentName, BU, TemplateJobCode, HCLevel, LevelCategory, ReqTitle, CityName, CurrentStatus, RecruiterName, HiringManagerName, LatestFullyApprovedDate, ClientGeographyValue, Justification, StaffingType, SkillType, SkillFamily, PrimarySkill, Is_Posted_Externally, Is_Posted_Internally, FYFlagHCCutOff, FYFlagHCCutOffMonth, FYFlagHCCutOffQuarter, Dated_On, File_Date from kgsonedatadb.'+table)

        # sampleDF.display()

        sampleDF = sampleDF.withColumn("FYFlagHCCutOffMonth_new",from_unixtime(unix_timestamp(col("FYFlagHCCutOffMonth"),'MMM'),'MM'))
        sampleDF = sampleDF.withColumn("Month_Key",
                                    when(sampleDF.FYFlagHCCutOffMonth_new.isin(['10','11','12']),concat(substring('LatestFullyApprovedDate',0,2),substring("FYFlagHCCutOff",4,2),'FYFlagHCCutOffMonth_new'))\
                                    .otherwise(concat(substring('LatestFullyApprovedDate',0,2),substring("FYFlagHCCutOff",7,2),'FYFlagHCCutOffMonth_new'))
                                    )
        # sampleDF.select('FYFlagHCCutOffMonth','FYFlagHCCutOffMonth_new',"FYFlagHCCutOff","Month_Key").display()

        sampleDF = sampleDF.drop('FYFlagHCCutOffMonth_new')
        
        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    # else:
    #     sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

# COMMAND ----------

# DBTITLE 1,TA_Post_Hire
if processName == "TA_post_hire":
    if table == "trusted_TA_post_hire_FTE":
        sampleDF=spark.sql('select Req__Identifier,Candidate_Identifier,Candidate_Name,Level,Department_Name,BU,Geo,Email_ID,Recruiter_Name,Project_Code,Source,Sub_Source,Employee_Category,Original_Start_Date,Contract_End_Date,Joining_Location,Sub_Location,Location_Category,Exception_Approval_To_On_Board,ZONE__For_Delhi__NCR_,Phone_Number,Current_Address_With_landmark_where_Laptop_Can_be_Delivered,City_and_State,Pin_Code,Can_come_to_office_for_Laptop_Collection,_when_can_they_come_to_office,`Have_you_returned_from_overseas_in_last_14_days_or_have_been_in_self__advised_quarantine?__YES___NO_`,Address___Phone_Number_Verified_with_Each_Candidate,JML_initiation_request_sent_Yes_No_,Offer_status,Gender,Offer_Extend_date,Performance_Manager,Performance_Manager_Code,Buddy_Employee,Buddy_Code,Buddy_Email_ID,HRBP,HRBP_Code,HRBP_Email_ID,Candidate_s_KPMG_Email_ID,Status,Remarks,Hiring_Manager_code,LWD_from_ED,BGV_Reference_No_,BGV_Status,Report_Colour,Mandatory_Checks_Completed,All_Checks_Completed,Case_Remarks,Onboarding_Approval,Approval_Date_HRSS,Approval_Date_COO,Final_Joining_Status,Reason,Updated_DOJ,Neetu_s_Attendance,Dated_On,File_Date,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        
        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted records successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_TA_post_hire_CWK":
        sampleDF=spark.sql('select Req__Identifier,Candidate_Identifier,Candidate_Name,Designation,Cost_Centre,BU,Geo,Recruiter_Name,Email,Hiring_Source,Project_Code,Start_Date,Joining_Status,Reason,Joining_Location,Sub_Location,Location_Category,Exception_Approval_To_On_Board,ZONE__For_Delhi__NCR_,Phone_Number,Current_Address_With_landmark_where_Laptop_Can_be_Delivered,Candidate_Category__ROD_or_RPO_,Remarks,KGS_Email_ID,Status,Updated_DOJ,BGV_Reference_No_,BGV_Status,Report_Colour,Mandatory_Checks_Completed,All_Checks_Completed,Case_Remarks,Onboarding_Approval,Approval_Date,Final_Joining_Status,Dated_On,File_Date,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        
        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted records successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)


# COMMAND ----------

# DBTITLE 1,BGV - Upcoming Joiners
if processName == "bgv":

    print(table)

    if table == "trusted_bgv_offer_release":
        # dbutils.notebook.exit(0)
        sampleDF=spark.sql('''select Candidate_Identifier,Candidate_Full_Name,Candidate_Email,
        CASE when (trim(Offered_Created_Date) == '' or trim(Offered_Created_Date) == '-')  then NULL else Offered_Created_Date END as Offered_Created_Date,Offer_Extended_Date,Offer_Accepted_Date,CASE when (trim(Start_Date) == '' or trim(Start_Date) == '-')  then NULL else Start_Date END as Start_Date,
        Offer_Status,Current_Step_Name,Business_Unit___Name,Req__Identifier,Work_Location,Cost_Center,Client_Geography,Entity_Name,India_Project_Code,Employee_Category,Recruiter_Name,City,Source_on_Offer_Page,Source_on_Candidate_Page,Sub_Source_on_Candidate_Page,Offer_Template_Used,Offer_Sent_BY,Designation,Job_Position_Code,CTC_Offered,Function,Gender,Is_Most_Recent,Dated_On,File_Date,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_bgv_progress_sheet":
        sampleDF=spark.sql('''select S_No,Client_Name,Reference_No_,Candidate_s_Name,Personal_Email_ID,Candidate_Official_email_,Candidate_Contact_Number,DOB,Father_s_Name,New_cases_old_cases,
        CASE when (trim(Case_Initiation_Date) == '' or trim(Case_Initiation_Date) == '-')  then NULL else Case_Initiation_Date END as Case_Initiation_Date,
        DOJ_DOR,Status,Checks_Name,Reopen_Date,KI_DUE_Date,Over_All_Status_Drop_Down_,Case_Status_Drop_Down_,Sub_Status_Drop_Down_,Case_Dependency,EDU_1,EDU2,EDU3,EMP1,EMP2,EMP3,EMP4,EMP5,ADD1,ADD2,ADD3,ADD4,ADD5,CRI1,CRI1_Closure_Date,CRI2,CRI2_Closure_Date,CRI3,CRI4,CRI5,REF_1,REF_2,REF3,Database1,Database1_Closure_Date,Database2,Database2_Closure_Date,Database_3,Database_4,Identity_Check_1,WIP,Insufficiency,UTV,Inaccessible,CEA,Discrepancy,Stop_check,Overall_Mandatory_Checks_Closure_Date,First_Level_Insufficiency_Intimate_Date,First_Level_Insufficiency_Remarks,First_Level_Insufficiency_Closure_Date,Second_Level_Insufficiency_Intimate_Date,Second_Level_Insufficiency_Remarks,Second_Level_Insufficiency_Closure_Date,Open_Insuff,Insufficiency_Handover_Date_KI_to_KGS,Insufficiency_Closure_Intimation_Date_to_KI,Insufficiency_Closure_Date__KGS_,Final_Report_date,Final_Report_Color_Code_Drop_Down_,FR_Remarks_For_Non_Green_Report,Supplimentry_Report_Date,Supplimentry_Report_Color_Code_Drop_Down_,SR_Remarks_For_Non_Green_Report,latest_color_code,CEA__Stop_Check_Initiation_Date,CEA_ClosureDate,All_Checks_Completed_Drop_Down_,Suspicious_Category_Employment_Education_,EDU_EMP_Name__Suspicious_,UTV_Inaccessible_remarks,Discrepancy_remarks,Last_Component_Closer_date,Dated_On,File_Date,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)


    if table == "trusted_bgv_kcheck":
        sampleDF=spark.sql('select Initiated_On,Created_By,Client,External_Status,KPMG_Ref_No,First_Name,Middle_Name,Sur_Name,Email,Father_s_Name,DOB,`Inv#`,Dated_On,File_Date,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_bgv_waiver_tracker":
        sampleDF=spark.sql('''select S_N_,Candidate_Name,BGV_Ref_no,Candidate_Email_ID,Joining_Status,DOJ,Designation,Business_Unit,Cost_Centre,Recruiter_Name,
        CASE when (trim(Report_Date) == '' or trim(Report_Date) == '-')  then NULL else Report_Date END as Report_Date,
        Report_Color,Discrepant_Component,Case_Remarks,Case_Status,Comments_Wrt_to_waiver,Clarification_Received_Date,Waiver_Sent_Date___Risk,Waiver_Received_Date___Risk,Risk_Approval_Status,Waiver_Sent_Date___HRBP_TA_Head,Waiver_Received_Date___HRBP_TA_Head,HR_Approval_Status,Case_Manager,Waiver_Open_Closed,Dated_On,File_Date,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_bgv_upcoming_joiners":

        sampleDF=spark.sql('''select Candidate_Identifier,BU,Candidate_Name,`Candidate_Email/Candidate_Email_Personal_Email`,Candidate_Alternate_Email_Candiddate_Official_Email,
        CASE when (trim(Start_Date) == '' or trim(Start_Date) == '-')  then NULL else Start_Date END as Start_Date,
        Recruiter_Name,BGV_Reference_No,
        CASE when (trim(BGV_Initiation_Date) == '' or trim(BGV_Initiation_Date) == '-')  then NULL else BGV_Initiation_Date END as BGV_Initiation_Date,
        BGV_Status,Report_Colour,Mandatory_Checks_Completed,All_Checks_Completed,Can_be_Onboarded_Yes_or_No,Waiver_Applicable,Waiver_Status_Closed_Yes_or_No,Insuff_Remarks,UTV_Remarks,Discrepancy_remarks,Found_in_Suspicious_List,Work_Location,Cost_Center,Client_Geography,Designation,CRI1,CRI2,CRI3,CRI4,CRI5,DTB_1,DTB_2,DTB_3,DTB_4,File_Date,Dated_On,concat(year(Start_Date),date_format(Start_Date,"MM")) as Month_Key from (select rank() over(partition by `Candidate_Email/Candidate_Email_Personal_Email` order by File_Date desc, dated_on desc) as rank, * from kgsonedatadb.'''+table+''') hist where rank = 1''')

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_bgv_cwk_employee_dump":
        sampleDF=spark.sql("select * ,left(File_Date,6) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()  

    
    if table == "trusted_bgv_cwk_termination_dump":
        sampleDF=spark.sql("select * ,left(File_Date,6) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save() 

    if table == "trusted_bgv_cwk_rekonnect_sheet":
        sampleDF=spark.sql("select * ,left(File_Date,6) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_bgv_ki_loaned_new_joiners_data":
        sampleDF=spark.sql("select * ,left(File_Date,6) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_bgv_insuff_component":
        sampleDF=spark.sql("select * ,left(File_Date,6) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_bgv_client_check_data":
        sampleDF=spark.sql("select * ,left(File_Date,6) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_bgv_cea":
        sampleDF=spark.sql("select S_N_,CASE_REFERENCE_NUMBER,CANDIDATE_S_NAME,PERSONAL_EMAIL_ID,CANDIDATE_CONTACT_NUMBER,CASE_INITIATION_DATE,EMPLOYER_NAME,STATUS,UPDATED_DOJ, KGS_STATUS,RL_RECEIVED_YES_NO,CEA_STOP_CHECK_INITIATION_DATE,CASE_REMARKS,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_bgv_insufficiency":
        sampleDF=spark.sql("select S_N_,REFERENCE_NO_,CANDIDATE_NAME,MAIL_ID,CONTACT_NUMBER,CASE_INITIATION_DATE,INSUFFICIENCY_REMARKS,L1__L2,DATE_OF_JOINING,KGS_STATUS,INSUFFICIENCY_CLOSURE_DATE,CASE_REMARK,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_bgv_joined_candidate_holiday_list":
        sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    # else:
    #     sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

# COMMAND ----------

# DBTITLE 1,BGV_JOINED_CANDIDATE
if processName == "bgv_joined_candidate":
    print("inside")
    if table == "trusted_bgv_joined_candidate_fte_fts":
        sampleDF=spark.sql("select EMPLOYEE_ID,BGV_CASE_REFERENCE_NUMBER,CANDIDATE_FULL_NAME,COST_CENTRE,CLIENT_GEOGRAPHY,DESIGNATION,LOCATION,CANDIDATE_EMAIL,PLANNED_START_DATE,RECRUITER,STATUS,EMPLOYEE_CATEGORY,SOURCE,NAME_OF_SUB_SOURCE,BU,OFFICIAL_EMAIL_ID_PREVIOUS,OFFER_RELEASE_DATE,OFFER_ACCEPTANCE_DATE,INDIA_PROJECT_CODE,HRBP_NAME_PREVIOUS,PERFORMANCE_MANAGER_PREVIOUS,SUPERVISOR_NAME_PREVIOUS,DOI_PREVIOUS,BGV_STATUS_FINAL_PREVIOUS,FR_PREVIOUS,SR_PREVIOUS,OVER_ALL_PREVIOUS,MANDATE_CHECKS_COMPLETED,ALL_CHECKS_COMPLETED_PREVIOUS,WAIVER_APPLICABLE_PREVIOUS,WAIVER_CLOSED_PREVIOUS,BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_PREVIOUS,CANDIDATE_IDENTIFIER,ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER_PREVIOUS,BGV_COMPLETION_DATE_PREVIOUS,SUSPICIOUS_COMPANY_PREVIOUS,EMPLOYEE_STATUS,OFFICIAL_EMAIL_ID,HRBP_NAME,PERFORMANCE_MANAGER,SUPERVISOR_NAME,INSUFF_REMARKS,INSUFF_COMPONENTS,DOI_NEW,DUE_DATE,AGEING_FROM_DOO,AGEING_FROM_DOI,AGEING_FROM_DOJ,AGEING_BUCKET,DOJ_MONTH,AGEING_FROM_DOJ_CALENDAR_DAY,AGEING_BUCKET_CALENDAR_DAY,BGV_STATUS_SELF,REMARKS,RESPONSIBILITY,BGV_STATUS_PROGRESS_SHEET,FR_SUPPORT_FOR_NEW,SR_SUPPORT_FOR_NEW,BGV_STATUS_FINAL,FR_OUTPUT,SR_OUTPUT,OVER_ALL_OUTPUT,ALL_CHECKS_COMPLETED_NEW,CEA_LATEST_REPORT_COLOR,WAIVER_APPLICABLE_NEW,WAIVER_CLOSED_NEW,BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION ,ALL_CHECKS_COMPLETED_ALONG_WITH_WAVIER,BGV_COMPLETION_DATE_NEW,LAST_GREEN_COMPLETE_DATE,SUSPICIOUS_COMPANY_NEW,BGV_EXCEPTION_Y_N,WAIVER_REMARKS,FINAL_REPORT_DATE,PREVIOUS_YES,ADD_CHECKS,Dated_On,File_Date,left(File_Date,8) as Month_Key from kgsonedatadb."+table)
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == "trusted_bgv_joined_candidate_ki_loaned":
        sampleDF=spark.sql("select * ,left(File_Date,8) as Month_Key from kgsonedatadb."+table)
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    if table == "trusted_bgv_joined_candidate_cwk":
        sampleDF=spark.sql("select * ,left(File_Date,8) as Month_Key from kgsonedatadb."+table)
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    if table == "trusted_bgv_joined_candidate_consolidated":
        sampleDF=spark.sql("select EMPLOYEE_ID,BGV_CASE_REFERENCE_NUMBER,CANDIDATE_FULL_NAME,COST_CENTRE,CLIENT_GEOGRAPHY,DESIGNATION,LOCATION,PLANNED_START_DATE,RECRUITER,EMPLOYEE_CATEGORY,BU,MANDATE_CHECKS_COMPLETED,ALL_CHECKS_COMPLETED_ALONG_WITH_WAIVER,BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION,HRBP_NAME,PERFORMANCE_MANAGER,INSUFF_REMARKS,INSUFF_COMPONENTS,AGEING_BUCKET,BGV_COMPLETION_DATE_NEW,BGV_STATUS,REMARKS,RESPONSIBILITY,SUSPICIOUS_COMPANY_NEW,SOURCE,BGV_EXCEPTION_Y_N,Dated_On,File_Date,left(File_Date,8) as Month_Key from kgsonedatadb."+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

# COMMAND ----------

# DBTITLE 1,TRANSPORT (Admin)
if processName == "transport":
    
    if table == "trusted_transport_on_time":
        sampleDF=spark.sql('''select  `DATE`,NAME,`EMP#` AS EMP,MOBILE,ALTERNATECONTACTNO,GENDER,CATEGORY,BUSINESSUNIT,COSTCENTER,OFFICE,SHIFTTIME,SPOC,MANAGER,TRIPID,ROUTETYPE,TRIPTYPE,ROUTENAME,`ROUTE#`,VEHICLETYPE,VENDOR,VEHICLENUMBER,DRIVER,DRIVER_MOBILE,ZONE,LOCALITY,PICKUPPOINT,SEQUENCE,ISFIRSTEMPLOYEE,BOARDING_TIME_PLANNED,VEHICLE_ARRIVAL_TIME__ACTUAL,CHECKINTIME,CHECKINMODE,CHECK_OUT_TIME,CHECKOUTMODE,NOSHOW,NOSHOWMARKEDBY,NOSHOWTIME,NOSHOW_CANCELLATION,DELAY_MINS,OFFICE_ARRIVAL_TIME,OTAWITHBUFFER,OTASHIFTTIME,DELAYEDLESSTHAN15MINS,DELAYEDBY,OTA_BUFFER,DISTANCE_TRAVELLED,TSREACHTIME,TSREACHPOIDISTANCE_KM,TSLEAVETIME,TSSKIPTIME,TSNOSHOW,CALL_TIME,INPUT_OF_TRACKER,COMMENTS,TRACKER_NAME,DTT_ETA,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table in ('trusted_transport_login_report_bangalore','trusted_transport_login_report_kochi'):
        hist_table = 'trusted_hist_transport_login_report'
        sampleDF=spark.sql('''select `DATE`,OFFICE,BU,NAME,EMPLOYEE_ID,MOBILE,ALTERNATE_CONTACT,GENDER,SPECIAL_EMPLOYEE,CATEGORY,COST_CENTER,SHIFT_TIME,BUFFER_TIME__MINS_,ROSTER_TYPE,ROSTER_MODE,SPOC,MANAGER,TRIP_ID,ROUTE_TYPE,ROUTE_NAME,ROUTE_NUMBER,VEHICLE_TYPE,VEHICLE_NUMBER,DRIVER,DRIVER_NUMBER,LOCALITY,PICKUP_POINT,SEQUENCE,BOARDING_TIME__PLANNED_,CAB_ARRIVAL_TIME__ACTUAL_,EMPLOYEE_DELAY,OFFICE_ARRIVAL_TIME,OTA__DELAY_,NOSHOW_MARKED_BY,NOSHOW,CHECKIN_TIME,CHECKIN_DELAY_STATUS,CHECKIN_MODE,CHECKIN_DISTANCE,CHECKIN_DELAY,DELAYED_BY,CHECKOUT_TIME,CHECKOUT_MODE,GUARD_CHECKIN_TIME,GUARD_CHECKIN_MODE,DIST_TRAVELLED,EXPECTED_COST,ACTUAL_COST,VENDOR,NO_SHOW_TIME,NO_SHOW_STATUS,SHIFT_DATETIME,COSTCENTER_CODE,TRIP_DELAY_STATUS,ISELIGIBLEFORPAYOUT,DESIGNATION,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table in ('trusted_transport_logout_report_bangalore','trusted_transport_logout_report_kochi'):
        hist_table = 'trusted_hist_transport_logout_report'
        sampleDF=spark.sql('''select `DATE`,OFFICE,BU,NAME,EMPLOYEE_ID,MOBILE,ALTERNATE_CONTACT,GENDER,SPECIAL_EMPLOYEE,CATEGORY,COST_CENTER,SHIFT_TIME,BUFFER_TIME__MINS_,ROSTER_TYPE,ROSTER_MODE,SPOC,MANAGER,TRIP_ID,ROUTE_TYPE,ROUTE_NAME,ROUTE_NUMBER,VEHICLE_TYPE,VEHICLE_NUMBER,DRIVER,DRIVER_NUMBER,LOCALITY,DROP_POINT,SEQUENCE,DROP_TIME__PLANNED_,CAB_ARRIVAL_TIME__ACTUAL_,EMPLOYEE_DELAY,OFFICE_DEPARTURE_TIME,OTD__DELAY_,NOSHOW_MARKED_BY,NOSHOW,CHECKIN_TIME,CHECKIN_DELAY_STATUS,CHECKIN_MODE,CHECKIN_DISTANCE,CHECKIN_DELAY,DELAYED_BY,CHECKOUT_TIME,CHECKOUT_MODE,GUARD_CHECKOUT_TIME,GUARD_CHECKIN_MODE,DIST_TRAVELLED,EXPECTED_COST,ACTUAL_COST,VENDOR,`SLOT_#`,NO_SHOW_TIME,NO_SHOW_STATUS,SHIFT_DATETIME,COSTCENTER_CODE,TRIP_DELAY_STATUS,ISELIGIBLEFORPAYOUT,DESIGNATION,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_transport_fleet_details":
        sampleDF=spark.sql('''select VEHICLEREGNO,BILLINGTAG,VEHICLETYPE,CAPACITY,FLEETOWNER,TRACKEENAME,	INSURANCEEXPIRYDATE,PERMITEXPIRYDATE,FCEXPIRYDATE,VEHICLESTATUS,SHIFTTYPE,SITE,STATUS,	REGISTRATIONDATE,VEHICLEMAKE,VEHICLEMODEL,ROUTENUMBER,BASIC,CHASSISNO,ROADTAXVALIDITY,PUCEXPDATE,INSURANCEAGENCY,	VERIFICATIONBV,VERIFICATIONPV,INDUCTION,OFFICELOCATION,VEHICLENO,OWNERNAME,REGISTRATIONEXPDATE,	FIREEXTINGUISHERDATE,FIRSTAIDKITDATE,ATTRITION,REMARKS,COSTPERTRIP,COSTPERKM,DEVICE,DRIVER,	DRIVERCONTACTNO1,DRIVERCONTACTNO2,LICENSENO,OWNERADDRESS,OWNER,ENGINECAPACITY,CCPERMITDATE,	GPSINSTALLEDDATE,ISGPSINSTALLED,PASSENGERTAXDATE,VALIDTOKENTAXDATE,ISCNGFITTED,ISCNGFITTEDENDROSEDINRC,	ISMANDATORYSIGNAGE,ISMOBILEDEVICEINSTALLED,MOBILEDEVICEID,MOBILETRACKEEID,TRACKEEID,	KMSRUNBEFOREONBOARDING,DOCUMENTCOUNT,DOCUMENTNAMES,AIRCONDITIONED,FUELTYPE,STANDARD,DEACTIVATEDAT,	DEACTIVATEDBY,DEACTIVATIONREMARKS,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    
    if table == "trusted_transport_vehicle_operating_fleet_details":
        sampleDF=spark.sql('''select  VENDOR,VEHICLE_TYPE,VEHICLE_NUMBER,ELECTRIC,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_transport_roster_details":
        sampleDF=spark.sql('''select DATE,DAY,NAME,`EMP#`,COST_CENTRE,MOBILE,ALTERNATECONTACTNO,GENDER,CATEGORY,BUSINESSUNIT,BUSINESSUNIT_1,COSTCENTER,OFFICE,SHIFTTIME,SPOC,MANAGER,TRIPID,ROUTETYPE,TRIPTYPE,ROUTENAME,`ROUTE#`,VEHICLETYPE,VENDOR,VEHICLENUMBER,DRIVER,DRIVER_MOBILE,ZONE,LOCALITY,PICKUPPOINT,SEQUENCE,ISFIRSTEMPLOYEE,BOARDING_TIME_PLANNED_,VEHICLE_ARRIVAL_TIME__ACTUAL_,CHECKINTIME,CHECKINMODE,CHECK_OUT_TIME,CHECKOUTMODE,NOSHOW,NOSHOWMARKEDBY,NOSHOWTIME,NOSHOW_CANCELLATION,NOSHOW_CANCELLATION_1,DELAY_MINS_,OFFICE_ARRIVAL_TIME,OTAWITHBUFFER,OTASHIFTTIME,DELAYEDLESSTHAN15MINS,DELAYEDBY,OTA_BUFFER,DISTANCE_TRAVELLED,TSREACHTIME,TSREACHPOIDISTANCE_KM_,TSLEAVETIME,TSSKIPTIME,TSNOSHOW,CALL_TIME,INPUT_OF_TRACKER,COMMENTS,TRACKER_NAME,DTT_ETA,REMARKS,REMARKS_1,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

# COMMAND ----------

# DBTITLE 1,EE 
if processName == "employee_engagement":
    print(table)

    import datetime
    from pyspark.sql.functions import concat

    month_list = [10,11,12]
    # current_date = datetime.datetime.now()
    # current_month = current_date.month
    # current_year = current_date.year

    if table == "trusted_employee_engagement_thanks_dump":
        sampleDF=spark.sql('select Reference_Id,Reward_Status,Reward_Name,Nomination_Date,Reward_Amount_in_Accounting_Currency__INR_,Budget_From,Nominee_s_Name,Nominee_s_Email,Nominee_s_Employee_ID,Nominee_s_Status,Nominee_s_Location,Nominee_s_City,Nominee_s_Unit,Nominee_s_Department,Nominee_s_Designation,Nominee_s_Division,Nominee_s_Function,Nominee_s_Region,Nominee_s_Legal_Entity,Nominee_s_Employee_Band,Team_Name,Team_Members,Nominator_s_Name,Nominator_s_Email,Nominator_s_Employee_ID,Nominator_s_Status,Nominator_s_Location,Nominator_s_City,Nominator_s_Unit,Nominator_s_Department,Nominator_s_Designation,Nominator_s_Division,Nominator_s_Function,Nominator_s_Region,Nominator_s_Legal_Entity,Nominator_s_Employee_Band,Actual_Nominator_s_Name,Actual_Nominator_s_Employee_Id,Actual_Nominator_s_Email,Pending_Approval_With__Name_,Pending_Approval_With__Emp_Id_,Pending_Approval_With__Email_,Reward_Date,First_Level_Approver_Name,First_Level_Approver_Employee_Id,First_Level_Approver_Email,Second_Level_Approver_Name,Second_Level_Approver_Employee_Id,Second_Level_Approver_Email,Reject_Date,Rejected_By__Name_,Rejected_By__Emp_Id_,Rejected_By__Email_,Deleted_Date,Deleted_By__Name_,Deleted_By__Emp_Id_,Deleted_By__Email_,Proxy_By__Name_,Proxy_By__Emp_Id_,Proxy_By__Email_,Reward_Redemption_Type,Reward_Label,Budget_Calendar,Financial_Year,Reward_Attributes,Citation,First_Level_Approval_Remarks,First_Level_Approval_Date,First_Level_Rejection_Date,Second_Level_Approval_Remarks,Second_Level_Approval_Date,Second_Level_Rejection_Date,Nomination_Form_Fields,Dated_On,File_Date,left(replace(Nomination_Date,"-",""),6) as Month_Key from kgsonedatadb.'+table)

        # month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        file_date = sampleDF.select('File_Date').distinct().rdd.map(lambda x:x[0]).first()
        current_month = int(file_date[4:6])
        current_year = int(file_date[0:4])
        print(current_month)
        print(current_year)
        
        if current_month in month_list:
            month_key = str(current_year)+"10"
            print(month_key)
        else:
            month_key = str(current_year-1)+"10"
            print(month_key)

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key >= '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key >= '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_employee_engagement_encore_output":

        sampleDF=spark.sql('select Emp_No,Final_Name,Designation,BU,Nominee_s_Unit,Category,Reward_Status,Quarter,Year,Email_ID,Level,Gender,Gratuity_Date,Company_Name,Budget_From,Nominator_s_Employee_ID,Reward_Date,Nomination_Date,File_Date,Dated_On,left(replace(Nomination_Date,"-",""),6) as Month_Key from kgsonedatadb.'+table)
        print(sampleDF.count())
        
        # month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        file_date = sampleDF.select('File_Date').distinct().rdd.map(lambda x:x[0]).first()
        current_month = int(file_date[4:6])
        current_year = int(file_date[0:4])
        print(current_month)
        print(current_year)
        
        if current_month in month_list:
            month_key = str(current_year)+"10"
            print(month_key)
        else:
            month_key = str(current_year-1)+"10"
            print(month_key)

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key >= '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key >= '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_employee_engagement_gps":
        sampleDF=spark.sql('select Employee_Number,Full_Name,Email_Address,Global_Job_Level,Global_Function,Employment_Type,Age,Full_time_or_Part_time,Gender,DOJ,Gratuity_Date,Tenure_Length_of_Service,Is_a_High_Performer,Is_a_Performance_Manager,Is_a_People_Leader,Is_Client_Facing,Has_Taken_Parental_Leave_in_the_Past_3_Years,Recently_Promoted_last_24_months,Cost_Centre,BU,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_employee_engagement_rock":
        sampleDF=spark.sql('select Employee_Number,Full_Name,Function,Sub_Function,Cost_centre,Final_BU as BU,Location,Position,Employee_Category,Gratuity_Date,Date_First_Hired,Email_Address,Company_Name,desired_LWD,Approved_LWD,RM_Employee_Name,Rock_Tenure,CAST(Amount as decimal),Tenure_Month,Tenure_Year,Tenure_as_on,File_Date,Dated_On,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    # Commented below post go-live of Comp&Ben as YE is moved to Iso Environment

    # if table == "trusted_employee_engagement_year_end":
    #     sampleDF=spark.sql('''select Emp_ID,Full_name,Function,Sub_function,Cost_center,BU,Location,Designation,Job_Code,Date_as_of,Pitched_at__Current_Positioning_,Date_since_at_current_designation,Years_at_current_designation_as_on_30_Sep_2023,DOJ,Years_in_the_Firm_as_on_30_Sep_2023,Work_experience_prior_to_KPMG__Years_,Total_years_of_work_experience_as_on_30_Sep_2023,RATING_OCT_2022_TO_SEP_2023_,RATING_OCT_2021_TO_SEP_2022_,RATING_OCT_2020_TO_SEP_2021_,RATING_OCT_2019_TO_SEP_2020_,RATING_OCT_2018_TO_SEP_2019_,Post_Graduation_Highest_Qualification,Graduation,Gender,Email,Emp_Category,`Include_in_rating_distribution?`,no_of_days_taken_in_current_year,Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_,Distribution_category,Performance_manager_name,`Proposed_Rating__Oct_22_-_Sep_23`,Promotion_recommendation_Yes_or_No_,Encore_winner,Comments_on_Promotion_Exceptions,BP_comments,Rating_Jump_or_Dip,PD_comments,HR_head_comments,Promotion_category,Promotion_approved,New_Cost_Center,New_Designation,New_job_code,New_pitching,Client_Geo,Operating_unit,Effective_date,Team_Band_as_per_CP_document,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        
    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("append") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

# COMMAND ----------

# DBTITLE 1,Headcount Weekly
if processName == "headcount":

    print(table)
    if table == "trusted_headcount_contingent":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_contingent_worker":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_contingent_worker_resigned":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_headcount_employee_details":

        sampleDF = spark.sql('''select * from kgsonedatadb.'''+table)
        columnList = ('Business_Category')
        BusinessCategoryAvailable = 'No'

        for columnName in sampleDF.columns:
            if (columnName in columnList):
                BusinessCategoryAvailable = 'Yes'
   

        if BusinessCategoryAvailable.upper() == 'YES':
            sampleDF=spark.sql('''select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,Remarks,'' as Current_Base_Location_of_the_Candidate,'' as Office_Location_in_the_KGS_Offer_Letter,'' as WFA_Option__Permanent_12_Months_,Dated_On,File_Date,Business_Category,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)
        else:
            sampleDF=spark.sql('''select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,Remarks,'' as Current_Base_Location_of_the_Candidate,'' as Office_Location_in_the_KGS_Offer_Letter,'' as WFA_Option__Permanent_12_Months_,Dated_On,File_Date,'' as Business_Category,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        # sampleDF=spark.sql("select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,Date_First_Hired,End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,Remarks,Current_Base_Location_of_the_Candidate,Office_Location_in_the_KGS_Offer_Letter,WFA_Option__Permanent_12_Months_,Dated_On,File_Date,Business_Category,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_employee_dump":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        
        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()

        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_headcount_leave_report":
        sampleDF=spark.sql('''select *,concat(year(leave_start_date),date_format(leave_start_date,"MM")) as Month_Key from kgsonedatadb.'''+hist_table)
        
        # sampleDF=spark.sql('''select *,concat(year(leave_start_date),date_format(leave_start_date,"MM")) as Month_Key from kgsonedatadb.'''+table+''' where file_Date = "20230522"''')

        # sampleDF=spark.sql('''select *,concat(year(leave_start_date),date_format(leave_start_date,"MM")) as Month_Key from kgsonedatadb.'''+hist_table+''' where coalesce(upper(Cancel_Status),"NO") not like "YES%" and (coalesce(upper(leave_type),"NA") not like "%KI%") and coalesce(upper(leave_type),"NA") not in ("ILLNESS OR INCAPACITY LEAVE","AUDIT SQL LEAVE")''')

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == "trusted_headcount_loaned_staff_from_ki":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_loaned_staff_resigned":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_headcount_maternity_cases":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)

        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517','20230522','20230529')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_resigned_and_left":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_sabbatical":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230828')")

        

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_secondee_outward":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    # if table == "trusted_headcount_talent_konnect_resignation_status_report":
    #     # sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)

    #     sampleDF=spark.sql('''select EMPLOYEENUMBER,FULLNAME,CONTACTNUMBER,Personal_Email_ID,REQUESTSTATUS,FUNCTION,SUB_FUNCTION,ORGANIZATIONNAME,COMPANY_NAME,JOB,LOCATION,RESIGNATIONDATE,POLICYLWD,No_of_days_Waved, 
    #     case when APPROVED_LWD = '1900-01-01' then null else APPROVED_LWD end as APPROVED_LWD, 
    #     LWD_DESIREDLWD,
    #     case when APPROVED_LWD = '1900-01-01' then LWD_DESIREDLWD else APPROVED_LWD end as COMPUTED_LWD,
    #     CLEARANCE_COMPLETE_DATE,TERMINATION_DATE,HCM_TERMINATION__DATE,PM_EMP_CODE,PERFORMANCEMANAGERNAME,Reporting_Partner,Reporting_Partner_Approval_Date,WITHDRAWAL_PENDING_WITHPM,LWD_PENDING_WITH_PARTNER_NAME,NO_DUES_CERTIFICATE_SENT,NO_DUES_CERTIFICATE_SENT_DATE,NO_DUES_CERTIFICATE_RECEIVED_FROM_EX_EMPLOYEE,NO_DUES_CERTIFICATE_RECEIVED_FROM_EX_EMPLOYEE_DATE,SERVICE_LETTER_SENT_TO_EX_EMPLOYEE,SERVICE_LETTER_SENT_TO_EX_EMPLOYEE_DATE,RESIGNATION_TYPE,EXIT_REASON,ONBEHALF_RESIGNATION,HRBP_EMP_Code_,HRBP_Name,Dated_On,File_Date,left(date_format(cast(resignationdate as date),'yyyyMMdd'),6) as Month_Key from kgsonedatadb.'''+table+''' where COMPANY_NAME IN ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited') and coalesce(resignationdate,"NA") != "NA"''')

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("truncate", "true") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()
    
    if table == "trusted_headcount_termination_dump":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_headcount_academic_trainee":
        sampleDF=spark.sql('select *,left(File_Date,8) as Month_Key from kgsonedatadb.'+table)
        sampleDF = sampleDF.drop("Remarks")
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb."+hist_table+" where File_Date in ('20230517')")
        # sampleDF=spark.sql("select *,left(File_Date,8) as Month_Key from kgsonedatadb.trusted_hist_headcount_academic_trainee where File_Date in ('20230517')")

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_attrition_data":
        year= spark.sql('select distinct YEAR from kgsonedatadb.trusted_headcount_attrition_data')
        fy=year.select('YEAR').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Employee_Number, Employee_Name, Function, Sub_Function, Sub_Function1, Profit_Centre, Client_Geography, Location, Sub_Location, Positions, Job, Date_First_Hired, Gender, Employee_Category, Entity_Name, Month, BU_Aligned, KGS_Levels, Service_Network, Revised_SL, Termination_Date, Leaving_Reason, Data, Year, Att_Type, Tenure, Vol, Invol, HC, MS_FTS_Emps_tag, Att_match, Tenure_Category, Options, Aligned_Geo, Attrition_Tenure, HC_Tenure, Geo1, Geo2, Level, Quarter, Qualified_Non_Qualified, Highest_Qualification, Education_Group, Prior_Experience, Maternity, Sabbatical, Nationality, Rating, Supervisor_Name, Performance_Manager, IGH_Name, Age_Category, Dated_On, File_Date,DATE_FORMAT(MONTH,'yyyyMM') as Month_Key from kgsonedatadb."""
            + table + """ WHERE UPPER(Att_Type) like '%VOL%'"""
        )
        display(sampleDF)

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)
    # else:
    #     sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

    # sampleDF.write \
    #   .format("jdbc") \
    #   .mode("append") \
    #   .option("url",jdbcUrl) \
    #   .option("dbtable",hist_table) \
    #   .option("user", username) \
    #   .option("password", password) \
    #   .save()

# COMMAND ----------

# DBTITLE 1,Resignation Dump
if (processName == "headcount") and (table == "trusted_headcount_talent_konnect_resignation_status_report"):

    print(table)

    # Get Resignation data where resignationdate is not null
    resignationDf = spark.sql("select * from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report where COMPANY_NAME IN ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited') and coalesce(resignationdate,'NA') != 'NA'")
   
    # Get cutoff Data
    cutoffDf = spark.sql("select distinct DATE_FORMAT(Cutoff_Date, 'MMM-yy') AS Month_Name,Cutoff_Date from kgsonedatadb.config_hist_hrss_payroll_cutoff_date order by Cutoff_Date asc")


    resignationDf =resignationDf.withColumn("Current_ResignationMonthYear",date_format(to_date(resignationDf['RESIGNATIONDATE'], 'yyyy-MM-dd'),"MMM-yy"))


    resignationDf =resignationDf.withColumn("Next_ResignationMonthYear",calculateNextMonthUDF(col('Current_ResignationMonthYear'),lit(1),lit('%b-%y'),lit('%b-%y')))


    joinResginationDf = resignationDf.join(cutoffDf,resignationDf.Current_ResignationMonthYear == cutoffDf.Month_Name,"left").select(resignationDf['*'],cutoffDf['Cutoff_Date']).withColumnRenamed("Cutoff_Date","Current_Cutoff_Date")
    

    joinDf = joinResginationDf.join(cutoffDf,joinResginationDf.Next_ResignationMonthYear == cutoffDf.Month_Name,"left").withColumnRenamed("Cutoff_Date","Next_Cutoff_Date").drop('Month_Name')

    joinDf =joinDf.withColumn("CurrentMonthKey",date_format(to_date(joinDf['Current_Cutoff_Date'], 'yyyy-MM-dd'),"yyyyMM"))

    joinDf =joinDf.withColumn("NextMonthKey",date_format(to_date(joinDf['Next_Cutoff_Date'], 'yyyy-MM-dd'),"yyyyMM"))

    
    joinDf = joinDf.withColumn("Month_Key",when((joinDf.RESIGNATIONDATE > joinDf.Current_Cutoff_Date) & (joinDf.RESIGNATIONDATE < joinDf.Next_Cutoff_Date) , joinDf.NextMonthKey ).otherwise(joinDf.CurrentMonthKey))



    joinDf.createOrReplaceTempView("vw_trusted_headcount_talent_konnect_resignation_status_report")


    
    sampleDF=spark.sql('''select EMPLOYEENUMBER,FULLNAME,CONTACTNUMBER,Personal_Email_ID,REQUESTSTATUS,`FUNCTION`,SUB_FUNCTION,ORGANIZATIONNAME,COMPANY_NAME,JOB,LOCATION,RESIGNATIONDATE,POLICYLWD,No_of_days_Waved,

    case when APPROVED_LWD = '1900-01-01' then null else APPROVED_LWD end as APPROVED_LWD,

    LWD_DESIREDLWD,

    case when APPROVED_LWD = '1900-01-01' then LWD_DESIREDLWD else APPROVED_LWD end as COMPUTED_LWD,

    CLEARANCE_COMPLETE_DATE,TERMINATION_DATE,HCM_TERMINATION__DATE,PM_EMP_CODE,PERFORMANCEMANAGERNAME,Reporting_Partner,Reporting_Partner_Approval_Date,WITHDRAWAL_PENDING_WITHPM,LWD_PENDING_WITH_PARTNER_NAME,NO_DUES_CERTIFICATE_SENT,NO_DUES_CERTIFICATE_SENT_DATE,NO_DUES_CERTIFICATE_RECEIVED_FROM_EX_EMPLOYEE,NO_DUES_CERTIFICATE_RECEIVED_FROM_EX_EMPLOYEE_DATE,SERVICE_LETTER_SENT_TO_EX_EMPLOYEE,SERVICE_LETTER_SENT_TO_EX_EMPLOYEE_DATE,RESIGNATION_TYPE,EXIT_REASON,ONBEHALF_RESIGNATION,HRBP_EMP_Code_,HRBP_Name,Dated_On,File_Date, Month_Key from vw_trusted_headcount_talent_konnect_resignation_status_report''')
    
    

    sampleDF.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("truncate", "true") \
    .option("url",jdbcUrl) \
    .option("dbtable",hist_table) \
    .option("user", username) \
    .option("password", password) \
    .save()

# COMMAND ----------

# DBTITLE 1,Resignation Dump - Fortnight
if (processName == "headcount") and (table == "trusted_headcount_talent_konnect_resignation_status_report_fortnight"):

    print(table)

    # Get Resignation data where resignationdate is not null
    resignationDf = spark.sql("select * from kgsonedatadb.trusted_headcount_talent_konnect_resignation_status_report_fortnight where COMPANY_NAME IN ('KPMG Global Services Management Private Limited','KPMG Global Services Private Limited','KPMG Resource Centre Private Limited','KPMG Global Delivery Center Private Limited') and coalesce(resignationdate,'NA') != 'NA'")
   
    # Get cutoff Data
    cutoffDf = spark.sql("select distinct DATE_FORMAT(Cutoff_Date, 'MMM-yy') AS Month_Name,Cutoff_Date from kgsonedatadb.config_hist_hrss_payroll_cutoff_date order by Cutoff_Date asc")


    resignationDf =resignationDf.withColumn("Current_ResignationMonthYear",date_format(to_date(resignationDf['RESIGNATIONDATE'], 'yyyy-MM-dd'),"MMM-yy"))


    resignationDf =resignationDf.withColumn("Next_ResignationMonthYear",calculateNextMonthUDF(col('Current_ResignationMonthYear'),lit(1),lit('%b-%y'),lit('%b-%y')))


    joinResginationDf = resignationDf.join(cutoffDf,resignationDf.Current_ResignationMonthYear == cutoffDf.Month_Name,"left").select(resignationDf['*'],cutoffDf['Cutoff_Date']).withColumnRenamed("Cutoff_Date","Current_Cutoff_Date")
    

    joinDf = joinResginationDf.join(cutoffDf,joinResginationDf.Next_ResignationMonthYear == cutoffDf.Month_Name,"left").withColumnRenamed("Cutoff_Date","Next_Cutoff_Date").drop('Month_Name')

    joinDf =joinDf.withColumn("CurrentMonthKey",date_format(to_date(joinDf['Current_Cutoff_Date'], 'yyyy-MM-dd'),"yyyyMM"))

    joinDf =joinDf.withColumn("NextMonthKey",date_format(to_date(joinDf['Next_Cutoff_Date'], 'yyyy-MM-dd'),"yyyyMM"))

    
    joinDf = joinDf.withColumn("Month_Key",when((joinDf.RESIGNATIONDATE > joinDf.Current_Cutoff_Date) & (joinDf.RESIGNATIONDATE < joinDf.Next_Cutoff_Date) , joinDf.NextMonthKey ).otherwise(joinDf.CurrentMonthKey))



    joinDf.createOrReplaceTempView("vw_trusted_headcount_talent_konnect_resignation_status_report_fortnight")


    
    sampleDF=spark.sql('''select EMPLOYEENUMBER,FULLNAME,CONTACTNUMBER,Personal_Email_ID,REQUESTSTATUS,`FUNCTION`,SUB_FUNCTION,ORGANIZATIONNAME,COMPANY_NAME,JOB,LOCATION,RESIGNATIONDATE,POLICYLWD,No_of_days_Waved,

    case when APPROVED_LWD = '1900-01-01' then null else APPROVED_LWD end as APPROVED_LWD,

    LWD_DESIREDLWD,

    case when APPROVED_LWD = '1900-01-01' then LWD_DESIREDLWD else APPROVED_LWD end as COMPUTED_LWD,

    CLEARANCE_COMPLETE_DATE,TERMINATION_DATE,HCM_TERMINATION__DATE,PM_EMP_CODE,PERFORMANCEMANAGERNAME,Reporting_Partner,Reporting_Partner_Approval_Date,WITHDRAWAL_PENDING_WITHPM,LWD_PENDING_WITH_PARTNER_NAME,NO_DUES_CERTIFICATE_SENT,NO_DUES_CERTIFICATE_SENT_DATE,NO_DUES_CERTIFICATE_RECEIVED_FROM_EX_EMPLOYEE,NO_DUES_CERTIFICATE_RECEIVED_FROM_EX_EMPLOYEE_DATE,SERVICE_LETTER_SENT_TO_EX_EMPLOYEE,SERVICE_LETTER_SENT_TO_EX_EMPLOYEE_DATE,RESIGNATION_TYPE,EXIT_REASON,ONBEHALF_RESIGNATION,HRBP_EMP_Code_,HRBP_Name,Dated_On,File_Date, Month_Key from vw_trusted_headcount_talent_konnect_resignation_status_report_fortnight''')

    file_date = sampleDF.select('File_Date').distinct().rdd.map(lambda x:x[0]).first()
    query = f"SELECT COUNT(*) FROM dbo.{hist_table} where File_Date = '{file_date}'"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    
    if result > 0:
        query = f"DELETE FROM dbo.{hist_table} where File_Date = '{file_date}'"
        conn.execute(query)
        conn.commit()
        print(hist_table + ' deleted successfully for ' + file_date +' in SQL Database')
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Enteries loaded successfully for month ' + file_date +' in '+ hist_table)
    else:
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Enteries loaded successfully for month ' + file_date +' in '+ hist_table)

# COMMAND ----------

# DBTITLE 1,Headcount Monthly - Need to use Case WHen for Date fields or Use other Scripts in Same folder
if processName == "headcount_monthly":
    if table == "trusted_headcount_monthly_contingent_worker":
        sampleDF=spark.sql('''select Candidate_Id,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_Centre,Operating_Unit,Client_Geography,User_Type,'' as Business_Category,Location,Sub_Location,Job,'' as People_Group15,
        CASE when Date_of_Joining == ' ' then NULL else Date_of_Joining END as Date_of_Joining,
        Subcontract_Agency,Official_Email_ID,Assignment_Category,Staff_Type,BU,File_Date,File_Month,File_Year,Dated_On,Position,People_Group,Gender,Company,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)


        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_monthly_contingent_worker_resigned":
        sampleDF=spark.sql('''select Candidate_Id,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,Client_Geography,User_Type,Location,Sub_Location,Job,Position,People_Group,
        CASE when (Date_of_Joining == ' ' or Date_of_Joining == '-') then NULL else Date_of_Joining END as Date_of_Joining ,
        Gender,Company,Subcontract_Agency,
        CASE when LWD== ' ' then NULL else LWD END as LWD,
        Official_Email_ID,Assignment_Category,BU,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)



    if table == "trusted_headcount_monthly_employee_details":
        sampleDF=spark.sql('''select Employee_Number,Full_Name,`Function`,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,
        CASE when (Date_First_Hired == ' ' or Date_First_Hired == '-') then NULL else Date_First_Hired END as Date_First_Hired,
        CASE when (End_Date == ' ' or End_Date == '-') then NULL else End_Date END as End_Date,
        Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,'' as Remarks,Current_Base_Location_of_the_Candidate,Office_Location_in_the_KGS_Offer_Letter,WFA_Option__Permanent_12_Months_,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_monthly_loaned_staff_from_ki":
        sampleDF=spark.sql('''select Employee_Number,Employee_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_Centre,Operating_Unit,Company,Sub_Location,Client_Geo,
        CASE when (Start_Date== ' ' or Start_Date== '-') then NULL else Start_Date END as Start_Date,
        CASE when (End_Date== ' ' or End_Date== '-' ) then NULL else End_Date END as End_Date,
        Position_Name,Email_Id,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_monthly_loaned_staff_resigned":
        sampleDF=spark.sql('''select Employee_Number,Employee_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_Center,Operating_Unit,Company,Sub_Location,Client_Geo,
        CASE when (Start_Date== ' ' or Start_Date== '-') then NULL else Start_Date END as Start_Date,
        CASE when (End_Date== ' ' or End_Date== '-' ) then NULL else End_Date END as End_Date,Position_Name,
        CASE when (LWD == ' ' or LWD == '-') then NULL else LWD END as LWD,
        File_Date,Dated_On,left(File_Date,6) as Month_Key,File_Month,File_Year from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_headcount_monthly_resigned_and_left":
        sampleDF=spark.sql('''select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Cost_Centre,User_Type,Client_Geography,Position,Location,Sub_Location,Job_Name,
        CASE when (Date_First_Hired == ' ' or Date_First_Hired == '-') then NULL else Date_First_Hired END as Date_First_Hired,
        CASE when (Termination_Date == ' ' or Termination_Date == '-') then NULL else Termination_Date END as Termination_Date,
        Gender,Employee_Category,People_Group_Name,Company_Name,Email_Address,BU,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_monthly_sabbatical":
        sampleDF=spark.sql('''select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,
        CASE when (Date_First_Hired == ' ' or Date_First_Hired == '-') then NULL else Date_First_Hired END as Date_First_Hired,
        End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,
        CASE when (Leave_Start_Date == ' ' or Leave_Start_Date == '-') then NULL else Leave_Start_Date END as Leave_Start_Date,
        CASE when (Leave_End_Date == ' ' or Leave_End_Date == '-') then NULL else Leave_End_Date END as Leave_End_Date
        ,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    
    if table == "trusted_headcount_monthly_secondee_outward":
        sampleDF=spark.sql('''select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,
        CASE when (Date_First_Hired == ' ' or Date_First_Hired == '-') then NULL else Date_First_Hired END as Date_First_Hired,
        End_Date,Gender,Company_Name,Supervisor_Name,Performance_Manager,Email_Address,BU,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
    
    if table == "trusted_headcount_monthly_academic_trainee":
        sampleDF=spark.sql('''select Candidate_Id,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_Centre,Operating_Unit,Client_Geography,User_Type,Location,Sub_Location,Job,Position,People_Group,
        CASE when (Date_of_Joining== ' ' or Date_of_Joining== '-') then NULL else Date_of_Joining END as Date_of_Joining,
        Gender,Company,Subcontract_Agency,Official_Email_ID,Assignment_Category,Staff_Type,BU,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_headcount_monthly_maternity_cases":
        sampleDF=spark.sql('''select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job_Name,People_Group_Name,Employee_Category,
        CASE when (Date_First_Hired == ' ' or Date_First_Hired == '-') then NULL else Date_First_Hired END as Date_First_Hired,
        Company_Name,
        CASE when (Start_Date == ' ' or Start_Date == '-') then NULL else Start_Date END as Start_Date,
        CASE when (End_Date == ' ' or End_Date == '-') then NULL else End_Date END as End_Date,
        Status,BU,File_Date,File_Month,File_Year,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    # else:
    #     sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

    # sampleDF.write \
    #   .format("jdbc") \
    #   .mode("append") \
    #   .option("url",jdbcUrl) \
    #   .option("dbtable",hist_table) \
    #   .option("user", username) \
    #   .option("password", password) \
    #   .save()

# COMMAND ----------

# DBTITLE 1,Compensation Non sensitive
if processName == "compensation_non_sensitive":
    if table == "trusted_compensation_non_sensitive_qualification_dump":
        sampleDF=spark.sql('''select EMP_ID,FULL_NAME,Post_Graduation___Highest_Qualification,GRAD,OTHERS,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        # sampleDF=spark.sql('''select *,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == "trusted_compensation_non_sensitive_yec":
        sampleDF=spark.sql('''select EMPLOYEE_NUMBER,PITCHED_AT__CURRENT_POSITIONING_,DATE_SINCE_AT_CURRENT_DESIGNATION,YEARS_AT_CURRENT_DESIGNATION,WORK_EXPERIENCE_PRIOR_TO_KPMG__YY_MM_FORMAT_,TOTAL_YEARS_OF_WORK_EXPERIENCE__YY_MM_FORMAT_,YEAR_OF_PITCHING,FY,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        # sampleDF=spark.sql('''select *,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        
        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_compensation_non_sensitive_finance_metrics":
        sampleDF=spark.sql('''select Cost_centre,Position,Function,Employee_Subfunction,Client_Geography,Aggregated_HC,Annual_Aggregated_CTC,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        # sampleDF=spark.sql('''select *,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


        # sampleDF.write \
        # .format("jdbc") \
        # .mode("overwrite") \
        # .option("truncate", "true") \
        # .option("url",jdbcUrl) \
        # .option("dbtable",hist_table) \
        # .option("user", username) \
        # .option("password", password) \
        # .save()
        

# COMMAND ----------

# DBTITLE 1,Compensation
# jdbcHostname = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseHostName")  
# # jdbcDatabase = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseName")
# jdbcPort = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databasePort")  
# username = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNameUserName")  
# password = password = dbutils.secrets.get(scope="adb-secretscope-keyvault",key="databaseNamePassword")  
# jdbcUrl_iso = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase_iso)  
# connectionProperties = {  
#   "user" : username,  
#   "password" : password,  
#   "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"  
# }

# if processName == "compensation":

#     print(table)
#     if table == "trusted_compensation_additional_pay":
#         sampleDF=spark.sql('select BATCH_NO,BATCH_DATE,`EMPLOYEE_NO**`,`EMPLOYEE_NAME**`,HEAD,TO_GROSSUP,MONTH1,AMOUNT1,CLAWBACK_DURATION_IN_MONTHS,MONTH2,AMOUNT2,CLAWBACK_DURATION_IN_MONTHS2,MONTH3,AMOUNT3,CLAWBACK_DURATION_IN_MONTHS22,MONTH4,AMOUNT4,AMOUNT5,REMARKS,Dated_On,File_Date,CLAWBACK_DURATION_IN_MONTHS222,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

#         sampleDF.write \
#         .format("jdbc") \
#         .mode("append") \
#         .option("url",jdbcUrl_iso) \
#         .option("dbtable",hist_table) \
#         .option("user", username) \
#         .option("password", password) \
#         .save()
    
#     if table == "trusted_compensation_paysheet":
#         sampleDF=spark.sql('select EMP_NO,NAME,NJ_INDICATOR,SALARY_INDICATOR,ACTIVE_IND,BIRTH_DATE,JOINING_DATE,DATE_OF_JOINING_FOR_GRATUITY,LEAVING_DATE,LOCATION,SUB_LOCATION,STATE,DESIGNATION,DIVISION_NAME,COMPANY,COMPANY_NAME,OPERATING_UNIT,GRADE_NAME,COSTCENT,COSTCENT_NAME,CC_PAY_MOD,DEPARTMENT,PF_NO,UAN,ADHAR,PRAN,CATEGORY,GEO,USER_TYPE,BUSS_CTGY,ORG_ID,EMP_PAN,STANDARD_DAYS,LWP_DAYS,DAYS_PAYABLE,PAYMENT_MODE,BANK_NAME,BANK_ACCOUNT,IFSC_CODE,BENE_NAME,M_BASIC,M_H_R_A_,M_SPECIAL_ALLOWANCE,M_CONVEYANCE_ALLW,M_CHILD_EDUCATION,M_STIPEND,CTC,MCTC,BASIC,ARREAR_BASIC,H_R_A_,ARREAR_HRA,SPECIAL_ALLOW,ARREAR_SPECIAL_ALLOW,CONVEYANCE_ALLOW,ARREAR_CONVEYANCE_ALLOW,CHILD_EDUCATION,ARREAR_CHILD_EDUCATION,STIPEND,ARREAR_STIPEND,COMPENSATORY_OFF_,LOYALTY_BONUS,PERFORMANCE_BONUS,EXAM_BONUS,SECONDMENT_IAP,INCENTIVE,NOTICE_PAY_BUYOUTS,OTHER_EARNINGS___NT,HOT_SKILL_ALLOWANCE,OTHER_EARNINGS,REFERRAL_BONUS,ONE_TIME_BONUS,RELOCATION_ALLOWANCE,RETENTION_BONUS,SHIFT_ALLOWANCE,SIGN_ON_BONUS,EXGRATIA,EXTRA_DAY_PAYMENT,BUSY_SEASON_BONUS,JOINING_BONUS,TAX_PERQUISITE,PT_PERQUISITE,PF_PERQUISITE,One_Time_WAH_Allowance,TIME_SHEET_NETPAY_REVERSAL,PAN_NO_DEFAULT_REVERSAL,BANK_DEFAULT_NETPAY_REVERSAL,RESIGNED_NETPAY_REVERSAL,SALARY_REJECTION,GROSS,P_F_,V_P_F_,PROFESSION_TAX,ESIC,L_W_F_,INCOME_TAX,SALARY_ADVANCE,DONATION,TRAVEL_ADVANCE_RECOVERY,RISK_PENALTY,OTHER_DEDUCTION,MEDICLAIM_FOR_SELF_SPOUSE___CH,KPMG_FOUNDATION,COVID_KAWACH,ID_CARD_RECOVERY,TIMESHEET_DEFAULTER_NET_PAY_HO,PAN_DEFAULTER_NET_PAY_HOLD,BANK_ACCOUNT_DEFAULT_HOLD,RESIGNED_HOLD,ADVANCE,Adhoc_Loan_Principal,TOT_DED,NET_PAY,EMPLOYER_PF,EMPLOYER_ESIC,EMPLOYER_LWF,NPS,Dated_On,File_Date,left(File_Date,6) as Month_Key,PENDING_PF_FORM11_REVERSAL,AMEX_RECOVERY,PENDING_PF_FORM11_HOLD,ADDL__INCOME_TAX_DEDUCTION from kgsonedatadb.'+table)

#         sampleDF.write \
#         .format("jdbc") \
#         .mode("append") \
#         .option("url",jdbcUrl_iso) \
#         .option("dbtable",hist_table) \
#         .option("user", username) \
#         .option("password", password) \
#         .save()
    
    # if table == "trusted_compensation_yec":
    #     sampleDF=spark.sql('select * from kgsonedatadb.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("truncate", "true") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()


#         # sampleDF.write \
#         #     .format("jdbc") \
#         #     .mode("append") \
#         #     .option("url",jdbcUrl) \
#         #     .option("dbtable",hist_table) \
#         #     .option("user", username) \
#         #     .option("password", password) \
#         #     .save()

    # if table == "trusted_compensation_finance_metrics":
    #     sampleDF=spark.sql('select * from kgsonedatadb.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("truncate", "true") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

#     # else:
#     #     sampleDF=spark.sql('select *,left(File_Date,6) as Month_Key from kgsonedatadb.'+table)

#     # sampleDF.write \
#     #   .format("jdbc") \
#     #   .mode("append") \
#     #   .option("url",jdbcUrl) \
#     #   .option("dbtable",hist_table) \
#     #   .option("user", username) \
#     #   .option("password", password) \
#     #   .save()

if processName == "employee_engagement":
    print(table)
    if table == "trusted_employee_engagement_year_end":
        
        sampleDF=spark.sql('''select Emp_ID,Employee_Name,`Function`,Sub_Function,Cost_centre,BU,Client_Geo,Operating_unit,Location,Designation,Job_Code,Date_as_of,Pitched_at__Current_Positioning_,Pitching_Year,Date_since_at_current_designation,round(cast(Years_at_current_designation_as_on_30_Sep_Current_FY as float),1) as Years_at_current_designation_as_on_30_Sep_Current_FY,DOJ,round(cast(Years_in_the_Firm_as_on_30_Sep_Current_FY as float),1) as Years_in_the_Firm_as_on_30_Sep_Current_FY,round(cast(Work_experience_prior_to_KPMG__Years_ as float),1) as Work_experience_prior_to_KPMG__Years_,round(cast(Total_years_of_work_experience_as_on_30_Sep_Current_FY as float),1) as Total_years_of_work_experience_as_on_30_Sep_Current_FY,Last_FY_Rating,Previous_FY_Rating,Previous_to_Previous_FY_Rating,Post_Graduation_Highest_Qualification,Graduation,Gender,Email,Emp_Category,Performance_manager_name,Encore_winner,`Include_in_rating_distribution?`,replace(no_of_days_taken_in_current_year,'null','') as no_of_days_taken_in_current_year,Reason_for_exclusion_from_distribution_Long_leaves_transfers_exits_etc_,Distribution_category,Team_Band_as_per_CP_document,Proposed_Rating_Current_FY,Rating_Jump_or_Dip,Promotion_recommendation_Yes_or_No_,Promotion_or_Progession,BP_comments,Comments_on_Promotion_Exceptions,PD_comments,HR_head_comments,Promotion_category,Promotion_approved,New_Designation,New_job_code,New_Pitched_at__proposed_positioning,New_Pitching_Year,Remarks,File_Date,Dated_On,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        
        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("truncate", "false") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

        # Triggering the logic app workflow to refresh datamodel        
        import requests
        website_link = "https://goprdinclakgs01.azurewebsites.net:443/api/wf_ye_template_model_refresh/triggers/When_a_HTTP_request_is_received/invoke?api-version=2022-05-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=gFpMS2UBcEcGfKxtxlcHw1euuLpjBeU-BIJRZy52koY"
        response = requests.get(website_link)
        if response.status_code == 202:
            print("Model refresh logic app workflow triggered successfully.")
        else:
            print("Error accessing logic app workflow:", response.status_code, response.text)

# COMMAND ----------

# DBTITLE 1,IT
if processName == "it":
    if table == "trusted_it_optional_business_application_installation":
        
        sampleDF=spark.sql('''select `NUMBER`,ITEM,OPENED,STATE,REQUESTED_FOR,LOCATION,OPENED_BY,UPDATED,UPDATED_BY,SOFTWARE_APPROVAL_REQUEST_ID,
        CASE when (trim(USAGE_START_DATE_) == '' or USAGE_START_DATE_ == '-') then NULL else USAGE_START_DATE_ END as USAGE_START_DATE,
        CASE when (trim(USAGE_END_DATE) == '' or USAGE_END_DATE == '-') then NULL else USAGE_END_DATE END as USAGE_END_DATE,
        PLEASE_ENTER_LICENSE_TYPE_,PLEASE_SELECT_LICENSE_SOURCE,PROJECT_SOFTWARE_INVENTORY,PLEASE_TYPE_THE_REKONNECT_PROJECT_CODE,PLEASE_SPECIFIC_ANY_ADDITIONAL_USER_S_WHO_NEED_THIS_SOFTWARE_INSTALLED,
        
        Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_it_project_code_dump":
        

        sampleDF=spark.sql('''select PROJECT_TYPE,PROJECT_NUMBER ,PRJ_OPERATINGUNIT ,PROJECT_NAME ,PROJECT_DESCRIPTION ,
        CASE when (trim(Project_Creation_Date) == '' or Project_Creation_Date == '-') then NULL else Project_Creation_Date END as Project_Creation_Date ,    
        CASE when (trim(Project_Start_Date) == '' or Project_Start_Date == '-') then NULL else Project_Start_Date END as Project_Start_Date,                       
        CASE when (trim(Project_Completion_Date) == '' or Project_Completion_Date == '-') then NULL else Project_Completion_Date END as Project_Completion_Date,
        PROJECT_OWNING_PROFIT_CENTRE_COST_CENTRE ,CLIENT_NO ,CLIENT_NAME ,ENGAGEMENT_MANAGER ,ENGAGEMENT_PARTNER ,CUSTOMER_AND_CONTACTS ,TASK_NUMBER ,TASK_NAME ,TASK_DESCRIPTION ,PROJECT_CURRENCY_CODE ,AGREEMENT_CURRENCY_CODE ,CLIENT_GEOGRAPHY ,SITE_ID ,TAX_CATEGORY ,CLIENT_ADDRESS ,Dated_On ,FILE_DATE ,
        left(File_Date,6) as Month_Key
        from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_it_servicenow_asset_report":
        
        sampleDF=spark.sql('''select SERIAL_NUMBER,MODEL_ID,CATEGORY,ASSIGNED_TO,EMPLOYEE_ID as EMPLOYEE_NUMBER,EMAIL,COMPANY,DEPARTMENT,FUNCTION,LOCATION,CONFIGURATION_ITEM,ASSIGNED,STATE,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key
        from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_it_software_product_license_inventory":
       

        sampleDF=spark.sql('''select SOFTWARE_NAME,LICENSE_INWARD_REQUEST,LICENSE_OWNER,EMAIL,SOFTWARE_APPROVAL_ID,REKONNECT_PROJECT_CODE,LICENSE_SOURCE,LICENSE_TYPE,LICENSE_AVAILABLE,
        CASE when (trim(LICENSE_START_DATE) == '' or LICENSE_START_DATE == '-') then NULL else LICENSE_START_DATE END as LICENSE_START_DATE,
        CASE when (trim(LICENSE_END_DATE) == '' or LICENSE_END_DATE == '-') then NULL else LICENSE_END_DATE END as LICENSE_END_DATE,
        LICENSE_COUNT,STATE,Dated_On,FILE_DATE,
        left(File_Date,6) as Month_Key
        from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_it_uninstall_an_application":
        
        sampleDF=spark.sql('''SELECT `NUMBER`,OPENED,STATE,REQUESTED_FOR,LOCATION,ITEM,OPENED_BY,APPROVAL,ASSIGNMENT_GROUP,ASSIGNED_TO,UPDATED,UPDATED_BY,EXISTING_SOFTWARE_INSTALLATION_REQUEST_NUMBER,APPLICATION_NAME,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key
        from kgsonedatadb.'''+table)
        
        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


    if table == "trusted_it_talent_connect_daily_transfer_report":

        sampleDF=spark.sql('''select EMPLOYEE_NUMBER,EMPLOYEE_NAME,EMPLOYEE_E_MAIL,TRANSFER_TYPE,
        CASE when (trim(TRANSFER_INITIATION_DATE) == '' or TRANSFER_INITIATION_DATE == '-') then NULL else TRANSFER_INITIATION_DATE END as TRANSFER_INITIATION_DATE,
        CASE when (trim(TRANSFER_EFFECTIVE_DATE) == '' or TRANSFER_EFFECTIVE_DATE == '-') then NULL else TRANSFER_EFFECTIVE_DATE END as TRANSFER_EFFECTIVE_DATE,
        B_TO_K_OR_K_TO_B_TRANSFER,APPROVAL_STATUS,PENDING_WITH,FINANCE_APPROVER,IT_APPROVER,RESOURCE_MANAGER_APPROVER,ADMIN_APPROVER,EMPLOYEE_ACCEPTANCE,PREVIOUS_ENTITY_NAME,NEW_ENTITY_NAME,PREVIOUS_DEPARTMENT,NEW_DEPARTMENT,CURRENT_LOCATION,NEW_LOCATION,SUB_LOCATION,JOB_NAME,POSITION_NAME,PC_NAME,PM_EMP_CODE,PM_NAME,NEW_PEOPLE_GROUP,EMPLOYEE_CATEGORY,HRBP_EMPLOYEE_NAME,
        CASE when (trim(ASSIGNED_DATE) == '' or ASSIGNED_DATE == '-') then NULL else ASSIGNED_DATE END as ASSIGNED_DATE,
        CASE when (trim(COMPLETION_DATE) == '' or COMPLETION_DATE == '-') then NULL else COMPLETION_DATE END as COMPLETION_DATE,
        TASK_PERFORMED_BY,Dated_On,FILE_DATE,
        left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)
        
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_it_talent_connect_daily_exit_report":
        
        sampleDF=spark.sql('''select EMPLOYEENUMBER,EMPLOYEENAME,EMAIL,DESIGNATION,ORGANIZATIONNAME,ENTITY,LOCATION,
        CASE when (trim(RESIGNATIONINITIATEDDATE) == '' or RESIGNATIONINITIATEDDATE == '-') then NULL else RESIGNATIONINITIATEDDATE END as RESIGNATIONINITIATEDDATE,
        CASE when (trim(LWD) == '' or LWD == '-') then NULL else LWD END as LWD,
        CASE when (trim(PMLEVELLWDAPPROVALDATE) == '' or PMLEVELLWDAPPROVALDATE == '-') then NULL else PMLEVELLWDAPPROVALDATE END as PMLEVELLWDAPPROVALDATE,
        CASE when (trim(PARTNERLWDAPPROVALDATE) == '' or PARTNERLWDAPPROVALDATE == '-') then NULL else PARTNERLWDAPPROVALDATE END as PARTNERLWDAPPROVALDATE,
        CASE when (trim(EMPLOYEECLEARANCEDATE) == '' or EMPLOYEECLEARANCEDATE == '-') then NULL else EMPLOYEECLEARANCEDATE END as EMPLOYEECLEARANCEDATE,
        CASE when (trim(ITCLEARANCEAPPROVALDATE) == '' or ITCLEARANCEAPPROVALDATE == '-') then NULL else ITCLEARANCEAPPROVALDATE END as ITCLEARANCEAPPROVALDATE,
        IT_CLEARANCE_APPROVED_BY,HRCLEARANCECOMMENT,WITHDRAWALSTATUS,
        CASE when (trim(PARTNERWITHDRAWALAPPROVALDATE) == '' or PARTNERWITHDRAWALAPPROVALDATE == '-') then NULL else PARTNERWITHDRAWALAPPROVALDATE END as PARTNERWITHDRAWALAPPROVALDATE,
        ID_DISABLED_DATE_STAMP,Dated_On,FILE_DATE,
        left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)
        
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_it_contractor_intern_daily_exit_report":
        
        sampleDF=spark.sql('''select EMPLOYEE_NUMBER,EMPLOYEE_NAME,EMAIL_ADDRESS,DESIGNATION,ENTITY,DEPARTMENT,LOCATION,
        CASE when (trim(CONTRACT_END_DATE) == '' or CONTRACT_END_DATE == '-') then NULL else CONTRACT_END_DATE END as CONTRACT_END_DATE,
        REPORTING_MANAGER_NAME,REPORTING_MANAGER_EMAIL_ID,RETAIN_MAIL,RETAIN_REASON,UPDATE_TYPE,Dated_On,FILE_DATE,
        left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)
        
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_it_daily_exit_report":

        sampleDF=spark.sql('''select EMPLOYEE_NUMBER,EMPLOYEE_NAME,EMAIL_ADDRESS,EMPLOYEE_CATEGORY,DESIGNATION,ENTITY,DEPARTMENT,LOCATION,
        CASE when (trim(RESIGNATION_INITITED_DATE) == '' or RESIGNATION_INITITED_DATE == '-') then NULL else RESIGNATION_INITITED_DATE END as RESIGNATION_INITITED_DATE,
        CASE when (trim(DESIRED_LWD) == '' or DESIRED_LWD == '-') then NULL else DESIRED_LWD END as DESIRED_LWD,
        CASE when (trim(POLICY_LWD) == '' or POLICY_LWD == '-') then NULL else POLICY_LWD END as POLICY_LWD,
        CASE when (trim(APPROVED_LWD) == '' or APPROVED_LWD == '-') then NULL else APPROVED_LWD END as APPROVED_LWD,
        CASE when (trim(EMPLOYEE_CLEARANCE_DATE) == '' or EMPLOYEE_CLEARANCE_DATE == '-') then NULL else EMPLOYEE_CLEARANCE_DATE END as EMPLOYEE_CLEARANCE_DATE,
        CASE when (trim(IT_CLEARANCE_DATE) == '' or IT_CLEARANCE_DATE == '-') then NULL else IT_CLEARANCE_DATE END as IT_CLEARANCE_DATE,
        HR_CLEARANCE_COMMENT,WITHDRAWAL_STATUS,
        CASE when (trim(PM_WITHDRAWALAPPROVAL_DATE) == '' or PM_WITHDRAWALAPPROVAL_DATE == '-') then NULL else PM_WITHDRAWALAPPROVAL_DATE END as PM_WITHDRAWALAPPROVAL_DATE,
        CASE when (trim(PM_APPROVAL_DATE) == '' or PM_APPROVAL_DATE == '-') then NULL else PM_APPROVAL_DATE END as PM_APPROVAL_DATE,
        CASE when (trim(REPORTING_PARTNER_APPROVAL_DATE) == '' or REPORTING_PARTNER_APPROVAL_DATE == '-') then NULL else REPORTING_PARTNER_APPROVAL_DATE END as REPORTING_PARTNER_APPROVAL_DATE,
        Dated_On,FILE_DATE,
        left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)
        
        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


    if table == "trusted_it_software_approval_request":

        sampleDF=spark.sql('''select Number,Item,Approval_Status,Software_Name,Publisher___OEM_Name,Requested_For,Usage_End_Date,License_Type,Project_Number__Rekonnect_,Entity,Function,Sub_Function,   Dated_On,FILE_DATE,left(File_Date,8) as Month_Key,Cost_Center,Business_Unit from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

 

    if table == "trusted_it_software_database":

        sampleDF=spark.sql('''select  License_Inward_Request,Publisher,Software_Name,Rekonnect_Project_Code,State,License_Start_Date,License_End_Date,License_Count,Licenses_Installed,License_Available,Purchase_Order_Number,License_Source,License_Type,Email,Function,Sub_Function,  Dated_On,FILE_DATE,left(File_Date,8) as Month_Key,Cost_Center from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

# DBTITLE 1,Admin
if processName == "admin":
    if table == "trusted_admin_qlikview_report":
        sampleDF=spark.sql('''select ENTITY_NAME,OPERATING_UNIT,VENDOR_NUMBER,VENDOR_NAME,MSME_STATUS,PO_AMOUNT,PO_NUMBER,INVOICE_NUMBER,CASE when (trim(PO_DATE) == '' or PO_DATE == '-') then NULL else PO_DATE END as PO_DATE,PAYMENT_METHOD,DESCRIPTION,STATUS,PROJECT_NUMBER,PROJECT_DESCRIPTION,CASE when (trim(CHEQUE_DATE) == '' or CHEQUE_DATE == '-') then NULL else CHEQUE_DATE END as CHEQUE_DATE,CHEQUE_AMOUNT,CHEQUE_NO___DOC_NO,BUYER_NAME,REQUESTER,REQUESTER_LOCATION,DD_NO__UTR,BATCH_NAME,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key,INVOICE_DATE from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


    # if table == "trusted_admin_xport_planned_vs_actuals":
    #     sampleDF=spark.sql('''select Location,Date,DEPARTMENT as BU,cast(Planned as int),cast(Actual as int),cast(NO_SHOWS as int) as Variance,BU2,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("append") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    if (tableName in ['xport_no_shows_pune', 'xport_no_shows_bangalore', 'xport_no_shows_kochi', 'xport_no_shows_kolkata', 'xport_no_shows_gurgaon','xport_no_shows_hyderabad']):

        hist_table = 'trusted_hist_admin_xport_no_shows'

        df= spark.sql('select distinct LOCATION,FILE_DATE from kgsonedatadb.'+table)

        if(df.count() > 0):
            location=df.select('LOCATION').rdd.flatMap(lambda x: x).collect()
            location=location[0]
            print(location)

            fileDate=df.select('FILE_DATE').rdd.flatMap(lambda x: x).collect()
            fileDate=fileDate[0]
            print(fileDate)

            sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
            cursor.execute(sql_command)
            result = cursor.fetchone()[0]

            if result == 1:
                query=(""" DELETE FROM dbo.{} where LOCATION = '{}' and FILE_DATE= '{}'""").format(hist_table,location,fileDate)
                print(query)
                conn.execute(query)
                conn.commit()
                print('delete executed')

        sampleDF=spark.sql('''select LOCATION,DATE,EMPLOYEE_NUMBER,NAME,DEPARTMENT as TEAM,BU2,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName in ['xport_planned_vs_actuals_pune', 'xport_planned_vs_actuals_bangalore', 'xport_planned_vs_actuals_kochi', 'xport_planned_vs_actuals_kolkata', 'xport_planned_vs_actuals_gurgaon', 'xport_planned_vs_actuals_hyderabad']):

        hist_table = 'trusted_hist_admin_xport_planned_vs_actuals'
        
        df= spark.sql('select distinct LOCATION,FILE_DATE from kgsonedatadb.'+table)

        if(df.count() > 0):
            location=df.select('LOCATION').rdd.flatMap(lambda x: x).collect()
            location=location[0]
            print(location)

            fileDate=df.select('FILE_DATE').rdd.flatMap(lambda x: x).collect()
            fileDate=fileDate[0]
            print(fileDate)

            sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
            cursor.execute(sql_command)
            result = cursor.fetchone()[0]

            if result == 1:
                query=(""" DELETE FROM dbo.{} where LOCATION = '{}' and FILE_DATE= '{}'""").format(hist_table,location,fileDate)
                print(query)
                conn.execute(query)
                conn.commit()
                print('delete executed')
       
        sampleDF=spark.sql('''select Location,Date,DEPARTMENT as BU,Planned,Actual,NO_SHOWS as Variance,BU2,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()



    # if table == "trusted_admin_xport_no_shows":
    #     sampleDF=spark.sql('''select LOCATION,DATE,EMPLOYEE_NUMBER,NAME,DEPARTMENT as TEAM,BU2,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("append") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()


    if table == "trusted_admin_card_swipe_master_data":
        sampleDF=spark.sql('''select `LOCATION`,DEPARTMENT AS BU,PAYROLL_NUM AS EMPLOYEE_ID,NAME___REG_NUM AS EMPLOYEE_NAME,JOB_TITLE AS LEVEL,TRANSACTION_TIME AS DATE,BU2,Office_Location,Cost_Code,ENTITY_TYPE,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


    if table == "trusted_admin_booked_and_no_shows":
        sampleDF=spark.sql('''select `LOCATION`,BU,EMPLOYEE_ID,EMPLOYEE_NAME,STATUS2,FROM_DATE,STATUS,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key,DEPARTMENT from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_admin_locationwise_holidaylist":
        sampleDF=spark.sql('''select Occasion,Date,Location,BU,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    if table == "trusted_admin_employee_data":
        sampleDF=spark.sql('''select Emp_ID,Name,Email_Id,Username,Employee_Type,Designation____Grade,Geography,Function,Sub_Function,Service_Line,Department,Company_Name,Location,Mobile,BU2,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_admin_cwk_data":
        sampleDF=spark.sql('''select Emp_ID,Name,Email_Id,Username,Employee_Type,Designation___Grade,Geography,Function,Subfunction,Service_Line,Department,Company_Name,Location,Mobile,BU2,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == "trusted_admin_loaned_data":
        sampleDF=spark.sql('''select Emp_ID,Name,Email_Id,Username,Employee_Type,Designation____Grade,Geography,Function,Subfunction,Service_Line,Department,Company_Name,Location,Mobile,BU2,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

# DBTITLE 1,Impact
if processName == "impact":

    if table == "trusted_impact_kgs_rec_purchases":
        sampleDF=spark.sql('''select FY,QUANTITY_OF_REC_PURCHASES_KWH,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_impact_fuel_reimbursement_leased_cars":
        sampleDF=spark.sql('''select REPORTING_PARAMETER, COMPONENT, FY, AMOUNT_SPENT,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_impact_sick_wellbeing_data":
        sampleDF=spark.sql('''select FY,BU,OCT,NOV,DEC,JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,FY_AVERAGE,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_impact_one_to_one_status_data":
        sampleDF=spark.sql('''select YEAR,MONTH,KGS_REGISTRATIONS_DONE,KGS_COUNSELLING_SESSIONS_DONE,GDC_REGISTRATIONS_DONE,GDC_COUNSELLING_SESSIONS_DONE,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_impact_gps_wellbeing":
        sampleDF=spark.sql('''select YEAR,QUESTION,CONSULTING,KRC,TAX,GDC,DAS,CF,RAS,DN,MS,CH,OVERALL_FOR_KGS_AND_KRC,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_impact_wlb_attrition_data":
        sampleDF=spark.sql('''select FY,BU,OCT,NOV,DEC,JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,FY_AVERAGE,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_impact_fatality_within_office_premises":
        sampleDF=spark.sql('''select YEAR,MONTH,CONSULTING,KRC,TAX,GDC,DAS,CORPORATE_FUNCTIONS,RAS,DN,MS,CAPABILITY_HUBS,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_impact_office_safety_compliance":
        sampleDF=spark.sql('''select YEAR,COMPLAINT,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == "trusted_impact_accident_within_office_premises":
        sampleDF=spark.sql('''select YEAR,MONTH,CONSULTING,KRC,TAX,GDC,DAS,CORPORATE_FUNCTIONS,RAS,DN,MS,CAPABILITY_HUBS,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    
    if table == "trusted_impact_it_purchased_goods_consumption_data":
        sampleDF=spark.sql('''select HEADS,DEFINITION,UNITS,KGS_DATA,USD,FY,COMMENTS,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_impact_integrity_training_data":
        sampleDF=spark.sql('''select TRAINING,HR_ID,FULL_NAME,CASE when (trim(DATE_FIRST_HIRED) == '' or DATE_FIRST_HIRED == '-') then NULL else DATE_FIRST_HIRED END as DATE_FIRST_HIRED,PEOPLE_GROUP,JOB,CLIENT_GEOGRAPHY,	EMPLOYEE_CATEGORY,FUNCTION,EMPLOYEE_SUBFUNCTION,EMPLOYEE_SUBFUNCTION_1,FINAL_BU,COST_CENTRE,EMPLOYEE_EMAIL,CASE when (trim(ASSIGNED_DATE) == '' or ASSIGNED_DATE == '-') then NULL else ASSIGNED_DATE END as ASSIGNED_DATE,FINANCIAL_YEAR,CASE when (trim(COMPLETION_DATE) == '' or COMPLETION_DATE == '-') then NULL else COMPLETION_DATE END as COMPLETION_DATE,COMPLETION_STATUS,UPDATED_COMPLETION_STATUS,CASE when (trim(DEADLINE) == '' or DEADLINE == '-') then NULL else DEADLINE END as DEADLINE,COMMENTS
        ,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save() 

    if table == "trusted_impact_waste_management_data":
        sampleDF=spark.sql('''select LOCATION,FY,MONTH,FOOD_WASTE__KGS_,WATER_CONSUMPTION__K_LTRS_,PAPER_CONSUMPTION__KGS_,DRY_WASTE__KGS_,PANTRY_CONSUMABLES__INR_,HOUSEKEEPING_CONSUMABLES__INR_,RECYCLED_WASTE__KGS_,	OTHER_WASTE___NON_RECYCLED_WASTE__KGS_,TOTAL_WASTE__INCLUDES_RECYCLED_WASTE___KGS_,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save() 

    if table == "trusted_impact_access_card_data":
        sampleDF=spark.sql('''select CASE when (trim(AS_OF_DATE) == '' or AS_OF_DATE == '-') then NULL else AS_OF_DATE END as AS_OF_DATE,LOCATION,COST_CENTRE,EMPLOYEE_ID,EMPLOYEE_NAME,LEVEL,CASE when (trim(DATE) == '' or DATE == '-') then NULL else DATE END as DATE,DAY,BU,Dated_On,FILE_DATE,left(File_Date,8) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_impact_admin_data":
        sampleDF=spark.sql('''select EXPENSE_CATEGORY,SUB_CAT,CATEGORY,LOCATION,SUB_LOCATION,BU,COST_CENTER,CONSUMPTION__VALUE_,SPEND__INR_,METRIC,MONTH,Dated_On,
       FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        
        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_impact_car_lease_data":
        sampleDF=spark.sql('''select NEW_DESIGNATION,LOCATION,CASE when (trim(START_DATE) == '' or START_DATE == '-') then NULL else START_DATE END as START_DATE,CASE when (trim(END_DATE) == '' or END_DATE == '-') then NULL else END_DATE END as END_DATE,ENGINE_CAPACITY,VARIANT,ENTITY,EMPLOYEE_NUMBER,NAME,EMAIL_ID,COST_CENTER,BU
       ,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        
        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()

    if table == "trusted_impact_csr_data":
        sampleDF=spark.sql('''select EMPLOYEE_NAME,EMPLOYEE_ID,EMAIL_ID,LOCATION,DESIGNATION,SUB_FUNCTION_1,ACTIVITY_NAME,VIRTUAL_OR_NON_VIRTUAL,TYPE,HOURS,OUT_REACH_OF_BENEFICIERIES,LEVELWISE_MAPPING,MONTH,FY,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        
        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save() 

    
    if table == "trusted_impact_electricity_and_dg_data":
        sampleDF=spark.sql('''select STATE,LOCATION,FY,SQUARE_FEET_AREA,ENTITY,TYPE,MONTH,SPENDS,UNITS,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        
        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()  

    if table == "trusted_impact_idne_gender_target_vs_actuals":
        sampleDF=spark.sql('''select GOAL__KPI,TARGET_TYPE,TARGET_VALUE,TARGET_YEAR,CURRENT_YEAR_TARGET,CURRENT_YEAR_ACTUALS,CURRENT_YEAR,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)  

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()  

    if table == "trusted_impact_official_travel_personal_car":
        sampleDF=spark.sql('''select EXPENSE_REPORT_NUMBER,POLICY,EMPLOYEE_ENTITY,EMPLOYEE,EMPLOYEE_ID,	EMPLOYEE_DEPARTMENT,EMPLOYEE_E_MAIL_ADDRESS,EMPLOYEE_POSITION,LAST_SUBMITTED_DATE,PURPOSE,TRANSACTION_DATE,EXPENSE_TYPE,SENT_FOR_PAYMENT_DATE,PAYMENT_TYPE,EXPENSE_AMOUNT__REIMBURSEMENT_CURRENCY_,EXPENSE_AMOUNT__TRANSACTION_CURRENCY_,	REIMBURSEMENT_CURRENCY,TRANSACTION_CURRENCY,EXCHANGE_RATE,APPROVED_AMOUNT,PROJECT_ENTITY,	PROJECT_CODE,PROJECT_NAME,TASK_CODE,TASK_NAME,APPROVAL_STATUS,PAYMENT_STATUS,PERSONAL,	IS_TEST_USER,CITY_LOCATION,COUNTRY,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)  

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save() 

    if table == "trusted_impact_purchased_goods_and_services":
        sampleDF=spark.sql('''select CATEGORY_A,CATEGORY_B,YEAR,BU,ENTITY,AMOUNT_VALUE,DESCRIPTION,
        Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)  

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()  

    if table == "trusted_impact_air_travel_data":
        sampleDF=spark.sql('''select FY,INVOICE_MONTH,DOMESTIC__INTERNATIONAL,DK_DESCRIPTION,ENTITY,TRAVELER_NAME,	CLASS_OF_SERVICE_SEQUENCE,ITINERARY,ORIGIN_AIRPORT__PREDOMINANT_,DESTINATION_AIRPORT__PREDOMINANT_,`INVOICE_#`,AIR_NET_TICKET_AMOUNT,AIR_MILES,AIR_KMS,HAUL_MARKING__BASIS_AIR_KMS,CHARGEABLE__NON_CHARGEABLE,VOUCHER_TYPE,DESIGNATION,DEPARTMENT,TICKET_NO,CO2__EMISSION__IN_TONS,BUSINESS_UNIT,`FUNCTION`,SUB_FUNCTION,SERVICE_LINE, Dated_On,FILE_DATE,left(File_Date,6) as Month_Key  from kgsonedatadb.'''+table)     

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    
    if table == "trusted_impact_employee_commute_data":
        sampleDF=spark.sql('''SELECT YEAR,AVERAGE_DISTANCE_IN_KM,PERCENTAGE_OF_COMMUTING_BY_BIKE,PERCENTAGE_OF_COMMUTING_BY_CAR,PERCENTAGE_OF_COMMUTING_BY_CARSHARING,PERCENTAGE_OF_COMMUTING_BY_MOTORCYCLE,PERCENTAGE_OF_COMMUTING_BY_WALK,PERCENTAGE_OF_COMMUTING_BY_BUS,PERCENTAGE_OF_COMMUTING_BY_SUBWAY,PERCENTAGE_OF_COMMUTING_BY_TRAIN,PERCENTAGE_OF_COMMUTING_BY_TRAM,PRESENTISM_INDEX,AVAILABLE_ANNUAL_WORKING_DAYS,PUBLIC_HOLIDAYS,VACATIONS,KGS_HEADCOUNT_AVERAGE,COMMUTING_DAYS,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl)\
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()


    if table == "trusted_impact_gps_data":
        sampleDF=spark.sql(''' SELECT YEAR,PARAMETERS ,KGS_OVERALL ,GDC ,KRC ,TAX ,CF ,CH ,CONSULTING ,DAS ,DN ,MS ,RA_C,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write.format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl).option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save() 
       
    if table == "trusted_impact_locationwise_seat_allocation":
        sampleDF=spark.sql('''select FY,LOCATION,BU,MONTH,TOTAL_SEATS,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)  

        sampleDF.write \
       .format("jdbc") \
       .mode("append") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save() 

# COMMAND ----------

# DBTITLE 1,Employee_Rating
if processName == "compensation":
    if table == "trusted_compensation_employee_ratings":
        sampleDF=spark.sql('''select EMPLOYEE_NUMBER,NAME,EMAIL,EMPLOYEE_CATEGORY,BU_MAPPING,DEPARTMENT,RATING,FY,Dated_On,FILE_DATE,concat("20",right(FY,2),"10") as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

# COMMAND ----------

# DBTITLE 1,FinanceMetrics
if processName == "compensation":
    if table == "trusted_compensation_finance_metrics":
        sampleDF=spark.sql('''select ROW_LABELS,GEO,POSITION,AGGREGATED_HC,AGGREGATED_MONTHLY_GROSS_AMOUNT,AGGREGATED_ANNUAL_CTC,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_compensation_finance_metrics_joiners":
        sampleDF=spark.sql('''select ROW_LABELS,GEO,POSITION,AGGREGATED_HC,AGGREGATED_MONTHLY_GROSS_AMOUNT,AGGREGATED_ANNUAL_CTC,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

    if table == "trusted_compensation_finance_metrics_leavers":
        sampleDF=spark.sql('''select ROW_LABELS,GEO,POSITION,AGGREGATED_HC,AGGREGATED_MONTHLY_GROSS_AMOUNT,AGGREGATED_ANNUAL_CTC,Dated_On,FILE_DATE,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

# COMMAND ----------

if processName == "compensation":
    if table == "trusted_compensation_yec":
        sampleDF=spark.sql('''select EMPLOYEE_NUMBER, PITCHED_AT__CURRENT_POSITIONING_ as PITCHED_AT_CURRENT_POSITIONING, DATE_SINCE_AT_CURRENT_DESIGNATION, YEARS_AT_CURRENT_DESIGNATION as YEARS_AT_CURRENT_DESIGNATION, WORK_EXPERIENCE_PRIOR_TO_KPMG__YY_MM_FORMAT_ as WORK_EXPERIENCE_PRIOR_TO_KPMG, TOTAL_YEARS_OF_WORK_EXPERIENCE__YY_MM_FORMAT_ as TOTAL_YEARS_OF_WORK_EXPERIENCE, YEAR_OF_PITCHING, FY, Dated_On, File_Date, left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        # sampleDF=spark.sql('''select *,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)
        
        month_key = sampleDF.select('Month_Key').distinct().rdd.map(lambda x:x[0]).first()
        query = f"SELECT COUNT(*) FROM dbo.{hist_table} where Month_Key = '{month_key}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        
        if result > 0:
            query = f"DELETE FROM dbo.{hist_table} where Month_Key = '{month_key}'"
            conn.execute(query)
            conn.commit()
            print(hist_table + ' deleted successfully for ' + month_key +' in SQL Database')
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
            print('Enteries loaded successfully for month ' + month_key +' in '+ hist_table)

# COMMAND ----------

# DBTITLE 1,Risk
if processName == "risk":
    if table == 'trusted_risk_kgs_india_mandatory_training':
        sampleDF=spark.sql('''select TRAINING, HR_ID, FULL_NAME,CASE when (trim(DATE_FIRST_HIRED) == '' or DATE_FIRST_HIRED == '-') then NULL else DATE_FIRST_HIRED END as DATE_FIRST_HIRED, PEOPLE_GROUP, JOB, CLIENT_GEOGRAPHY, EMPLOYEE_CATEGORY, FUNCTION, EMPLOYEE_SUBFUNCTION, EMPLOYEE_SUBFUNCTION_1, BU, COST_CENTRE, EMPLOYEE_EMAIL,CASE when (trim(ASSIGNED_DATE) == '' or ASSIGNED_DATE == '-') then NULL else ASSIGNED_DATE END as ASSIGNED_DATE,CASE when (trim(COMPLETION_DATE) == '' or COMPLETION_DATE == '-') then NULL else COMPLETION_DATE END as COMPLETION_DATE, COMPLETION_STATUS,CASE when (trim(DEADLINE) == '' or DEADLINE == '-') then NULL else DEADLINE END as DEADLINE, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_risk_kin_data':
        sampleDF=spark.sql('''select EMP_ID, FULL_NAME, BU, LOCATION, EMPLOYEE_EMAIL_ADDRESS,CASE when (trim(1ST_KIN_DATE) == '' or 1ST_KIN_DATE == '-') then NULL else 1ST_KIN_DATE END as 1ST_KIN_DATE,CASE when (trim(2ND_KIN_DATE) == '' or 2ND_KIN_DATE == '-') then NULL else 2ND_KIN_DATE END as 2ND_KIN_DATE,CASE when (trim(3RD_KIN_DATE) == '' or 3RD_KIN_DATE == '-') then NULL else 3RD_KIN_DATE END as 3RD_KIN_DATE,CASE when (trim(4TH_KIN_DATE) == '' or 4TH_KIN_DATE == '-') then NULL else 4TH_KIN_DATE END as 4TH_KIN_DATE,CASE when (trim(5TH_KIN_DATE) == '' or 5TH_KIN_DATE == '-') then NULL else 5TH_KIN_DATE END as 5TH_KIN_DATE, FINAL_STATUS, CASE when (trim(ATTENDED_KIN_DATE) == '' or ATTENDED_KIN_DATE == '-') then NULL else ATTENDED_KIN_DATE END as ATTENDED_KIN_DATE,CASE when (trim(DATE_FIRST_HIRED) == '' or DATE_FIRST_HIRED == '-') then NULL else DATE_FIRST_HIRED END as DATE_FIRST_HIRED, MONTH, DEFAULTER_CATEGORY, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_kgs_mandatory_training_ki_led':
        sampleDF=spark.sql('''select TRAINING, HR_ID, FULL_NAME,CASE when (trim(DATE_FIRST_HIRED) == '' or DATE_FIRST_HIRED == '-') then NULL else DATE_FIRST_HIRED END as DATE_FIRST_HIRED, PEOPLE_GROUP, JOB, CLIENT_GEOGRAPHY, EMPLOYEE_CATEGORY, FUNCTION, EMPLOYEE_SUBFUNCTION, EMPLOYEE_SUBFUNCTION_1, BU, COST_CENTRE, EMPLOYEE_EMAIL,CASE when (trim(ASSIGNED_DATE) == '' or ASSIGNED_DATE == '-') then NULL else ASSIGNED_DATE END as ASSIGNED_DATE,CASE when (trim(COMPLETION_DATE) == '' or COMPLETION_DATE == '-') then NULL else COMPLETION_DATE END as COMPLETION_DATE, COMPLETION_STATUS,CASE when (trim(DEADLINE) == '' or DEADLINE == '-') then NULL else DEADLINE END as DEADLINE, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_risk_kgs_uk_mandatory_training':
        sampleDF=spark.sql('''select TRAINING, HR_ID, FULL_NAME,CASE when (trim(DATE_FIRST_HIRED) == '' or DATE_FIRST_HIRED == '-') then NULL else DATE_FIRST_HIRED END as DATE_FIRST_HIRED, PEOPLE_GROUP, JOB, CLIENT_GEOGRAPHY, EMPLOYEE_CATEGORY, FUNCTION, EMPLOYEE_SUBFUNCTION, EMPLOYEE_SUBFUNCTION_1, BU, COST_CENTRE, EMPLOYEE_EMAIL,CASE when (trim(ASSIGNED_DATE) == '' or ASSIGNED_DATE == '-') then NULL else ASSIGNED_DATE END as ASSIGNED_DATE,CASE when (trim(COMPLETION_DATE) == '' or COMPLETION_DATE == '-') then NULL else COMPLETION_DATE END as COMPLETION_DATE, COMPLETION_STATUS,CASE when (trim(DEADLINE) == '' or DEADLINE == '-') then NULL else DEADLINE END as DEADLINE, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_risk_kgs_us_mandatory_training':
        sampleDF=spark.sql('''select TRAINING, HR_ID, FULL_NAME,CASE when (trim(DATE_FIRST_HIRED) == '' or DATE_FIRST_HIRED == '-') then NULL else DATE_FIRST_HIRED END as DATE_FIRST_HIRED, PEOPLE_GROUP, JOB, CLIENT_GEOGRAPHY, EMPLOYEE_CATEGORY, FUNCTION, EMPLOYEE_SUBFUNCTION, EMPLOYEE_SUBFUNCTION_1, BU, COST_CENTRE, EMPLOYEE_EMAIL,CASE when (trim(ASSIGNED_DATE) == '' or ASSIGNED_DATE == '-') then NULL else ASSIGNED_DATE END as ASSIGNED_DATE,CASE when (trim(COMPLETION_DATE) == '' or COMPLETION_DATE == '-') then NULL else COMPLETION_DATE END as COMPLETION_DATE, COMPLETION_STATUS,CASE when (trim(DEADLINE) == '' or DEADLINE == '-') then NULL else DEADLINE END as DEADLINE, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_independence_violations':
        sampleDF=spark.sql('''select EMPLOYEE_ID,NAME,DATE_OF_JOINING,BUSINESS_UNIT,DESIGNATION,LINE_MANAGER,TYPE_OF_VIOLATION,CATEGORY_OF_VIOLATION,INVESTMENT_TYPE,RESTRICTION_DATE,PURCHASE_DATE,USER_ACQUISITION_DATE,KICS_ACQUISITION_DATE,ACTUAL_SALE_DATE,PURCHASE_PRICE,NUMBER_OF_SHARES,AMOUNT,ACTUAL_SALE_AMOUNT,NET_PROFIT,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_risk_intune_compliance':
        sampleDF=spark.sql('''select DEVICE_NAME,COMPLIANCE,OS,OS_VERSION,PRIMARY_USER_EMAIL_ADDRESS,DEPARTMENT,ENTITY,DESIGNATION,INTUNE_REGISTERED,PHONE_NUMBER,PRIMARY_USER_DISPLAY_NAME,SUBSCRIBER_CARRIER,AZURE_AD_REGISTERED,DEVICE_ID,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_risk_in_dl_allow_user_outbound':
        sampleDF=spark.sql('''select USERNAME, DISPLAYNAME, EMAILADDRESS, TITLE, DEPARTMENT, COMPANY, OFFICE, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_in_dl_kgs_allow_user_outbound':
        sampleDF=spark.sql('''select USERNAME, DISPLAYNAME, EMAILADDRESS, TITLE, DEPARTMENT, COMPANY, OFFICE, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_local_admin_users_kgs':
        sampleDF=spark.sql('''select HOSTNAME,NAME_OF_THE_LOCAL_GROUP,ACCOUNT_CONTAINED_WITHIN_THE_GROUP,ACCOUNT_TYPE,DOMAIN_FOR_ACCOUNT,TYPE_OF_ACCOUNT,RELIABILITY_OF_INFORMATION,Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_laptop_recovery':
        sampleDF=spark.sql('''select EMPLOYEE_NUMBER, EMPLOYEE_NAME, EMAIL_ADDRESS, DESIGNATION, DEPARTMENT, ENTITY, LOCATION, RESIGNATION_INITIATED_DATE, APPROVED_LWD, AGEING, EMPLOYEE_CATEGORY, IT_CLEARANCE_DATE, IT_CLEARANCE_STATUS, IT_REMARKS, ADMIN_STATUS, ADMIN_REMARKS, HRSS_STATUS, HRSS_REMARKS, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_laptop_ageing':
        sampleDF=spark.sql('''select AGEING,SORT, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_usb_access':
        sampleDF=spark.sql('''select NAME,DESCRIPTION,EMAIL,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_ipf':
        sampleDF=spark.sql('''select SOURCE,TRAINING_NAME,EMPLOYEE_NUMBER,FULL_NAME,FUNCTION, EMPLOYEE_SUBFUNCTION, EMPLOYEE_SUBFUNCTION_1, ORGANIZATION_NAME, COST_CENTRE, OPERATING_UNIT, USER_TYPE, CLIENT_GEOGRAPHY, LOCATION, SUB_LOCATION, POSITION, JOB_NAME, PEOPLE_GROUP_NAME, EMPLOYEE_CATEGORY, DATE_FIRST_HIRED, END_DATE, GENDER, COMPANY_NAME, SUPERVISOR_NAME, PERFORMANCE_MANAGER, EMAIL_ADDRESS, IPF_ASSIGNMENT_DATE, IPF_COMPLETION_DATE, STATUS, DEADLINE_DATE, COMMENTS, EXCEPTION_START_DATE, EXCEPTION_END_DATE, BUSINESS_UNIT, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_dp':
        sampleDF=spark.sql('''select SOURCE, TRAINING_NAME, EMPLOYEE_NUMBER, FULL_NAME, FUNCTION, EMPLOYEE_SUBFUNCTION, EMPLOYEE_SUBFUNCTION_1, ORGANIZATION_NAME, COST_CENTRE, OPERATING_UNIT, USER_TYPE, CLIENT_GEOGRAPHY, LOCATION, SUB_LOCATION, POSITION, JOB_NAME, PEOPLE_GROUP_NAME, EMPLOYEE_CATEGORY, DATE_FIRST_HIRED, END_DATE, GENDER, COMPANY_NAME, SUPERVISOR_NAME, PERFORMANCE_MANAGER, EMAIL_ADDRESS, DATA_PRIVACY_ASSIGNMENT_DATE, DATA_PRIVACY_COMPLETION_DATE, STATUS, DEADLINE_DATE, COMMENTS, EXCEPTION_START_DATE, EXCEPTION_END_DATE, BUSINESS_UNIT, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_gsoc':
        sampleDF=spark.sql('''select INCIDENT_ID, PRIORITY, THREAT_CATEGORY_GRUPING, THREAT_CATEGORY, DAY_OF_DATETIME_ASSIGNED, CONFIRMATION_SOURCE, CURRENT_STATUS, CLOSURE_COMMENT, DESCRIPTION, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_euda':
        sampleDF=spark.sql('''select SR__NO, EUDA_NAME, DESCRIPTION_OF_EUDA_PURPOSE_USE, ENTITY_NAME, FUNCTION, SUB_FUNCTION, CRITICALITY_ASSESSMENT_RATING, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_risk_cloud':
        sampleDF=spark.sql('''select NUMBER, BUSINESS_UNIT_LIST as BUSINESS_UNIT, OPENED, STATE, BUSINESS_UNIT as BUSINESS_UNIT_LIST, REQUESTED_FOR, LOCATION, ITEM, SOFTWARE_NAME, REQUEST, OPENED_BY, APPROVAL, ASSIGNMENT_GROUP, ASSIGNED_TO, UPDATED, UPDATED_BY, LICENSE_TYPE, `DOES_THIS_APPLICATION_PROCESS_PHI_INFORMATION?`, `DOES_THIS_APPLICATION_PROCESS_SENSITIVE_PERSONAL_INFORMATION_?`, `DOES_THIS_APPLICATION_PROCESS_PII_INFORMATION?`,`IS_THIS_A_CLOUD_APPLICATION_SUBSCRIPTION?`,`WHAT_PHI_INFO_DO_YOU_PROCESS_?`,`WHAT_PII_INFO_DO_YOU_PROCESS_?`, `WHAT_SENSITIVE_INFO_DO_YOU_PROCESS_?`,`WILL_THIS_SOFTWARE_ALLOW_DATA_TRANSFER_OUTSIDE_KPMG?`, CLOUD_REGISTER, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_applications':
        sampleDF=spark.sql('''select ROW_LABELS, COUNT_OF_SOFTWARE_NAME, COMMENT, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_exit_report':
        sampleDF=spark.sql('''select EMPLOYEENUMBER, EMPLOYEENAME, EMAIL, DESIGNATION, ORGANIZATIONNAME, BU, ENTITY, LOCATION, LWD, PARTNERLWDAPPROVALDATE, ID_DISABLED_DATE_STAMP, SLA_MET___YES_NO, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_late_arrival_to_it_bin':
        sampleDF=spark.sql('''select EMPLOYEENUMBER, EMPLOYEENAME, EMAIL, DESIGNATION, ORGANIZATIONNAME, ENTITY, LOCATION, LWD, PARTNERLWDAPPROVALDATE, ID_DISABLED_DATE_STAMP, SLA_MET_YES_NO, REMARKS, BU, DELAY, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_m_bu':
        sampleDF=spark.sql('''select EMAIL_ADDRESS,PLEASE_PROVIDE_BU, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_m_job_name':
        sampleDF=spark.sql('''select EMPLOYEE_NUMBER,EMAIL_ADDRESS,POSITION,SHEET_NAME,JOB_NAME, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_m_applications':
        sampleDF=spark.sql('''select KGS_PURCHASED_CLOUD_APPLICATIONS,SNO,Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_risk_level':
        sampleDF=spark.sql('''select BU, POSITION, JOB, LEVELS, RANK, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_risk_laptop_bu_mapping':
        sampleDF=spark.sql('''select DEPARTMENT,BU,Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

# DBTITLE 1,Legal
if processName == "legal":
    if table == 'trusted_legal_clra_data':
        sampleDF=spark.sql('''select KGS_ENTITIES,LOCATION,BU,KGS_LOCATION_ADDRESS,KGS_ADDRESS,CONTRACTOR_S_NAME,CONTRACTOR_S_ADDRESS,CONTRACTOR_S_STATE,CONTRACTORMOBILE,CONTRACTORMAILID,LIN_NUMBER,SHOP_ID_REG__NUMBER,NATURE_OF_BUSINESS,CATEGORY,NO_OF_CONTRACT_LABOUR_EMPLOYED,COMMENCEMENT_DATE_OF_CONTRACT,TERMINATION_DATE_OF_CONTRACT,REMARKS,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        print(sampleDF.count())
        query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        if result >0:
            print('Inside Delete Block')
            for val in sampleDF.collect():
                location=val['LOCATION']
                bu=val['BU']
                kgs_address=val['KGS_ADDRESS']
                contractor_name=val['CONTRACTOR_S_NAME']
                contractor_mobile=val['CONTRACTORMOBILE']
                contractor_mail=val['CONTRACTORMAILID']
                lin_number=val['LIN_NUMBER']
                shop_id=val['SHOP_ID_REG__NUMBER']
                nature_business=val['NATURE_OF_BUSINESS']
                category=val['CATEGORY']
                if (shop_id == None) & (lin_number == None):
                    query = f"DELETE FROM dbo.{hist_table} where LOCATION = '{location}' and BU ='{bu}' and KGS_ADDRESS ='{kgs_address}' and CONTRACTOR_S_NAME ='{contractor_name}' and CONTRACTORMOBILE ='{contractor_mobile}' and CONTRACTORMAILID ='{contractor_mail}' and LIN_NUMBER IS NULL and NATURE_OF_BUSINESS ='{nature_business}' and CATEGORY ='{category}' and SHOP_ID_REG__NUMBER IS NULL"
                    conn.execute(query)
                    conn.commit()
                    print(hist_table + ' deleted successfully for ' + contractor_name +' in SQL Database')
                elif (lin_number == None) & (shop_id != None) :
                    query = f"DELETE FROM dbo.{hist_table} where LOCATION = '{location}' and BU ='{bu}' and KGS_ADDRESS ='{kgs_address}' and CONTRACTOR_S_NAME ='{contractor_name}' and CONTRACTORMOBILE ='{contractor_mobile}' and CONTRACTORMAILID ='{contractor_mail}' and LIN_NUMBER IS NULL and NATURE_OF_BUSINESS ='{nature_business}' and CATEGORY ='{category}' and SHOP_ID_REG__NUMBER ='{shop_id}'"
                    conn.execute(query)
                    conn.commit()
                    print(hist_table + ' deleted successfully for ' + contractor_name +' in SQL Database')
                elif (shop_id == None) & (lin_number != None) :
                    query = f"DELETE FROM dbo.{hist_table} where LOCATION = '{location}' and BU ='{bu}' and KGS_ADDRESS ='{kgs_address}' and CONTRACTOR_S_NAME ='{contractor_name}' and CONTRACTORMOBILE ='{contractor_mobile}' and CONTRACTORMAILID ='{contractor_mail}' and LIN_NUMBER ='{lin_number}' and NATURE_OF_BUSINESS ='{nature_business}' and CATEGORY ='{category}' and SHOP_ID_REG__NUMBER IS NULL"
                    conn.execute(query)
                    conn.commit()
                    print(hist_table + ' deleted successfully for ' + contractor_name +' in SQL Database')
                else:
                    query = f"DELETE FROM dbo.{hist_table} where LOCATION = '{location}' and BU ='{bu}' and KGS_ADDRESS ='{kgs_address}' and CONTRACTOR_S_NAME ='{contractor_name}' and CONTRACTORMOBILE ='{contractor_mobile}' and CONTRACTORMAILID ='{contractor_mail}' and NATURE_OF_BUSINESS ='{nature_business}' and CATEGORY ='{category}'"
                    print(query)
                    conn.execute(query)
                    conn.commit()
                    print(hist_table + ' deleted successfully for ' + contractor_name +' in SQL Database')

            sampleDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()
                
        else:
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()

# COMMAND ----------

# DBTITLE 1,CSR
hist_table = "trusted_hist_"+processName+"_"+tableName

if processName == "csr":
    if table == "trusted_csr_fund_recevied_in_csr_account":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_fund_recevied_in_csr_account')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity,Received_From,Date,Amount,Reference_Number,Particulars_in_the_GL_and_the_Bank_statement,Bank,Account_No_,Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_budget":
        year= spark.sql('select distinct Year from kgsonedatadb.trusted_csr_budget')
        fy=year.select('Year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity,Year,PBT,CSR_Contribution__At_2_percent,Opening_Amount_to_be_spent_during_the_year,Total_budget_contributed_by_company,Carried_Forward_unspent_by_NGO,TOTAL_BUDGET__INCLUDING_UNSPENT_BY_THE_NGO_,Total_Admin_Overheads_Allocation, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    

    if table == "trusted_csr_indirect_exp_kgs":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_indirect_exp_kgs')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity_Name,Ledger_Name,Operating_Unit,Business_Category,Client_Geography,FUTURE1,FUTURE2, Interoperating_Unit, User_Type,Source, Function, Sub_Function,Service_line,Cost_Centre, Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_Code,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period_Name,Invoice_Number,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date, Created_By,Created_By___Desc,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator,Receipt_Number,Currency_Code,Requisition_Number,Requisition_Description,Posted_By,Posted_Date,AR_Invoice_Number,FCY,Employee_Number,Remarks,Year_Month,  Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_indirect_exp_kgsmpl":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_indirect_exp_kgsmpl')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity_Name,Ledger_Name,Operating_Unit,Business_Category,Client_Geography,FUTURE1,FUTURE2, Interoperating_Unit, User_Type,Source, Function, Sub_Function,Service_line,Cost_Centre, Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_Code,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period_Name,Invoice_Number,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date, Created_By,Created_By___Desc,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator,Receipt_Number,Currency_Code,Requisition_Number,Requisition_Description,Posted_By,Posted_Date,AR_Invoice_Number,FCY,Employee_Number,Remarks,Year_Month,  Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_indirect_exp_krc":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_indirect_exp_krc')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity_Name,Ledger_Name,Operating_Unit,Business_Category,Client_Geography,FUTURE1,FUTURE2, Interoperating_Unit, User_Type,Source, Function, Sub_Function,Service_line,Cost_Centre, Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_Code,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period_Name,Invoice_Number,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date, Created_By,Created_By___Desc,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator,Receipt_Number,Currency_Code,Requisition_Number,Requisition_Description,Posted_By,Posted_Date,AR_Invoice_Number,FCY,Employee_Number,Funding_Project_code,  Dated_On,Year_Month, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_per_hour_rate":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_per_hour_rate')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Business_Units,Designation,Cost_Per_Hour, Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_kgs_raw_data_cy":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_kgs_raw_data_cy')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Employee_Name,Employee_ID,Email_ID,Location,Designation,Revised_Designation,Revised_Designation_2,Entity,Function, Sub_Function,Sub_Function_1,BU,Sub_Function_2,Volunteering_Activities,Description_of_the_activity,Skilled__General,Virtual_Or_Non_Virtual,Type,Name_of_the_NGO,Partner_or_non_partner,Government_Schools, Outreach___No_of_beneficiaries,Age_of_beneficiaries,Details_Of_Beneficiaries,Revised_Benedicieries_details,Central_or_BU,Hours, Month, Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )

        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()

        print("table created :" + hist_table)

    if table == "trusted_csr_krcpl_gl":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_krcpl_gl')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity_Name,Ledger_Name,Operating_Unit,Business_Category,Client_Geography,FUTURE1,FUTURE2, Interoperating_Unit, User_Type,Source, Function, Sub_Function,Service_line,Cost_Centre, Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_Code,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period_Name,Invoice_Number,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date, Created_By,Created_By___Desc,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator,Receipt_Number,Currency_Code,Requisition_Number,Requisition_Description,Posted_By,Posted_Date,AR_Invoice_Number,FCY,Employee_Number,Funding_Project_code,  Dated_On,Year_Month, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_kgspl_gl":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_kgspl_gl')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity_Name,Ledger_Name,Operating_Unit,Business_Category,Client_Geography,FUTURE1,FUTURE2, Interoperating_Unit, User_Type,Source, Function, Sub_Function,Service_line,Cost_Centre, Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_Code,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period_Name,Invoice_Number,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date, Created_By,Created_By___Desc,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator,Receipt_Number,Currency_Code,Requisition_Number,Requisition_Description,Posted_By,Posted_Date,AR_Invoice_Number,FCY,Employee_Number,Remarks,Year_Month,  Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_kgsmpl_gl":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_kgsmpl_gl')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity_Name,Ledger_Name,Operating_Unit,Business_Category,Client_Geography,FUTURE1,FUTURE2, Interoperating_Unit, User_Type,Source, Function, Sub_Function,Service_line,Cost_Centre, Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_Code,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period_Name,Invoice_Number,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date, Created_By,Created_By___Desc,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator,Receipt_Number,Currency_Code,Requisition_Number,Requisition_Description,Posted_By,Posted_Date,AR_Invoice_Number,FCY,Employee_Number,Funding_Project_code,Year_Month,  Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_fund_recevied_in_csr_account_kgdc":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_fund_recevied_in_csr_account_kgdc')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity,Received_From,Date,Amount,Reference_Number,Particulars_in_the_GL_and_the_Bank_statement,Bank,Account_No_,Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_budget_kgdc":
        year= spark.sql('select distinct Year from kgsonedatadb.trusted_csr_budget_kgdc')
        fy=year.select('Year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity,Year,PBT,CSR_Contribution__At_2_percent,Opening_Amount_to_be_spent_during_the_year,Total_budget_contributed_by_company,Carried_Forward__Unspent_By_NGO_,TOTAL_BUDGET__INCLUDING_UNSPENT_BY_THE_NGO_,Total_Admin_Overheads_Allocation, Dated_On, File_Date,left(File_Date,6) as Month_Key from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_kgdcpl_gl":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_kgdcpl_gl')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity_Name,Ledger_Name,Operating_Unit,Business_Category,Client_Geography,FUTURE1,FUTURE2, Interoperating_Unit, User_Type,Source, Function, Sub_Function,Service_line,Cost_Centre, Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_Code,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period_Name,Invoice_Number,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date, Created_By,Created_By___Desc,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator,Receipt_Number,Currency_Code,Requisition_Number,Requisition_Description,Posted_By,Posted_Date,AR_Invoice_Number,FCY,Employee_Number,Year_Month, Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_indirect_expense_kgdc":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_indirect_expense_kgdc')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select Entity_Name,Ledger_Name,Operating_Unit,Business_Category,Client_Geography,FUTURE1,FUTURE2, Interoperating_Unit, User_Type,Source, Function, Sub_Function,Service_line,Cost_Centre, Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_Code,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period_Name,Invoice_Number,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date, Created_By,Created_By___Desc,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator,Receipt_Number,Currency_Code,Requisition_Number,Requisition_Description,Posted_By,Posted_Date,AR_Invoice_Number,FCY,Employee_Number,Year_Month,  Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    
    if table == "trusted_csr_ngo_fund_util":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_ngo_fund_util')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select NGO_Name,Project_Type,Focus_area,Location,Month_and_Year,Total_CY_utilised,CY_Unspent,Entity_Name, Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_ngo_fund_budget":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_ngo_fund_budget')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select NGO_Name,Project_Type,Focus_area,Location,Month_and_Year,Budget_Amount,Entity_Name, Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_ngo_fund_util_kgdc":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_ngo_fund_util_kgdc')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select NGO_Name,Project_Type,Focus_area,Location,Month_and_Year,Total_CY_utilised,CY_Unspent,Entity_Name, Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

    if table == "trusted_csr_ngo_fund_budget_kgdc":
        year= spark.sql('select distinct Financial_year from kgsonedatadb.trusted_csr_ngo_fund_budget_kgdc')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')

        sampleDF = spark.sql(
            """select NGO_Name,Project_Type,Focus_area,Location,Month_and_Year,Budget_Amount,Entity_Name, Dated_On, File_Date,left(File_Date,6) as Month_Key,Financial_year from kgsonedatadb."""
            + table
        )
        

        sampleDF.write.format("jdbc").mode("append").option("url",jdbcUrl).option("dbtable",hist_table).option("user", username).option("password", password).save()
        print("table created :" + hist_table)

# COMMAND ----------

if processName == "gdc":

    if table == "trusted_gdc_onboarding_master":
        sampleDF=spark.sql('''select *,VDI_ID_RECEIVED_DATE AS File_Date from kgsonedatadb.'''+table)

        sampleDF.write \
       .format("jdbc") \
       .mode("overwrite") \
       .option("url",jdbcUrl) \
       .option("dbtable",hist_table) \
       .option("user", username) \
       .option("password", password) \
       .save()
    
    if table == 'trusted_gdc_opt':
        sampleDF=spark.sql('''select *,left(File_Date,6) as Month_Key from kgsonedatadb.'''+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

