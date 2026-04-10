# Databricks notebook source
# MAGIC %pip install numpy

# COMMAND ----------

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")


# COMMAND ----------

# DBTITLE 1,Import required functions
from datetime import datetime
import pytz
from pyspark.sql import functions as F
from pyspark.sql.functions import expr
from pyspark.sql.functions import when,to_timestamp
from pyspark.sql.types import FloatType,IntegerType

currentdatetime= datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')

# COMMAND ----------

# DBTITLE 1,Call common connection notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common compononets notebook
# MAGIC %run
# MAGIC /kgsonedata/common_utilities/common_components

# COMMAND ----------

# DBTITLE 1,Write Onboarding master table from analytics base file
if tableName=='analytics':
    
    currentDf = spark.sql("select * from kgsonedatadb.trusted_"+processName + "_"+ tableName+ " where PSID is not null and PSID!='WIP'")

    currentDf=currentDf.select("EMPLOYEE_NAME","PSID","DESIGNATION","LOCATION","DOJ","GENDER","INDUSTRY_GROUP_COST_CENTER","INDUSTRY","TYPE_OF_HIRE_FTE_FTS_SUB_CON","CAMPUS_PROFESSIONAL","INDIA_EMAIL_ID","US_VDI_EMAIL_ID","File_Date","PERSONAL_EMAIL_ID","PM","PM_EMAIL_ID","BUDDY_NAME","BUDDY_EMAIL_ID","Dated_On")

    currentDf=currentDf.withColumnRenamed("EMPLOYEE_NAME","EMP_NAME") \
                       .withColumnRenamed("TYPE_OF_HIRE_FTE_FTS_SUB_CON","EMP_CATEGORY") \
                       .withColumnRenamed("CAMPUS_PROFESSIONAL","LATERAL_CAMPUS") \
                       .withColumnRenamed("INDIA_EMAIL_ID","INDIA_ID") \
                       .withColumnRenamed("US_VDI_EMAIL_ID","VDI_ID") \
                       .withColumnRenamed("File_Date","VDI_ID_RECEIVED_DATE") \
                       .withColumnRenamed("PERSONAL_EMAIL_ID","CANDIDATE_PERSONAL_EMAIL_ID") \
                       .withColumn("Dated_On",to_timestamp(lit(currentdatetime)))

    currentDf.createOrReplaceTempView("temp_df")

    temp_df=spark.sql("SELECT EMP_NAME,CAST(NULL AS STRING) AS EMP_ID,PSID,DESIGNATION,LOCATION,DOJ,GENDER,CAST(NULL AS STRING) AS UG,CAST(NULL AS STRING) AS PG,CAST(NULL AS STRING) AS HIGHEST_QUALIFICATION,INDUSTRY_GROUP_COST_CENTER,CAST(NULL AS STRING) AS SERVICE_LINE,INDUSTRY,CASE WHEN EMP_CATEGORY=='Staff on Probation' THEN 'FTE' WHEN EMP_CATEGORY=='Fixed Term Staff' THEN 'FTS' END AS EMP_CATEGORY,LATERAL_CAMPUS,CAST(NULL AS STRING) AS ATTENDED_GENESIS_KBS,CAST(NULL AS INT) AS NO_OF_COURSES_PENDING,CAST(NULL AS STRING) AS BGV_STATUS,CAST(NULL AS DATE) AS BGV_CLEAR_DATE,CAST(NULL AS DATE) AS LND_HANDOVER_TO_RISK_TEAM,CAST(NULL AS DATE) AS RISK_TEAM_HANDOVER_TO_HRBP,CAST(NULL AS DATE) AS HRBP_HANDOVER_TO_RMT_TEAM,CAST(NULL AS DATE) AS RMT_HANDOVER_TO_INDUSTRY,'BCP Access Blocked' AS BCP,INDIA_ID,VDI_ID,CAST(NULL AS DATE)  AS VDI_ID_RECEIVED_DATE,CAST(NULL AS STRING) AS JOINERS_STATUS,CAST(NULL AS STRING) AS BYOD,CAST(NULL AS STRING) AS LAPTOP_DELIVERY_STATUS,CAST(NULL AS STRING) AS LAPTOP_DISPATCH_DATE,CAST(NULL AS DATE) AS LAPTOP_REACHED_ON,CAST(NULL AS STRING) AS CANDIDATES_STATUS,DATE_FORMAT(DOJ,'MMM-yyyy') AS DOJ_BY_MONTH,CAST(NULL AS STRING) AS ONBOARD_SUMMARY_DOJ,CAST(NULL AS DATE) AS TARGET_TRAINING_COMPLETION_DATE,CAST(NULL AS DATE) AS TARGET_RISK_CLEARANCE_DATE,CAST(NULL AS DATE) AS BGV_SLA_DATE,CANDIDATE_PERSONAL_EMAIL_ID,'Joined from office' as CANDIDATE_JOINED_OFFICE_HOME,CAST(NULL AS STRING) AS PRIOR_INDUSTRY,PM,PM_EMAIL_ID,BUDDY_NAME,BUDDY_EMAIL_ID,CAST(NULL AS INT) AS TIME_TAKEN_TO_RECEIVE_LND_CLEARANCE_DATE_FROM_LAPTOP_RECEIVED_DATE,CAST(NULL AS INT) AS LND_DELAY_IN_DAYS,CAST(NULL AS STRING) AS DELAY_REASONS,CAST(NULL AS INT) AS DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS,VDI_ID_RECEIVED_DATE AS File_Date,CAST( NULL AS STRING) AS LnD_Team_Comments ,CAST (NULL AS STRING) AS Risk_Team_Comments,CASE WHEN ((EMP_NAME IS NULL) or (PSID IS NULL) or (DESIGNATION IS NULL) OR (LOCATION IS NULL) OR (DOJ IS NULL )OR (GENDER IS NULL) OR (INDUSTRY_GROUP_COST_CENTER IS NULL)  OR (INDUSTRY IS NULL) OR (EMP_CATEGORY IS NULL) OR (LATERAL_CAMPUS IS NULL) OR (INDIA_ID IS NULL) OR (VDI_ID IS NULL) OR (CANDIDATE_PERSONAL_EMAIL_ID IS NULL) OR (PM IS NULL) OR (PM_EMAIL_ID IS NULL) OR (BUDDY_NAME IS NULL) OR (BUDDY_EMAIL_ID IS NULL))  THEN 'InComplete' ELSE 'Complete' END AS DATABASE_STATUS FROM temp_df")

    display(temp_df)

    temp_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "True")\
    .option("path",trusted_curr_savepath_url+"gdc"+"/"+"onboarding_master") \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb.trusted_gdc_onboarding_master")

    currentDf = spark.sql("select * from kgsonedatadb.trusted_gdc_onboarding_master")
    currentDf=currentDf.withColumn("Dated_On", to_timestamp(lit(currentdatetime)))
    display(currentDf)
    
    
    currentDf.write \
    .mode("append") \
    .format("delta") \
    .option("mergeschema","true")  \
    .option("path",trusted_hist_savepath_url+"gdc"+"/"+"onboarding_master") \
    .option("compression","snappy") \
    .saveAsTable("kgsonedatadb.trusted_hist_gdc_onboarding_master")
    
  

# COMMAND ----------

#Update PSID Having only integer
if tableName=='analytics':
    spark.sql('''Update kgsonedatadb.trusted_hist_gdc_onboarding_master
                 SET PSID=REGEXP_REPLACE(PSID,'[^0-9]','')''')

# COMMAND ----------

# DBTITLE 1,Update Service Line 
#Update service line column by joining with mapping table
if tableName=='analytics':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.config_gdc_serviceline_mapping b
               ON a.INDUSTRY_GROUP_COST_CENTER=b.Industry_Group_Cost_Center  
               WHEN MATCHED  THEN  
                UPDATE SET a.SERVICE_LINE=b.Service_Line''')
    

# COMMAND ----------

# DBTITLE 1,Update Training Target Completion Date
if tableName=='analytics':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.config_gdc_training_days b
               ON a.SERVICE_LINE=b.Service_Line  
               WHEN MATCHED  THEN  
                UPDATE SET a.TARGET_TRAINING_COMPLETION_DATE=a.DOJ+b.Total_days''')

# COMMAND ----------

# DBTITLE 1,Update Industry
if tableName=='analytics':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.config_gdc_industry_mapping b
               ON a.INDUSTRY_GROUP_COST_CENTER=b.Cost_Center  
               WHEN MATCHED  THEN  
                UPDATE SET a.INDUSTRY=b.Industry''')

# COMMAND ----------

# DBTITLE 1,Update Laptop Delivery Status
#except hyderabad location update laptop delivery status for all other locations with laptop reached on= DOJ+1
if tableName=='analytics':
    spark.sql('''Update kgsonedatadb.trusted_hist_gdc_onboarding_master
                 SET LAPTOP_DELIVERY_STATUS="Delivered",LAPTOP_DISPATCH_DATE="collected by employee",LAPTOP_REACHED_ON= DOJ+INTERVAL 1 DAY WHERE LOCATION!="Hyderabad"''')

# COMMAND ----------

# DBTITLE 1,Update Lateral/Campus 
#PG Campus - A1 - US Core Audit & Canada Audit
#CA Campus - A2 - US Core Audit & Canada Audit
#Campus - A1/A2 - Other
#Professional - Lateral
if tableName=='analytics':
    spark.sql('''Update kgsonedatadb.trusted_hist_gdc_onboarding_master
              SET LATERAL_CAMPUS= CASE 
                                        WHEN ((DESIGNATION="Associate 1") and ((SERVICE_LINE="Canada Audit") or (SERVICE_LINE="US Core Audit"))) then "PG Campus" WHEN ((DESIGNATION="Associate 2") and ((SERVICE_LINE="Canada Audit") or (SERVICE_LINE="US Core Audit"))) then "CA Campus" when (LATERAL_CAMPUS="Professional") then "Lateral" 
                                        else "Campus"
                                    end''')

# COMMAND ----------

# DBTITLE 1,Creation of temp view to Get KPMG ID and Joiners status
#join hc tables and find kpmg id and joiners status using PSID
if tableName  in ("absconding","active_hc","attrition","attrition_pipeline","long_leave","fts","secondee","transfers","contractors"):

    spark.sql('''CREATE OR REPLACE TEMPORARY VIEW hc_vw AS (
    select a.EMP_ID as OnBoarding_EMP_ID,a.PSID,b.KPMG_EMP_ID as Active_Employee_ID,c.EMP_ID as FTS_ID,d.KPMG_ID as Secondee_ID,e.KPMG_ID as Transfers_ID,f.KPMG_EMP_ID as Attrition_Pipeline_ID,g.KPMG_ID as Attrition_ID,h.KPMG_ID as Absconding_ID,i.KPMG_ID as LongLeave_ID,coalesce(b.KPMG_EMP_ID,c.EMP_ID,d.KPMG_ID,e.KPMG_ID,f.KPMG_EMP_ID,g.KPMG_ID,h.KPMG_ID,i.KPMG_ID,z.CANDIDATE__ID) as Final_KPMG_ID,coalesce(b.COST_CENTER,c.NEW_COST_CENTER,z.NEW_COST_CENTER,G.COST_CENTER) as HC_Cost_Center,
    case when (a.PSID=b.PS_ID or a.PSID=c.PS_ID or a.PSID=d.PSID or a.PSID=e.US_ID or a.PSID=z.PS_ID) and (i.PS_ID is null) and (f.PS_ID is null) and (g.PS_ID is null) then 'Active'
    when a.PSID=f.PS_ID then 'Pipeline Attrition'
    when a.PSID=g.PS_ID then 'Attrition'    
    when a.PSID=h.PS_ID then 'Abscondidng'
    when a.PSID=i.PS_ID then 'Long Leave' end as Candidate_Status,b.UG,b.PG,b.HIGHEST_QUALIFICATION,b.KPMG_DOJ,g.APPROVED_LWD as Attrition_LWD,coalesce(f.APPROVED_LWD,i.LEAVE_END_DATE,g.APPROVED_LWD) as LWD
    from kgsonedatadb.trusted_hist_gdc_onboarding_master a left join kgsonedatadb.trusted_gdc_active_hc b on a.PSID=b.PS_ID
    left join kgsonedatadb.trusted_gdc_fts c on a.PSID=c.PS_ID 
    left join kgsonedatadb.trusted_gdc_secondee d on a.PSID=d.PSID 
    left join kgsonedatadb.trusted_gdc_transfers e on a.PSID=e.US_ID 
    left join kgsonedatadb.trusted_gdc_contractors z on a.PSID=z.PS_ID 
    left join kgsonedatadb.trusted_gdc_attrition_pipeline f on a.PSID=f.PS_ID
    left join kgsonedatadb.trusted_gdc_attrition g on a.PSID=g.PS_ID
    left join kgsonedatadb.trusted_gdc_absconding h on a.PSID=h.PS_ID
    left join kgsonedatadb.trusted_gdc_long_leave i on a.PSID=i.PS_ID
    where a.PSID!="WIP")''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hc_vw where PSID in ('4166747','4165860')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kgsonedatadb.trusted_gdc_active_hc

# COMMAND ----------

# MAGIC %sql
# MAGIC Update kgsonedatadb.trusted_hist_gdc_bcp
# MAGIC                  SET PSID=REGEXP_REPLACE(PSID,'[^0-9]','')  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hc_vw where PSID in ('4182569','4181604','4180577','4167096','4133130')

# COMMAND ----------

if tableName  in ("absconding","active_hc","attrition","attrition_pipeline","long_leave","fts","secondee","transfers","contractors"):
    spark.sql('''create or replace temporary view hc_unique_vw as select * from ( select PSID,Final_KPMG_ID,HC_Cost_Center,case when Candidate_Status='Attrition' and Attrition_LWD<KPMG_DOJ then 'Active' when Candidate_Status='Attrition' and Attrition_LWD>KPMG_DOJ then 'Attrition' else Candidate_Status end as Candidate_Status,UG,PG,HIGHEST_QUALIFICATION,CAST(LWD as string) as LWD,row_number() over(partition by PSID order by Candidate_Status DESC) as rn from hc_vw) t where rn=1''')

# COMMAND ----------

# DBTITLE 1,Update KPMG ID and Joiners status by joining with View
if tableName  in ("absconding","active_hc","attrition","attrition_pipeline","long_leave","fts","secondee","transfers","contractors"):
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a
    USING  hc_unique_vw b
    ON a.PSID=b.PSID 
    WHEN MATCHED  THEN  
    UPDATE SET a.EMP_ID=b.Final_KPMG_ID,a.Joiners_Status=b.Candidate_Status,a.INDUSTRY_GROUP_COST_CENTER=b.HC_Cost_Center,a.UG=b.UG,a.PG=b.PG,a.HIGHEST_QUALIFICATION=b.HIGHEST_QUALIFICATION,a.BYOD=b.LWD''')

# COMMAND ----------

# DBTITLE 1,Updating Service line based on cost center change in HC File
if tableName  in ("absconding","active_hc","attrition","attrition_pipeline","long_leave","fts","secondee","transfers","contractors"):
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
                USING  kgsonedatadb.config_gdc_serviceline_mapping b
                ON a.INDUSTRY_GROUP_COST_CENTER=b.Industry_Group_Cost_Center  
                WHEN MATCHED  THEN  
                    UPDATE SET a.SERVICE_LINE=b.Service_Line''')

# COMMAND ----------

# DBTITLE 1,Update BGV Status and BGV Clearance Date
if tableName=="bgv_check":
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.trusted_gdc_bgv_check b
               ON a.EMP_ID=b.EMPLOYEE_ID  and a.EMP_ID is NOT NULL and b.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_="Yes" and a.BGV_CLEAR_DATE is NULL
               WHEN MATCHED  THEN  
                UPDATE SET a.BGV_STATUS=b.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_,a.BGV_CLEAR_DATE=date_format(TO_DATE(b.File_Date,'yyyyMMdd'),'yyyy-MM-dd')''')
    
    

# COMMAND ----------

# DBTITLE 1,For those who didn't get BGV clearance
if tableName=="bgv_check":
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.trusted_gdc_bgv_check b
               ON a.EMP_ID=b.EMPLOYEE_ID  and a.EMP_ID is NOT NULL and b.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_="No" and a.BGV_CLEAR_DATE is NULL
               WHEN MATCHED  THEN  
                UPDATE SET a.BGV_STATUS=b.BGV_CHECK_COMPLETED_ALONG_WITH_WAIVER_EXCEPT_CEA_AND_CAMPUS_EDUCATION_,a.BGV_CLEAR_DATE=NULL''')
    
    

# COMMAND ----------

if tableName=="newjoiners_status":
    spark.sql('''create or replace temporary view new_joiners_vw as (select * from (select *,row_number() over(partition by PSID order by employee_name) as rn from kgsonedatadb.trusted_gdc_newjoiners_status) a where rn=1)''')

# COMMAND ----------

# DBTITLE 1,Update Genesis Date and Pending Course columns
if tableName=="newjoiners_status":
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING new_joiners_vw b
               ON a.PSID=b.PSID and a.EMP_NAME=b.Employee_Name
               WHEN MATCHED  THEN  
                UPDATE SET a.ATTENDED_GENESIS_KBS=b.GENESIS_DATE,a.NO_OF_COURSES_PENDING=case when b.TOTAL_PNG_COURSE<0 then 0 else b.TOTAL_PNG_COURSE end''')
    
    

# COMMAND ----------

# DBTITLE 1,BCP Column Update
if tableName=='BCP':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.trusted_gdc_bcp b
               ON a.PSID=b.PSID   
               WHEN MATCHED  THEN  
                UPDATE SET a.BCP="BCP Access re-activated"''')

# COMMAND ----------

# DBTITLE 1,Update Laptop delivery details for hyderabad location
if tableName=='laptop_delivery':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.trusted_gdc_laptop_delivery b
               ON a.PSID=b.PSID 
               WHEN MATCHED  THEN  
                UPDATE SET a.LAPTOP_DELIVERY_STATUS= case when b.LAPTOP_DISPATCHED_DATE is not null and b.LAPTOP_DELIVERY_DATE is not null then 'Delivered' else 'not delivered' end,a.LAPTOP_DISPATCH_DATE=b.LAPTOP_DISPATCHED_DATE,a.LAPTOP_REACHED_ON=b.LAPTOP_DELIVERY_DATE''')

# COMMAND ----------

# DBTITLE 1,LND Clearance date update
if tableName=='lnd_mandatory_trainings':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.trusted_gdc_lnd_mandatory_trainings b
               ON a.PSID=b.PSID  
               WHEN MATCHED  THEN  
                UPDATE SET a.LND_HANDOVER_TO_RISK_TEAM
                =date_format(TO_DATE(b.File_Date,'yyyyMMdd'),'yyyy-MM-dd') ''')

# COMMAND ----------

# DBTITLE 1,Risk Clearance Date
if tableName=='risk_check':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.trusted_gdc_risk_check b
               ON a.PSID=b.PSID   
               WHEN MATCHED  THEN  
                UPDATE SET a.RISK_TEAM_HANDOVER_TO_HRBP
                =date_format(TO_DATE(b.File_Date,'yyyyMMdd'),'yyyy-MM-dd') ''')

# COMMAND ----------

if tableName=="hrbp_handover":
    spark.sql('''create or replace temporary view hrbp_handover_vw as (select * from (select *,row_number() over(partition by PSID,employee_name order by DATE_OF_HRBP_HANDOVER desc) as rn from kgsonedatadb.trusted_gdc_hrbp_handover) a where rn=1)''')

# COMMAND ----------

# DBTITLE 1,HRBP Clearance date
if tableName=='hrbp_handover':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  hrbp_handover_vw b
               ON a.PSID=b.PSID  and a.EMP_ID=b.INDIA_EMP_ID
               WHEN MATCHED  THEN  
                UPDATE SET a.HRBP_HANDOVER_TO_RMT_TEAM
                =date_format(TO_DATE(b.DATE_OF_HRBP_HANDOVER,'yyyy-MM-dd'),'yyyy-MM-dd') ''')

# COMMAND ----------

# DBTITLE 1,RMT Clearance same as HRBP for SL GTA,GTS,Canada Audit
if tableName=='hrbp_handover':
    spark.sql('''update kgsonedatadb.trusted_hist_gdc_onboarding_master set RMT_HANDOVER_TO_INDUSTRY=case when SERVICE_LINE in ('GTA','GTS','Canada Audit','DPP','QMCI') then HRBP_HANDOVER_TO_RMT_TEAM else null end''')

# COMMAND ----------

if tableName=="ready_for_production":
    spark.sql('''create or replace temporary view rmt_vw as (select * from (select *,row_number() over(partition by PSID,EMPID order by HANDOVER desc) as rn from kgsonedatadb.trusted_gdc_ready_for_production) a where rn=1)''')

# COMMAND ----------

# DBTITLE 1,RMT Clearance date
if tableName=='ready_for_production':
    spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  rmt_vw b
               ON a.PSID=b.PSID and a.EMP_ID=b.EMPID and a.SERVICE_LINE not in ('GTA','GTS','Canada Audit')
               WHEN MATCHED  THEN  
                UPDATE SET a.RMT_HANDOVER_TO_INDUSTRY=
                date_format(TO_DATE(b.HANDOVER,'yyyy-MM-dd'),'yyyy-MM-dd')''')

# COMMAND ----------

# DBTITLE 1,Candidate Status
# IF col( "EMP_NAME") is null then "",
# IF col("BGV_CLEAR_DATE") is null  then "BGV Pending & In Training (Scheduled)",
# IF col("LND_HANDOVER_TO_RISK_TEAM") is null then "In Training (Scheduled)",
# IF col("RISK_TEAM_HANDOVER_TO_HRBP") ="" then "Risk Team clearance pending",
# IF col("HRBP_HANDOVER_TO_RMT_TEAM") is null  then "With HRBP",
# IF col("RMT_HANDOVER_TO_INDUSTRY") ="" then "With RMT"
# else "Handed over to Business"


spark.sql('''Update kgsonedatadb.trusted_hist_gdc_onboarding_master 
            SET CANDIDATES_STATUS= CASE 
                                        WHEN (EMP_NAME is Null or EMP_NAME = "") then ""
                                        WHEN ((CANDIDATES_STATUS='Attrition') or (CANDIDATES_STATUS='Absconding') or (CANDIDATES_STATUS='Handed over to Business')) then CANDIDATES_STATUS
                                        WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE='null' or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM='null' or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=CURRENT_DATE())) then "BGV Pending & In Training (Scheduled)"

                                        WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE='null' or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM='null' or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<CURRENT_DATE())) then "BGV Pending & In Training (Delay)"
                                        
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
                                
                ''')

# COMMAND ----------

# DBTITLE 1,TIME_TAKEN_TO_RECEIVE_LND_CLEARANCE_DATE_FROM_LAPTOP_RECEIVED_DATE
# if col("LND_HANDOVER_TO_RISK_TEAM") is null and col("TARGET_TRAINING_COMPLETION_DATE") <=today then today - col("LAPTOP_REACHED_ON" )
# if col("LND_HANDOVER_TO_RISK_TEAM") is null and col("TARGET_TRAINING_COMPLETION_DATE") > today then  col("TARGET_TRAINING_COMPLETION_DATE") - col("LAPTOP_REACHED_ON")
# else col("LND_HANDOVER_TO_RISK_TEAM") - col("LAPTOP_REACHED_ON")
#go with DOJ for other locations only Hyderabad should calculate with Laptop reached on

spark.sql('''Update kgsonedatadb.trusted_hist_gdc_onboarding_master
        
            SET TIME_TAKEN_TO_RECEIVE_LND_CLEARANCE_DATE_FROM_LAPTOP_RECEIVED_DATE= CASE 
                                        WHEN ((LND_HANDOVER_TO_RISK_TEAM is Null) and (TARGET_TRAINING_COMPLETION_DATE <= current_date()) and (LOCATION!='Hyderabad')) then (current_date() - DOJ)

                                        WHEN ((LND_HANDOVER_TO_RISK_TEAM is Null) and (TARGET_TRAINING_COMPLETION_DATE > current_date()) and (LOCATION!='Hyderabad') ) then (TARGET_TRAINING_COMPLETION_DATE - DOJ)

                                        when ((LND_HANDOVER_TO_RISK_TEAM is not null) and (Location!='Hyderabad')) then (LND_HANDOVER_TO_RISK_TEAM - DOJ)

                                        WHEN ( (LND_HANDOVER_TO_RISK_TEAM is Null) and (TARGET_TRAINING_COMPLETION_DATE <= current_date()) and (LOCATION='Hyderabad') ) then (current_date() - LAPTOP_REACHED_ON)

                                        WHEN ( (LND_HANDOVER_TO_RISK_TEAM is Null) and (TARGET_TRAINING_COMPLETION_DATE > current_date()) and (LOCATION='Hyderabad') ) then (TARGET_TRAINING_COMPLETION_DATE - LAPTOP_REACHED_ON)

                                        else (LND_HANDOVER_TO_RISK_TEAM - LAPTOP_REACHED_ON)
                                    end''')

# COMMAND ----------

# DBTITLE 1,LND_DELAY_IN_DAYS
# VLOOKUP using "SERVICE_LINE" column in the "IGH" sheet and fetch the "Total_days"
# if col("TIME_TAKEN_TO_RECEIVE_LND_CLEARANCE_DATE_FROM_LAPTOP_RECEIVED_DATE")  <= col( "Total_days")   then "No delay"
# else col("TIME_TAKEN_TO_RECEIVE_LND_CLEARANCE_DATE_FROM_LAPTOP_RECEIVED_DATE") - col("Total_days")  
spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a
            USING  kgsonedatadb.config_gdc_training_days b
            ON a.SERVICE_LINE=b.Service_Line  
            WHEN MATCHED  THEN   
                UPDATE SET a.LND_DELAY_IN_DAYS= CASE 
                WHEN ( a.TIME_TAKEN_TO_RECEIVE_LND_CLEARANCE_DATE_FROM_LAPTOP_RECEIVED_DATE <= b.Total_days) then "No delay"
                else (a.TIME_TAKEN_TO_RECEIVE_LND_CLEARANCE_DATE_FROM_LAPTOP_RECEIVED_DATE - b.Total_days) end
                ''')

# COMMAND ----------

# DBTITLE 1,Days Taken from DOJ till Handover to business
# Apply the UDF to calculate business days
#calculate  business days btw DOJ & RMT_HANDOVER_TO_INDUSTRY
#VDI Received date-DOJ+1
#BGV SLA Date-DOJ+25 working days
#TARGET_RISK_CLEARANCE_DATE when LND_HANDOVER_TO_RISK_TEAM is blank then TARGET_TRAINING_COMPLETION_DATE + 2 working days else LND_HANDOVER_TO_RISK_TEAM + 2 working days

currentDf = spark.sql("select * from kgsonedatadb.trusted_hist_gdc_onboarding_master")
currentDf = currentDf.withColumn("DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS", business_days_udf("DOJ", "RMT_HANDOVER_TO_INDUSTRY")+1)\
                    .withColumn("VDI_ID_RECEIVED_DATE",working_days_udf("DOJ",lit("1"))) \
                    .withColumn("BGV_SLA_DATE",working_days_udf("DOJ",lit("25"))) \
                    .withColumn("TARGET_RISK_CLEARANCE_DATE",when(col("LND_HANDOVER_TO_RISK_TEAM").isNull(),working_days_udf("TARGET_TRAINING_COMPLETION_DATE",lit("2"))).otherwise(working_days_udf("LND_HANDOVER_TO_RISK_TEAM",lit("2"))))             
currentDf.createOrReplaceTempView("temp_2_df")

# COMMAND ----------

spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a
            USING  temp_2_df b
            ON a.PSID=b.PSID  
            WHEN MATCHED  THEN   
                UPDATE SET a.DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS=case when b.DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS is not null then b.DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS else a.CANDIDATES_STATUS end,a.BGV_SLA_DATE=b.BGV_SLA_DATE,a.VDI_ID_RECEIVED_DATE=b.VDI_ID_RECEIVED_DATE,a.TARGET_RISK_CLEARANCE_DATE=b.TARGET_RISK_CLEARANCE_DATE''')


# COMMAND ----------

# DBTITLE 1,Load Data to SQL DB

PSID= spark.sql('select distinct PSID from kgsonedatadb.trusted_hist_gdc_onboarding_master')
PSID=PSID.select('PSID').rdd.flatMap(lambda x: x).collect()
PSID=tuple(PSID)
print(PSID)
sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'trusted_hist_gdc_onboarding_master'"
cursor.execute(sql_command)
result = cursor.fetchone()[0]
if result == 1:
    query=(""" DELETE FROM dbo.trusted_hist_gdc_onboarding_master where PSID in {} """).format(PSID)
    print(query)
    conn.execute(query)
    conn.commit()
    print('delete executed')

hist_table="dbo.trusted_hist_gdc_onboarding_master"
sampleDF=spark.sql('select * from kgsonedatadb.trusted_hist_gdc_onboarding_master')

sampleDF.write \
.format("jdbc") \
.mode("overwrite") \
.option("url",jdbcUrl) \
.option("dbtable",hist_table) \
.option("user", username) \
.option("password", password) \
.save() 

# COMMAND ----------

