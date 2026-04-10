# Databricks notebook source
# MAGIC %sql
# MAGIC delete from kgsonedatadb.trusted_hist_gdc_onboarding_master where EMP_ID=130510

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

# DBTITLE 1,Update Training Target Completion Date
spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.config_gdc_training_days b
               ON a.SERVICE_LINE=b.Service_Line  
               WHEN MATCHED  THEN  
                UPDATE SET a.TARGET_TRAINING_COMPLETION_DATE=a.DOJ+b.Total_days''')

# COMMAND ----------

# DBTITLE 1,Update Industry
spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a 
               USING  kgsonedatadb.config_gdc_industry_mapping b
               ON a.INDUSTRY_GROUP_COST_CENTER=b.Cost_Center  
               WHEN MATCHED  THEN  
                UPDATE SET a.INDUSTRY=b.Industry''')

# COMMAND ----------

# DBTITLE 1,Target Risk Completion Date
# MAGIC %sql
# MAGIC UPDATE kgsonedatadb.trusted_hist_gdc_onboarding_master SET TARGET_RISK_CLEARANCE_DATE=case when LND_HANDOVER_TO_RISK_TEAM is null then DATE_ADD(TARGET_TRAINING_COMPLETION_DATE,2) else DATE_ADD(LND_HANDOVER_TO_RISK_TEAM,2) end 

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
                                        WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=GETDATE())) then "BGV Pending & In Training (Scheduled)"

                                        WHEN ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<GETDATE())) then "BGV Pending & In Training (Scheduled)"
                                        
                                        WHEN ((BGV_CLEAR_DATE is not Null or BGV_CLEAR_DATE !="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE>=GETDATE())) then "In Training (Scheduled)"
                                        
                                        WHEN ((BGV_CLEAR_DATE is not Null or BGV_CLEAR_DATE !="") and (LND_HANDOVER_TO_RISK_TEAM is Null or LND_HANDOVER_TO_RISK_TEAM ="") and (TARGET_TRAINING_COMPLETION_DATE<GETDATE())) then "In Training (Delay)"
                                        
                                        when ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="")) then "BGV Pending & Risk Team clearance pending"

                                        when ((BGV_CLEAR_DATE is Null or BGV_CLEAR_DATE ="") and (RISK_TEAM_HANDOVER_TO_HRBP is not Null or RISK_TEAM_HANDOVER_TO_HRBP !="")) then "BGV Pending & HRBP clearance pending"
                                        
                                        WHEN (RISK_TEAM_HANDOVER_TO_HRBP is Null or RISK_TEAM_HANDOVER_TO_HRBP ="") then "Risk Team clearance pending"
                                        WHEN (HRBP_HANDOVER_TO_RMT_TEAM is Null or HRBP_HANDOVER_TO_RMT_TEAM ="") then "With HRBP"
                                        WHEN (RMT_HANDOVER_TO_INDUSTRY is Null or RMT_HANDOVER_TO_INDUSTRY ="") then "With RMT"
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
currentDf = spark.sql("select * from kgsonedatadb.trusted_hist_gdc_onboarding_master")
currentDf = currentDf.withColumn("DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS", business_days_udf("DOJ", "RMT_HANDOVER_TO_INDUSTRY")+1)
currentDf.createOrReplaceTempView("temp_2_df")

# COMMAND ----------

spark.sql('''MERGE INTO kgsonedatadb.trusted_hist_gdc_onboarding_master a
            USING  temp_2_df b
            ON a.PSID=b.PSID  
            WHEN MATCHED  THEN   
                UPDATE SET a.DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS=case when b.DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS is not null then b.DAYS_TAKEN_FROM_DOJ_TILL_HANDOVER_TO_BUSINESS else a.CANDIDATES_STATUS end''')


# COMMAND ----------


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
print(sampleDF.count())

sampleDF.write \
.format("jdbc") \
.mode("overwrite") \
.option("url",jdbcUrl) \
.option("dbtable",hist_table) \
.option("user", username) \
.option("password", password) \
.save() 

# COMMAND ----------

