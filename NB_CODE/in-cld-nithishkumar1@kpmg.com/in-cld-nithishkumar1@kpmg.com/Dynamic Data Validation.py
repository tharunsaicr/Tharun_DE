# Databricks notebook source
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.functions import unix_timestamp,lit


# COMMAND ----------

# Get the current date
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %run /kgsonedata/common_utilities/connection_configuration

# COMMAND ----------

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

sqlschema="dbo"
deltaschema="kgsonedatadb"
baddeltaschema="kgsonedatadb_badrecords"

# COMMAND ----------

summary_data=[]

# COMMAND ----------

NA_Tables=[]

# COMMAND ----------

check_azure_trusted_tables=['GPS','Rock','Year_End','upcoming_joiners','secondment_details','Employee_Details','Resigned_And_Left','Secondee_Outward','Sabbatical','Maternity_Cases']

# COMMAND ----------

# Neglect_Tables=['k_check','project_code_dump','Contractor_Intern_Daily_Exit_Report','Optional_Business_Application_Installation','Software_Product_License_Inventory','Uninstall_An_Application','holiday_list','post_hire','progress_sheet_joined_candidate','waiver_tracker_closed','waiver_tracker_open']

# COMMAND ----------

# Query the config table to get all process names, table names, and corresponding execution log table names

config_table_query = f"SELECT a.Process_name,a.File_Name,b.SourceFileName,b.SourceFormat FROM [dbo].[config_table] a left join [dbo].[config_table_excel_to_csv] b  on replace(a.File_Name,'_',' ')=replace(b.TargetFileName,'_',' ') and replace(a.Process_name,'_',' ')=replace(b.ProcessName,'_',' ')  where a.Process_name in ('jml','talent_acquisition','employee_engagement','headcount','lnd','it','headcount_monthly','BGV','global_mobility') and a.File_Name not in ('glms_details','kva_details')" 
 

config_table_df = spark.read\
  .format("jdbc")\
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
  .option("url", jdbcUrl)\
  .option("query",config_table_query)\
  .option("user", username)\
  .option("password", password)\
  .load()

# COMMAND ----------


# Loop through rows in the config table

for row in config_table_df.collect():

    process_name = row["Process_name"]
    # print(process_name)
    table_name = row["File_Name"]
    # print(table_name)
    SourceFileName=row["SourceFileName"]
    SourceFormat=row["SourceFormat"]
    

    # Define the raw delta table name and trusted table name based on the process name and table name

    raw_table_name = f"raw_hist_{process_name}_{table_name}"

    trusted_table_name = f"trusted_hist_{process_name}_{table_name}"

    bad_table_name=f"{trusted_table_name}_bad"
  

    # Execute SQL queries to get the maximum file date from the execution log table
    
    
 
    max_file_date_table_query= "select * from (select processName, DeltaTableName,FileName,FileDate,Status,EndTime,row_number() over (partition by processName,DeltaTableName order by FileDate desc,EndTime desc) as rn from [dbo].[execution_log])a  where rn=1 and DeltaTableName='{}' and processName='{}'".format(table_name,process_name)

    
    
    max_file_date_table_df = spark.read\
    .format("jdbc")\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .option("url", jdbcUrl)\
    .option("query",max_file_date_table_query)\
    .option("user", username)\
    .option("password", password)\
    .load()

    
    


    max_file_date_table_count=max_file_date_table_df.count()

    

    if max_file_date_table_count>0:
        max_file_date_var=max_file_date_table_df.first()["FileDate"]
        # print(max_file_date_var)
        status_var=max_file_date_table_df.first()["Status"]
        File_Name=max_file_date_table_df.first()["FileName"]
    
    
        table_exists_query= "SELECT COUNT(1) as table_exists FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}'".format(trusted_table_name)
        table_exists_df = spark.read\
            .format("jdbc")\
            .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .option("url", jdbcUrl)\
            .option("query",table_exists_query)\
            .option("user", username)\
            .option("password", password)\
            .load()

        

        table_exists=table_exists_df.first()[0]
    
        
        bad_record_table=baddeltaschema+'.'+bad_table_name
        raw_hist_table_name=deltaschema+'.'+raw_table_name
        trusted_hist_table_name=deltaschema+'.'+trusted_table_name
        if spark.catalog.tableExists(raw_hist_table_name):
            raw_table_intial_count = spark.sql(f"SELECT COUNT(1) FROM {deltaschema}.{raw_table_name}").collect()[0][0]

            if raw_table_intial_count>0: 
                    
                # Execute SQL queries to get counts for the tables using the max file date

                raw_max_file_dated_on=spark.sql(f"SELECT max(Dated_on) FROM {deltaschema}.{raw_table_name} WHERE file_date = '{max_file_date_var}'").collect()[0][0]

                # print(raw_max_file_dated_on)

                if table_name not in check_azure_trusted_tables:
                    raw_count = spark.sql(f"SELECT COUNT(1) FROM {deltaschema}.{raw_table_name} WHERE file_date = '{max_file_date_var}' and dated_on='{raw_max_file_dated_on}'").collect()[0][0]
                else:
                    raw_count='NA'

            else:
                raw_count=0
        else:
            raw_count='NA'
            
            NA_Tables.append(raw_table_name)
            
        if spark.catalog.tableExists(trusted_hist_table_name):
            
            trusted_table_intial_count = spark.sql(f"SELECT COUNT(*) FROM {deltaschema}.{trusted_table_name}").collect()[0][0]

            if trusted_table_intial_count>0:

                trusted_max_file_dated_on = spark.sql(f"SELECT max(dated_on) FROM {deltaschema}.{trusted_table_name} WHERE file_date = '{max_file_date_var}'").collect()[0][0]

                # print(trusted_max_file_dated_on)
                
                trusted_count = spark.sql(f"SELECT COUNT(*) FROM {deltaschema}.{trusted_table_name} WHERE file_date = '{max_file_date_var}'and dated_on='{trusted_max_file_dated_on}'").collect()[0][0]
        
            else:
                trusted_count=0
        else:
            trusted_count='NA'
            
            NA_Tables.append(trusted_table_name)

        if table_exists==1:

            azure_sql_intial_count_query= "SELECT COUNT(*) as Count_of_records FROM {}.{} WHERE File_Date ='{}'".format(sqlschema,trusted_table_name,max_file_date_var)
            
            azure_sql_intial_count_df = spark.read\
                .format("jdbc")\
                .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                .option("url", jdbcUrl)\
                .option("query",azure_sql_intial_count_query)\
                .option("user", username)\
                .option("password", password)\
                .load()
            azure_sql_intial_count=azure_sql_intial_count_df.first()[0]
            

            if azure_sql_intial_count>=1:
            
                azure_sql_max_file_dated_on= "SELECT max(dated_on) as maximum_dated_on FROM {}.{} WHERE File_Date ='{}'".format(sqlschema,trusted_table_name,max_file_date_var)
                azure_sql_max_dated_on_df = spark.read\
                    .format("jdbc")\
                    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                    .option("url", jdbcUrl)\
                    .option("query",azure_sql_max_file_dated_on)\
                    .option("user", username)\
                    .option("password", password)\
                    .load()
                azure_sql_max_dated_on=azure_sql_max_dated_on_df.first()[0]
                azure_sql_max_dated_on=str(azure_sql_max_dated_on)
                if "." in azure_sql_max_dated_on:
                    azure_sql_max_dated_on=azure_sql_max_dated_on[:19]
                else:
                    azure_sql_max_dated_on=azure_sql_max_dated_on
                
                
            
            
        
                azure_sql_count_query= "SELECT COUNT(*) as Count_of_records FROM {}.{} WHERE File_Date ='{}' and Dated_on='{}'".format(sqlschema,trusted_table_name,max_file_date_var,azure_sql_max_dated_on)
                

                azure_sql_count_df = spark.read\
                    .format("jdbc")\
                    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                    .option("url", jdbcUrl)\
                    .option("query",azure_sql_count_query)\
                    .option("user", username)\
                    .option("password", password)\
                    .load()
                azure_sql_count=azure_sql_count_df.first()[0]
                # print(azure_sql_count)

            else:
                azure_sql_count=0
        else:
            azure_sql_count='NA'
            
            NA_Tables.append(table_name)
             
        if spark.catalog.tableExists(bad_record_table):

            bad_record_intial_count= spark.sql(f"SELECT COUNT(*) FROM {baddeltaschema}.{bad_table_name}").collect()[0][0]

            if bad_record_intial_count>0:
                if process_name!='lnd':
                    bad_record_dated_on = spark.sql(f"SELECT max(dated_on) FROM {baddeltaschema}.{bad_table_name} WHERE file_date = '{max_file_date_var}'").collect()[0][0]
                    

                    bad_record_count= spark.sql(f"SELECT COUNT(*) FROM {baddeltaschema}.{bad_table_name} WHERE file_date = '{max_file_date_var}' and dated_on='{bad_record_dated_on}'").collect()[0][0]
                else:
                    bad_record_count= spark.sql(f"SELECT COUNT(*) FROM {baddeltaschema}.{bad_table_name} WHERE file_date = '{max_file_date_var}'").collect()[0][0]

            else:
                bad_record_count=0
        else:
            bad_record_count='NA'
        
            NA_Tables.append(bad_table_name)   
            # Check if all counts match or mismatch
        
        if table_name in check_azure_trusted_tables:
            count_match = trusted_count == azure_sql_count
        elif (raw_count=='NA' and trusted_count=='NA' and azure_sql_count=='NA'):
                count_match='NA'
        else:
            count_match = raw_count == trusted_count == azure_sql_count

            

            # Add data to summary list

        summary_data.append((process_name, table_name,File_Name,SourceFileName,SourceFormat,status_var,max_file_date_var, str(azure_sql_count), str(raw_count), str(trusted_count), str(bad_record_count),str(count_match)))     

    else:
        summary_data.append((process_name, table_name,File_Name,SourceFileName,SourceFormat,'NA','NA', 'NA', 'NA', 'NA', 'NA','NA')) 


# COMMAND ----------

# Create a DataFrame from the summary list

summary_df = spark.createDataFrame(summary_data, ["Process Name", "Table Name","File Name","SourceFileName","SourceFormat","status","Max File Date", "Azure SQL Count", "Raw Layer Count", "Trusted Layer Count","Bad Record Count","Count Match"])
summary_df=summary_df.filter(summary_df["status"]!='NA')

# COMMAND ----------

display(summary_df)

# COMMAND ----------

print(NA_Tables)

# COMMAND ----------

display(config_table_df)

# COMMAND ----------

# # Define the file name with the current date

# file_name = f"summary_{current_date}.xlsx"

 

# # Write the summary DataFrame to ADLS with the current date in the file name
# adls_path=f"/mnt/landinglayermount/completed/summary_report/{current_date}/{file_name}"
# # summary_df.coalesce(1).write.option("header","true").format("csv").save(adls_path)
# summary_df.write.format("com.crealytics.spark.excel")\
#   .option("header", "true")\
#   .mode("overwrite")\
#   .save(adls_path)


# COMMAND ----------

sql_final_table_name="dbo.data_validation_check"

# COMMAND ----------

summary_df.write \
        .format("jdbc") \
        .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("url", jdbcUrl)\
        .option("dbtable",sql_final_table_name)\
        .option("user", username)\
        .option("password", password)\
        .mode("append") \
        .save()

# COMMAND ----------

