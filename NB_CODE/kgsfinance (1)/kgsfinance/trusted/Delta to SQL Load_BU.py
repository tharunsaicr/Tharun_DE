# Databricks notebook source
# DBTITLE 1,Input Parameters
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

table = "trusted_curr_"+reportName+"_"+processName+"_"+tableName
hist_table = "trusted_hist_"+reportName+"_"+processName+"_"+tableName

print(tableName)
print(processName)
print(table)
print(hist_table)
print(reportName)

# COMMAND ----------

# DBTITLE 1,Call connection configuration
# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/install_mssql

# COMMAND ----------

# DBTITLE 1,BU Pack Dimension list
#account_lookup
#accounts
#business_category
#employee_type
#entity
#geo
#location
#operating_unit
#projects
#years
#scenario
#period
#rls
#hyperion
#console
#kgs
#uk
#us
#kdn
#krc_mapping
#ch_leads_mapping
#default_rls
#incremental_rls
#reporting_view_rls
#bu_view_rls

# COMMAND ----------

# DBTITLE 1,BU Report - CF Pack
#load trusted hist tables for cf_pack from delta to sql 
if (reportName =='bu_report') and (processName =='cf_pack'):
    print('true')

    #CF Function
    if table == 'trusted_curr_bu_report_cf_pack_cf_function':
        print(table)
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_CF_Pack_cf_function')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     print(query)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Display_SL, CF_Function, Dated_On, File_Year, File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table :"+ hist_table)

    #CF headcount remarks
    if table == 'trusted_curr_bu_report_cf_pack_cf_headcount_remarks':
        print(table)
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_lnd_dim_account_code_category')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     print(query)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Function	Remarks,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table :"+ hist_table)

    #Year Classification
    if table == 'trusted_curr_bu_report_cf_pack_year_classification':
        print(table)
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_cf_pack_year_classification')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     print(query)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Name, FY, Dated_On, File_Year, File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table :"+ hist_table)


# COMMAND ----------

# DBTITLE 1,BU Report - HR P&L Dimensions
#load trusted hist tables for HR P&L from delta to sql 
if (reportName =='bu_report') and (processName =='hrpandl_pack'):
    print('true')
    #EE and IandD Cost
    if table == 'trusted_curr_bu_report_hrpandl_pack_EE_and_IandD_cost':
        # fin_year = spark.sql('select distinct case when File_Month in (10,11,12) then string(int(substring(File_Year,3,2))+1) else substring(File_Year,3,2) end as financial_year from kgsfinancedb.trusted_curr_bu_report_hrpandl_pack_ee_and_iandd_cost')
        # fin_year = fin_year.select('financial_year').rdd.flatMap(lambda x: x).collect()
        # fin_year=fin_year[0]

        # query = 'select Particulars,Index,Var__Vs_FY'+fin_year+'_Plan,Var_FY'+fin_year+'_Plan_In_Percent,FY'+fin_year+'_P_Feb_YTD,FY'+fin_year+'_A__Feb_YTD,Dated_On,File_Year,File_Month from kgsfinancedb.trusted_curr_bu_report_hrpandl_pack_ee_and_iandd_cost'
        # print(query)

        # sampleDF=spark.sql(query)
        # display(sampleDF)

        sampleDF=spark.sql('select Particulars,Attribute,Value,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)        

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table :"+ hist_table)


# COMMAND ----------

# DBTITLE 1,BU Report - BU Pack Facts
#load trusted hist fact tables for BU_pack from delta to sql 
if (reportName =='bu_report') and (processName =='bu_pack'):
    print('true')
    if table == 'trusted_curr_bu_report_bu_pack_actual_hcplan_data_extract':
        print("Fact table1:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_actual_hcplan_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT, YEARS, Scenario, VERSION, COSTCENTER, CURRENCY, BUSINESSCATEGORY,EMPLOYEE_TYPE,ENTITY,GEOGRAPHY, LOCATION, OPERATING_UNIT, ROLE,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')

    if table == 'trusted_curr_bu_report_bu_pack_actual_opex_data_extract':
        print("Fact table2:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_actual_opex_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,VERSION,Scenario,COSTCENTER,CURRENCY,BUSINESSCATEGORY,ENTITY,GEOGRAPHY,LOCATION, OPERATING_UNIT,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')

   
    if table == 'trusted_curr_bu_report_bu_pack_actual_projects_data_extract':
        print("Fact table3:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_actual_projects_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,Scenario,VERSION,COSTCENTER,BUSINESSCATEGORY,CURRENCY,DESTINATION,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,PROJECTS,ROLE,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')
    
    if table == 'trusted_curr_bu_report_bu_pack_forecast_hcplan_data_extract':
        print("Fact table4:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_forecast_hcplan_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,Scenario,VERSION,COSTCENTER,CURRENCY,BUSINESSCATEGORY,EMPLOYEE_TYPE,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,ROLE,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')

    if table == 'trusted_curr_bu_report_bu_pack_outlook_hcplan_data_extract':
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_forecast_hcplan_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,Scenario,VERSION,COSTCENTER,CURRENCY,BUSINESSCATEGORY,EMPLOYEE_TYPE,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,ROLE,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')
  
    if table == 'trusted_curr_bu_report_bu_pack_forecast_opex_data_extract':
        print("Fact table5:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_forecast_opex_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,VERSION,Scenario,COSTCENTER,CURRENCY,BUSINESSCATEGORY,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')

    if table == 'trusted_curr_bu_report_bu_pack_outlook_opex_data_extract':
        print("Fact table5:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_forecast_opex_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,VERSION,Scenario,COSTCENTER,CURRENCY,BUSINESSCATEGORY,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')

    if table == 'trusted_curr_bu_report_bu_pack_forecast_projects_data_extract':
        print("Fact table6:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_forecast_projects_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,Scenario,VERSION,COSTCENTER,BUSINESSCATEGORY,CURRENCY,DESTINATION,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,PROJECTS,ROLE,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')

    if table == 'trusted_curr_bu_report_bu_pack_outlook_projects_data_extract':
        print("Fact table6:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_forecast_projects_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,Scenario,VERSION,COSTCENTER,BUSINESSCATEGORY,CURRENCY,DESTINATION,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,PROJECTS,ROLE,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')
   
    if table == 'trusted_curr_bu_report_bu_pack_plan_hcplan_data_extract':
        print("Fact table7:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_plan_hcplan_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select ACCOUNT,YEARS,Scenario,VERSION,COSTCENTER,CURRENCY,BUSINESSCATEGORY,EMPLOYEE_TYPE,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,ROLE,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')
   
    if table == 'trusted_curr_bu_report_bu_pack_plan_opex_data_extract':
        print("Fact table8:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_plan_opex_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,VERSION,Scenario,COSTCENTER,CURRENCY,BUSINESSCATEGORY,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')

    if table == 'trusted_curr_bu_report_bu_pack_plan_projects_data_extract':
        print("Fact table9:"+table)
        # year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_bu_report_bu_pack_plan_projects_data_extract')
        # fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select ACCOUNT,YEARS,Scenario,VERSION,COSTCENTER,BUSINESSCATEGORY,CURRENCY,DESTINATION,ENTITY,GEOGRAPHY,LOCATION,OPERATING_UNIT,PROJECTS,ROLE,PERIOD,DATA,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,Updated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print('Append executed')

# COMMAND ----------

# DBTITLE 1,BU Report - BU Pack Dimensions
#load trusted hist dimension tables for BU_pack from delta to sql 
if (reportName =='bu_report') and (processName =='bu_pack'):
    print('true')
    if table == 'trusted_curr_bu_report_bu_pack_account_lookup':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_account_lookup')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     print(query)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Account,SL,Parent,Priority,Parent_Priority,SL_Priority,Big_Buckets_1,Pie_Chart_1,Big_Bucket_Priority,Pie_Chart__Priority,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_accounts':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_accounts')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select RecordID,Account,Spaces,Level,level_0,level_1,level_2,level_3,level_4,level_5,level_6,level_7,level_8,level_9,level_10,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)    

    if table == 'trusted_curr_bu_report_bu_pack_business_category':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_business_category')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select BusinessCategory,BusinessCategory_Alias,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_employee_type':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_employee_type')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Emp_Type,EmpType_Alias,Finance_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_entity':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_entity')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select Entity,Entity_Alias,Finance_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)
    
    if table == 'trusted_curr_bu_report_bu_pack_geo':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_geo')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select Geo,Geo_Level_2,Finance_Mapping,Payroll_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_location':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_location')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select Location,Location_Alias,Finance_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_operating_unit':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_operating_unit')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select OU,OU_Alias,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_projects':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_projects')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select Projects,Projects_Alias,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_years':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_years')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Year,Year_Alias,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_scenario':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_scenario')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select Scenario,Scenario_Alias,Finance_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_period':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_period')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:

        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        
        sampleDF=spark.sql('select Period,Period_Alias,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_rls':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_rls')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select Accees,E_Mail,CC,Cost_Center,Display_BU,Display_SL,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_hyperion':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_hyperion')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select FY,Month,Total_Costs_Including_Mark_up,Total_Hours,Total_HC,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_console':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_console')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
        
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select CC,CC_Name,Service_Line,BU,Cost_Center,Display_BU,Display_SL,Mapping___Data_points,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_kgs':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_kgs')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:

        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select CC,CC_Name,SL,BU,Cost_Center,Display_BU,Display_SL,CF_P_L_Cuts,Consulting_Cuts,CH_Cuts,Lead,Comments,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_uk':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_uk')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select CC,CC_Name,Service_Line,BU,Cost_Center,Display_BU,Display_SL,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_us':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_us')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select CC,CC_Name,Service_Line,BU,Cost_Center,Display_BU,Display_SL,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_kdn':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_kdn')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select CC,CC_Name,Service_Line,BU,Cost_Center,Display_BU,Display_SL,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_krc_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_krc_mapping')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select CC_det,CC_2,Geo_Detailed,CC_Geo,BU,Leader,CC,Geo_2,CC___Geo_2,email,Tech_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_tax_us':
        
        sampleDF=spark.sql('select CC,CC_Name,Service_Line,BU,Cost_Center,Display_BU,Display_SL,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_ch_leads_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_ch_leads_mapping')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select Profit_Cost_Centre,Client_Geography,Team_Name,Concate,CC,Lead,Capability,Geo_Codes,Merged,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_default_rls':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_default_rls')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select CC,User,Email,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_incremental_rls':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_incremental_rls')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select CC,User,Email,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_reporting_view_rls':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_reporting_view_rls')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        sampleDF=spark.sql('select Geo,User,Email,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_bu_view_rls':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_bu_report_bu_pack_bu_view_rls')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1:
            
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')

        
        sampleDF=spark.sql('select Geo,BU,User,Email,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_capabilty_mapping':
        sampleDF=spark.sql('select CC_No,Cost_Center,Capabilities,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_designation_mapping':
        sampleDF=spark.sql('select Order,Position,Designation,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_fx_rate':
        sampleDF=spark.sql('select Year,FX,Rate,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_fx_rate_us_uk':
        sampleDF=spark.sql('select Year,US,UK,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_bu_mapping':
        sampleDF=spark.sql('select KGS_Cost_Center,CC_No,CC_Name,Mapping_for_Costline__Expense_Dashboards,KGS_Mapping_for_Payroll_Analytics__BU_Mapping,KGS_SL,Mapping_for_Consolidation__Consol_Mapping,Consol_SL,US_BU,US_SL,UK_BU,UK_SL,KDN_BU,KDN_SL,Entity,KGS_BU_Mapping,Console_BU_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_project_h_us':
        sampleDF=spark.sql('select CC,CC_Name,SL,BU,Cost_Center,Display_BU,Display_SL,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_project_h_uk':
        sampleDF=spark.sql('select CC,CC_Name,SL,BU,Cost_Center,Display_BU,Display_SL,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_bpg_mapping':
        sampleDF=spark.sql('select CC,CC_Name,Service_Line,BU,Cost_Center,Display_BU,Display_SL,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_ytd_hc_hyperion':
        sampleDF=spark.sql('select Business_Category,Company_Name,Employee_Type,Designation,Operating_Unit,Payroll,Location2,Cost_Centre2,Geo,Month,Amount,Dated_On,File_Year,File_Month,Month_Key,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)

    if table == 'trusted_curr_bu_report_bu_pack_unique_cc_geo':
        sampleDF=spark.sql('select CostCenter,CostCenter_Description,Geo_Description,CC,CC_Description,Geo,Dated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
        print("table  :"+ hist_table)


