# Databricks notebook source
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

# MAGIC %run /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run /kgsfinance/common_utilities/install_mssql

# COMMAND ----------

# DBTITLE 1,Functional Reports - LnD
if (reportName =='fr') and (processName =='lnd'):
    print('true')
    if table == 'trusted_curr_fr_lnd_actual_cost':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_lnd_actual_cost')
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

        sampleDF=spark.sql('select Entity,Ledger_Name,Operating_Unit,Business_Category,Client_Geo,GDC_Future,GDC_Future_1,Inter_Operating_Unit,User_Type,Source,Function,Sub_Function,Service_Line,Profit_Centre_Cost_Centre,Profit_Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,amount_in_dollar_000,amount_in_dollar_million,Location,MI_Grouping,Project_ID,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period,Invoice_No_ER_No_Adjustment_No,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date,Created_By,CREATED_BY__DESC,Creation_Date,GL_Date,DD_UTR_Number,Receipt_Creator_Name,Receipt_Number,Vendor_PAN_No,Vendor_Address,Customer_Address,Vendor_Service_Tax_RegNo,Customer_Service_Tax_RegNo,Requisition_Number,Requisition_Description,MI_Mapping,MI_Mapping_2,Additional_Comments,BU,Geo,Training_name,Training_name_1,Training_Name_2,Comment,Period_1,Mapping_Geo,Account_name_as_per_GL,Category_1,Category_2,Service_Line_1,L_D_Team__L_D_Cost,Source_1,Spoc,Reviewer,Remarks,Date,Type,CC_Geo,Affiliate_Operating_Unit,Affliate_MI_Grouping,Inter_Entity,Common_Cost_Entity,Common_Cost_Location,1_Check___MI_Mapping,2_Opearting_Unit_check__per_MI_Mapping,Bonus_In_Percent,Markup_In_Percent,Bonus,Gratuity___LE,Total_Incl_Bonus___Gratuity,Total_Incl_Markup,Remarks_1,Revenue,GEO_Final,BU_Wise,MI_Mapping_1,Check,MI_Gourping_Basis_Account_Code,MI_Grouping_Check,Entity_Name_as_per_OU,Entity_Name_Check,Description_Length,Jounal_Name_Check,CY_PY,Finance_Comments,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_lnd_plan_cost':
        year= spark.sql('select distinct Financial_Year from kgsfinancedb.trusted_curr_fr_lnd_plan_cost')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')
        
        sampleDF=spark.sql('select CC,CC_1,CC_2,Entity,Geo,Location,Account,BU,Month,Value,YTD,Geo_1,Service_Line,Cat,Period,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_lnd_plan_cost_full_year':
        year= spark.sql('select distinct Financial_Year from kgsfinancedb.trusted_curr_fr_lnd_plan_cost_full_year')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')
        
        sampleDF=spark.sql('select CC,CC_1,CC_2,Entity,Geo,Location,Account,BU,Month,Value,YTD,Geo_1,Service_Line,Cat,Period,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    

    if table == 'trusted_curr_fr_lnd_forecast_cost':
        year= spark.sql('select distinct Financial_Year from kgsfinancedb.trusted_curr_fr_lnd_forecast_cost')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')
        
        sampleDF=spark.sql('select Entity,Geo,Location,CC,CC_No,CC_Name,Account,BU,Geo_1,Month,Value,YTD,Service_Line,L_D_Team__L_D_Cost,Period,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_lnd_forecast_cost_full_year':
        year= spark.sql('select distinct Financial_Year from kgsfinancedb.trusted_curr_fr_lnd_forecast_cost_full_year')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')
        
        sampleDF=spark.sql('select Entity,Geo,Location,CC,CC_No,CC_Name,Account,BU,Geo_1,Month,Value,YTD,Service_Line,L_D_Team__L_D_Cost,Period,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_lnd_training_cost_per_entity':
        year= spark.sql('select distinct Financial_Year from kgsfinancedb.trusted_curr_fr_lnd_training_cost_per_entity')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')
        
        sampleDF=spark.sql('select KGS,GDC,Total,Month,Period,MMM,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_lnd_training_cost_per_head':
        year= spark.sql('select distinct Financial_Year from kgsfinancedb.trusted_curr_fr_lnd_training_cost_per_head')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')
        
        sampleDF=spark.sql('select Business_Category,Company_Name,Employee_Type,Designation,Operating_Unit,Payroll,Location2,Cost_Centre2,CC_No,CC_Name,Geo,BU,SL,Month,Headcount,Period,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()    

    if table == 'trusted_curr_fr_lnd_no_of_training':
        year= spark.sql('select distinct Financial_Year from kgsfinancedb.trusted_curr_fr_lnd_no_of_training')
        fy=year.select('Financial_year').rdd.flatMap(lambda x: x).collect()
        fy=fy[0]
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Financial_Year = {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')
        
        sampleDF=spark.sql('select Month,Internal,External,Total,Period,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


# COMMAND ----------

# DBTITLE 1,Functional Reports - LnD - Dimensions
if (reportName =='fr') and (processName =='lnd'):
    print('true')
    if table == 'trusted_curr_fr_lnd_dim_account_category':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_lnd_dim_account_category')
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
        
        sampleDF=spark.sql('select Account_name_as_per_GL,Category,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        if sampleDF.count() > 1:
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()

    if table == 'trusted_curr_fr_lnd_dim_account_code_category':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_lnd_dim_account_code_category')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result ==1 :
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     print(query)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Account_Code_1,Account_name_as_per_GL_1,Category_2,Category_3,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        if sampleDF.count() > 1:
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()

    if table == 'trusted_curr_fr_lnd_dim_account_name_category':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_lnd_dim_account_name_category')
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
        
        sampleDF=spark.sql('select Account_Code,Account_Name,Category_Mapping,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        if sampleDF.count() > 1:
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()

    if table == 'trusted_curr_fr_lnd_dim_client_geo_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_lnd_dim_client_geo_mapping')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result ==1:
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     print(query)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Client_Geo,Geo_Mapping,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        if sampleDF.count() > 1:
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()

    if table == 'trusted_curr_fr_lnd_dim_designation':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_lnd_dim_designation')
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        # sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        # cursor.execute(sql_command)
        # result = cursor.fetchone()[0]
        # if result == 1 :
        #     query=("""DELETE FROM dbo.{} where concat(File_Year,File_Month) = {}""").format(hist_table,fy)
        #     print(query)
        #     conn.execute(query)
        #     conn.commit()
        #     print('delete executed')
        
        sampleDF=spark.sql('select Job_Title,Designation,Designation_for_Reporting,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        if sampleDF.count()>1:
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()

    if table == 'trusted_curr_fr_lnd_dim_geo_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_lnd_dim_geo_mapping')
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
        
        sampleDF=spark.sql('select Geo,Geo_mapping_1,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        if sampleDF.count() > 1:
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()

    if table == 'trusted_curr_fr_lnd_dim_training_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_lnd_dim_training_mapping')
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
        
        sampleDF=spark.sql('select Training,Training_Mapping,Training_Mapping_1,New,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        if sampleDF.count()>1:
            sampleDF.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url",jdbcUrl) \
            .option("dbtable",hist_table) \
            .option("user", username) \
            .option("password", password) \
            .save()

# COMMAND ----------

# DBTITLE 1,Functional Reports - Travel
if (reportName =='fr') and (processName =='travel'):
    print('true')
    if table == 'trusted_curr_fr_travel_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_travel_actual')
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
        
        sampleDF=spark.sql('select Entity,Ledger_Name,Operating_Unit,Business_Category,Client_Geo,GDC_Future,GDC_Future_1,Inter_Operating_Unit,User_Type,Source,Function,Sub_Function,Service_Line,Profit_Centre_Cost_Centre,Profit_Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,mi_map,MI_Grouping,Project_ID,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period,Invoice_No_ER_No_Adjustment_No,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date,Created_By,CREATED_BY__DESC,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator_Name,Receipt_Number,Vendor_PAN_No,Customer_PAN_No,Vendor_Address,Customer_Address,Vendor_Service_Tax_RegNo,Customer_Service_Tax_RegNo,Requisition_Number,Requisition_Description,MI_Mapping,MI_Mapping_2,Additional_Comments,Source_1,Spoc,Reviewer,Remarks,Date,Type,CC_Geo,Affiliate_Operating_Unit,Affliate_MI_Grouping,Inter_Entity,Common_Cost_Entity,Common_Cost_Location,1_Check___MI_Mapping,2_Opearting_Unit_check__per_MI_Mapping,Bonus_In_Percent,Markup_In_Percent,Bonus,Gratuity___LE,Total_Incl_Bonus___Gratuity,Total_Incl_Markup,Remarks_1,Revenue,GEO_Final,BU_Wise,MI_Mapping_1,Check,MI_Gourping_Basis_Account_Code,MI_Grouping_Check,Entity_Name_as_per_OU,Entity_Name_Check,Description_Length,Jounal_Name_Check,Jounal_Description_Check,BU,BU_1,Geo,amount_in_dollar_000,amount_in_dollar_million,Period_1,Comments,BU_Travel_OPE,International_Domestic,BU__2,Geo_1,Mapping,000_1,Concatanate,Geo_Mapping,BU_Mapping_with_MGT,Final_Mapping,Air_Fare_Hotel_Stay,Type_1,SL_Mapping,BU_SL,Account_Code_1,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_travel_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_travel_plan')
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
        
        sampleDF=spark.sql('select CC,CC_No,CC_Name,Entity,Geo,Location,Account,Final_Mapping,Final_Mapping_1,Month,Net_Amount,YTD,Full_year,SL_Mapping,BL_SL,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_travel_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_travel_forecast')
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
        
        sampleDF=spark.sql('select CC,CC_No,CC_Name,Entity,Geo,Location,Account,Final_Mapping,Final_Mapping_1,Month,Net_Amount,YTD,Full_year,SL_Mapping,BL_SL,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_travel_travel_dump':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_travel_travel_dump')
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
        
        sampleDF=spark.sql('select Bill_No,Party_Name,Code,Invoice_Date,Voucher_No,Voucher_Type,Passenger_Name,Arln_Code,Ticket_Number,Sector,Class,Deal_Code,BRN_ID,Basic,YQ_Tax,YR_Tax,JN_Tax,EBT,Tax,OC_Tax,OB_Charges,OTHR_TAX,Service_Tax,CGST_Amount,CGST_Amount_1,IGST_Amount,Extra_Charges,Service_Chg,Mang_Fee,Visa,SGST_Amount,CGST_Amount_2,IGST_Amount_1,CXL_Amount,CXL_SCG,SBC,KKC,Discount,Net_Amount,Trvl_Dt,Trvl_Dt_1,Trvl_Dt_2,Return_Date,Dom_Int,Airline,PSR_Date,CLS,Lead_Time,Slab,Ticket_Type,Ticket_Count,Location,Chargeble_Non_Chargeble,Refundable__Non_Refundable,TYPE_OF_FARE,PNR_No,TRAVEL_COUNSELLOR_NAME,Destination,DES,ORIGIN,ORG,Personal_Booking,Booker_Name,Bookers_Login_ID,Destination_country,PaxCategory,Reason_For_Travel,Detailed_Sector,User_Role,Employee_code,LOCATION_CODE,DESIGNATION,DEPARTMENT,LOCATION_1,EMPLOYEE_CODE_1,FUNCTION,FUNCTION_DESC,SUB_FUNCTION,SUB_FUNCTION_DESC,SR_number,Project_Code,Chargeable,Domestic_International,Policy_Mpaping,BU_Mapping,Geo,Sector_1,Origin_1,Destination_1,Sector_INT,SL_Mapping,BL_SL,Month,monthYear,Dated_On,MMM,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

# DBTITLE 1,Functional Reports - Travel - Dimensions
if (reportName =='fr') and (processName =='travel'):
    print('true')
    if table == 'trusted_curr_fr_travel_dim_country_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_travel_dim_country_mapping')
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
        
        sampleDF=spark.sql('select Place,Country,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_travel_dim_geo_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_travel_dim_geo_mapping')
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
        
        sampleDF=spark.sql('select Geo,BU,Concatenate,Geo_1,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_travel_dim_slab_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_travel_dim_slab_mapping')
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
        
        sampleDF=spark.sql('select Slab,Policy_mapping,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


# COMMAND ----------

# DBTITLE 1,Functional Reports - Contractor
if (reportName =='fr') and (processName =='contractor'):
    print('true')
    if table == 'trusted_curr_fr_contractor_cost_actual':
        print("table1"+table)
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_contractor_cost_actual')
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
        
        sampleDF=spark.sql('select Entity,Ledger_Name,Operating_Unit,Business_Category,GDC_Future,Inter_Operating_Unit,User_Type,Source,Function,Sub_Function,Profit_Centre_Cost_Centre,Profit_Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,mi_map,MI_Grouping,Project_ID,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period,Invoice_No_ER_No_Adjustment_No,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date,Created_By,CREATED_BY__DESC,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator_Name,Receipt_Number,Vendor_PAN_No,Customer_PAN_No,Vendor_Address,Customer_Address,Vendor_Service_Tax_RegNo,Customer_Service_Tax_RegNo,Requisition_Number,Requisition_Description,MI_Mapping,MI_Mapping_2,Additional_Comments,Source_1,Spoc,Reviewer,Remarks,Date,Type,Affiliate_Operating_Unit,Affliate_MI_Grouping,Inter_Entity,Common_Cost_Entity,Common_Cost_Location,BU_1,Geo,amount_in_dollar_000,amount_in_dollar_million,Geo_Location,Vendor_Mapping,Service_line_Final,Location,Dated_On,Month,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write\
        .format("jdbc")\
        .mode("append")\
        .option("url",jdbcUrl)\
        .option("dbtable",hist_table)\
        .option("user", username)\
        .option("password", password)\
        .save()

    if table == 'trusted_curr_fr_contractor_cost_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_contractor_cost_plan')
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
        
        sampleDF=spark.sql('select CC,CC_Name,Entity,Geo,Location,Account,Account_1,BU,Service_line,Net_Amount,Service_line_Mapping,Dated_On,Month,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_contractor_cost_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_contractor_cost_forecast')
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
        
        sampleDF=spark.sql('select CC,CC_Name,Entity,Geo,Location,Account,Account_1,BU,Service_line,Net_Amount,Service_line_Mapping,Dated_On,Month,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_contractor_hc_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_contractor_hc_actual')
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
        
        sampleDF=spark.sql('select Business_Category,Company,Employee_Type,Designation,Operating_Unit,Payroll_Contractor,Location2,Cost_Centre2,Geo,Grand_Total,BU_Mapping,Designation_Mapping,Geo_Mapping,Service_line,Contractor_Count,cc,cc_name,Dated_On,Month,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_contractor_hc_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_contractor_hc_plan')
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
        
        sampleDF=spark.sql('select Business_Category,Entity,Payroll,Designation,Operating_Unit,Geo_Detailed,CC_det,Location,Employee_Type,BU_Mapping,Geo_Mapping,Designation_Mapping,Service_Line,Contractor_Count,cc,cc_name,Dated_On,Month,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


    if table == 'trusted_curr_fr_contractor_hc_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_contractor_hc_forecast')
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
        
        sampleDF=spark.sql('select Business_Category,Entity,Payroll,Designation,Operating_Unit,Geo_Detailed,CC_det,Location,Employee_Type,BU_Mapping,Geo_Mapping,Designation_Mapping,Service_Line,Contractor_Count,cc,cc_name,Dated_On,Month,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl)\
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

# DBTITLE 1,Functional Reports - Contractor - Dimensions
if (reportName =='fr') and (processName =='contractor'):
    print('true')
    if table == 'trusted_curr_fr_contractor_dim_geo':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_contractor_dim_geo')
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
        
        sampleDF=spark.sql('select Geo_Location,Geo_Level2,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_contractor_dim_vendor':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_contractor_dim_vendor')
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
        
        sampleDF=spark.sql('select Vendor_Name,Vendor_Mapping,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_contractor_dim_designation_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_contractor_dim_designation_mapping')
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
        
        sampleDF=spark.sql('select Designation_Mapping,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_contractor_dim_employee_type':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_contractor_dim_employee_type')
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
        
        sampleDF=spark.sql('select Employee_Type,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


# COMMAND ----------

# DBTITLE 1,Functional Reports - Admin
if (reportName =='fr') and (processName =='admin'):
    print('true')
    if table == 'trusted_curr_fr_admin_facility_bu_geo_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_facility_bu_geo_actual')
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
        
        sampleDF=spark.sql('select Key_1, Entity_Name, Ledger_Name, Operating_Unit, Business_Category, Client_Geo, GDC_Future, GDC_Future_1, Inter_Operating_Unit, User_Type, Source, Function, Sub_Function, Service_Line, Profit_Centre_Cost_Centre, Profit_Cost_Centre_Code, Function_Code, Debit_Amt, Credit_Amt, Net_Amount, Location, MI_Grouping, Project_ID, Account_Code, Account_Name, Account_Flex_Field, Category, Account_Grouping, Project_Org, Project_Name, Project_Description, Vendor_Name, Customer_Name, Period, Invoice_No_ER_No_Adjustment_No, Description_Full, GL_Description, Batch_Name, Journal_Desc, Journal_Name, Justification_from_ER, PO_Number, Payment_Doc_No, Payment_Date, Created_By, CREATED_BY__DESC, Creation_Date, Creation_Time, GL_Date, DD_UTR_Number, Receipt_Creator_Name, Receipt_Number, Vendor_PAN_No, Customer_PAN_No, Vendor_Address, Customer_Address, Vendor_Service_Tax_RegNo, Customer_Service_Tax_RegNo, Requisition_Number, Requisition_Description, MI_Mapping, Mapping_2, Geo, Source_1, Spoc, Reviewer, Remarks, Date, Type, CC_Geo, Affliate_Entity, Affliate_MI_Grouping, Inter_Co, Common_Cost_Entity, Common_Cost_Location, Location_1, Geo___BU__KI, BU__KI, Res, BU_Mapping, Geo_Mapping, Month, Calendar_Year, Financial_Year, Dated_On, Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_facility_bu_geo_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.'+table)
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
        
        sampleDF=spark.sql('select Entity,Geo,Location,CC,Value,Value_Mapping,CC_Number,CC_Name,BU_Mapping,Geo_Mapping,Amount,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_facility_bu_geo_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_facility_bu_geo_plan')
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
        
        sampleDF=spark.sql('select Entity,Geo,Location,CC,Value,CC_Number,CC_Name,Amount,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key,BU_Mapping,Geo_Mapping from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_transport_bu_geo_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_transport_bu_geo_actual')
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
        
        sampleDF=spark.sql('select Key_1, Entity, Ledger_Name, Operating_Unit, Business_Category, Client_Geo, GDC_Future, GDC_Future_1, Inter_Operating_Unit, User_Type, Source, Function, Sub_Function, Service_Line, Profit_Centre_Cost_Centre, Profit_Cost_Centre_Code, Function_Code, Debit_Amt, Credit_Amt, Net_Amount, mi_map, MI_Grouping, Project_ID, Account_Code, Account_Name, Account_Flex_Field, Category, Account_Grouping, Project_Org, Project_Name, Project_Description, Vendor_Name, Customer_Name, Period, Invoice_No_ER_No_Adjustment_No, Description_Full, GL_Description, Batch_Name, Journal_Desc, Journal_Name, Justification_from_ER, PO_Number, Payment_Doc_No, Payment_Date, Created_By, CREATED_BY__DESC, Creation_Date, Creation_Time, GL_Date, DD_UTR_Number, Receipt_Creator_Name, Receipt_Number, Vendor_PAN_No, Customer_PAN_No, Vendor_Address, Customer_Address, Vendor_Service_Tax_RegNo, Customer_Service_Tax_RegNo, Requisition_Number, Requisition_Description, MI_Mapping, MI_Mapping_2, Additional_Comments, Source_1, Spoc, Reviewer, Remarks, Date, Type, CC_Geo, Affiliate_Operating_Unit, Affliate_MI_Grouping, Inter_Entity, Common_Cost_Entity, Common_Cost_Location, Location, Geo___BU__KI, BU__KI, Res, BU_Mapping, Geo_Mapping, Month, Calendar_Year, Financial_Year, Dated_On, Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_transport_bu_geo_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_transport_bu_geo_actual')
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
        
        sampleDF=spark.sql('select Entity,Geo,Location,CC,Value,Value_Mapping,CC_Number,CC_Name,BU_mapping,Geo_Mapping,Dated_On,Financial_Year,Month,Amount,Calendar_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_admin_transport_bu_geo_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_transport_bu_geo_actual')
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
        
        sampleDF=spark.sql('select Entity,Geo,Location,CC,Value,CC_Number,CC_Name,Dated_On,Financial_Year,Month,Amount,Calendar_Year,Month_Key,BU_Mapping,Geo_Mapping from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


    if table == 'trusted_curr_fr_admin_hc_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_hc_actual')
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
        
        sampleDF=spark.sql('select Business_Category,Company,Employee_Type,Designation,Operating_Unit,Payroll_Contractor,Location2,Cost_Centre2,Cost_Centre_Code,Cost_Centre_Name,Geo,BU_Mapping,Geo_Mapping,Designation_Mapping,Average,BU_Shrinkage_in_Percent,HC_post_shrinkage,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_hc_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.'+table)
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
        
        sampleDF=spark.sql('select Group_A_B,Entity,Payroll_Contractor,Designation,OU,Location,Geo,CC,Account_Name,BU_Mapping,Geo_Mapping,Designation_Mapping,CC_Number,CC_Name,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write\
        .format("jdbc")\
        .mode("append")\
        .option("url",jdbcUrl)\
        .option("dbtable",hist_table)\
        .option("user", username)\
        .option("password", password)\
        .save()

    if table == 'trusted_curr_fr_admin_hc_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_hc_plan')
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
        
        sampleDF=spark.sql('select Group_A_B,Entity,Payroll_Contractor,Designation,OU,Location,Geo,CC,Account_Name,BU_Mapping,Geo_Mapping,Designation_Mapping,CC_Number,CC_Name,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write\
        .format("jdbc")\
        .mode("append")\
        .option("url",jdbcUrl)\
        .option("dbtable",hist_table)\
        .option("user", username)\
        .option("password", password)\
        .save()


    if table == 'trusted_curr_fr_admin_seats_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_seats_actual')
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
        
        sampleDF=spark.sql('select FY,Key,Key_2,Location,Geo,Function,BU_Mapping,Additional_category,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_seats_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_seats_forecast')
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
        
        sampleDF=spark.sql('select FY,Key,Key_2,Location,Geo,Function,BU_Mapping,Additional_category,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_admin_seats_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_seats_plan')
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
        
        sampleDF=spark.sql('select FY,Key,Key_2,Location,Geo,Function,BU_Mapping,Additional_category,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_facility_cost_location_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_facility_cost_location_actual')
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
        
        sampleDF=spark.sql('select Scenario,Description,Grouping,Building,Location,Grouping_Category,Grouping_2,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save() 

    if table == 'trusted_curr_fr_admin_facility_cost_location_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.'+table)
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
        
        sampleDF=spark.sql('select Scenario,Description,Grouping,Building,Location,Grouping_Category,Grouping_2,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_facility_cost_location_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_admin_facility_cost_location_plan')
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
        
        sampleDF=spark.sql('select Scenario,Description,Grouping,Building,Location,Grouping_Category,Grouping_2,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_function_cost':
        
        sampleDF=spark.sql('select Period,Value,Dated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("truncate", "true")\
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_fx_rate':
        
        sampleDF=spark.sql('select CFY_A,Dated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("truncate", "true")\
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_admin_fy_classification':
        
        sampleDF=spark.sql('select Year,Name,Priority,Dated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("truncate", "true")\
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_admin_transport_split_regular':
        
        sampleDF=spark.sql('select Location,Dated_On,Month,Value from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("truncate", "true")\
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

 if table == 'trusted_curr_fr_admin_facility_cost_location_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.'+table)
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
        
        sampleDF=spark.sql('select Scenario,Description,Grouping,Building,Location,Grouping_Category,Grouping_2,Month,Value,Dated_On,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


# COMMAND ----------

# DBTITLE 1,Functional Reports - Admin - Dimensions
if (reportName =='fr') and (processName =='admin'):
    print('true')
    if table == 'trusted_curr_fr_admin_dim_account_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_admin_dim_account_mapping')
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
        
        sampleDF=spark.sql('select Account,Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_dim_designation':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_admin_dim_designation')
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
        
        sampleDF=spark.sql('select as_per_HR_report,Final_Report,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_dim_geo_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_admin_dim_geo_mapping')
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

        sampleDF=spark.sql('select Geo,Geo_Mapping,Geo_Facility,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_admin_dim_location':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_admin_dim_location')
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
        
        sampleDF=spark.sql('select Operating_Unit,Location_2,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


# COMMAND ----------

# DBTITLE 1,Functional Reports - Recruitment
if (reportName =='fr') and (processName =='recruitment'):
    print('true')
    if table == 'trusted_curr_fr_recruitment_team_cost_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_team_cost_actual')
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
        
        sampleDF=spark.sql('select Entity,Operating_Unit,Cost_Center,Cost_Center_ID,Cost_Center_Name,Geo,Compensation_Type,Geo_1,Service_Line,Value,Period,Account_mapping,BU,Total,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_team_cost_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_team_cost_forecast')
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
        
        sampleDF=spark.sql('select Entity,Operating_Unit,Cost_Center,Cost_Center_ID,Cost_Center_Name,Compensation_Type,Geo,Account_mapping,BU,Total,BU_mapping_as_per_console,Service_Line,FY,Period,Value,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write\
        .format("jdbc")\
        .mode("append")\
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_team_cost_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_team_cost_plan')
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
        
        sampleDF=spark.sql('select Entity,CC,trim as CC_with_code,CC_Code, CC_Name, BU, Geo, Account, Total, Account_mapping, Geo_1, Trim___cost_line,BU_mapping_as_per_console,Service_Line, Financial_Year, Dated_On, Month, Amount, Calendar_Year, Month_Key from kgsfinancedb.'+table)

        sampleDF.write\
        .format("jdbc")\
        .mode("append")\
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_hiring_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_hiring_actual')
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
        
        sampleDF=spark.sql('select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,BU_as_per_HR,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job,People_Group,Employee_Category,Date_First_Hired,End_Date_for_FTS,Gender,Company,Supervisor_Name,Performance_Manager,email,Payroll_Contractor,Count,Geo,Designation,Designation_for_Cost_line,Designation_for_hiring_Mix,Month_Hired,Final_BU_Mapping,Source,Source_2,Entity,Service_line,Geo_2,Period,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_hiring_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_hiring_forecast')
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
        
        sampleDF=spark.sql('select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,BU_as_per_HR,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job,People_Group,Employee_Category,Date_First_Hired,End_Date_for_FTS,Gender,Company,Supervisor_Name,Performance_Manager,email,Payroll_Contractor,Count,Geo,Geo_Total,Geo_2,Designation,Designation_for_Cost_line,Designation_for_hiring_Mix,Month_Hired,Month_number_hired,Period,Source,Source_2,Final_BU_Mapping,Entity,Service_Line,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_recruitment_hiring_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_hiring_plan')
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
        
        sampleDF=spark.sql('select Employee_Number,Full_Name,Function,Employee_Subfunction,Employee_Subfunction_1,Organization_Name,Cost_centre,BU_as_per_HR,Operating_Unit,User_Type,Client_Geography,Location,Sub_Location,Position,Job,People_Group,Employee_Category,Date_First_Hired,End_Date_for_FTS,Gender,Company,Supervisor_Name,Performance_Manager,email,Payroll_Contractor,Count,Geo,Geo_Total,Geo_2,Designation,Designation_for_Cost_line,Designation_for_hiring_Mix,Month_Hired,Month_number_hired,Period,Source,Source_2,Final_BU_Mapping,Entity,Service_Line,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_cost_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_cost_actual')
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
        
        sampleDF=spark.sql('select Entity,Ledger_Name,Operating_Unit,Business_Category,Client_Geo,GDC_Future,GDC_Future_1,Inter_Operating_Unit,User_Type,Source,Function,Sub_Function,Service_Line,Profit_Centre_Cost_Centre,Profit_Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_ID,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period,Invoice_No_ER_No_Adjustment_No,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date,Created_By,CREATED_BY__DESC,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator_Name,Receipt_Number,Vendor_PAN_No,Customer_PAN_No,Vendor_Address,Customer_Address,Vendor_Service_Tax_RegNo,Customer_Service_Tax_RegNo,Requisition_Number,Requisition_Description,MI_Mapping,GEO,Source_1,Spoc,Type,Date,Remarks,CC_Geo,Affiliate_Operating_Unit,Affliate_MI_Grouping,Inter_Entity,Common_Cost_Entity,Common_Cost_Location,1_Check___MI_Mapping,Bonus_In_Percent,Markup_In_Percent,Bonus,Gratuity___LE,Total_Incl_Bonus___Gratuity,Total_Incl_Markup,Remarks_1,Revenue,GEO_Final,BU_Wise,MI_Mapping_1,GEO_revised,Geo_mapping_for_total,revised_BU,Category_1,BU_mapping_as_per_console,Category_2,Service_Line_Final,Reviewer,BU,YTD_Sep,PY_CY,USD_K,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_cost_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_cost_forecast')
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
        
        sampleDF=spark.sql('select Entity,Ledger_Name,Operating_Unit,Business_Category,Client_Geo,GDC_Future,GDC_Future_1,Inter_Operating_Unit,User_Type,Source,Function,Sub_Function,Service_Line,Profit_Centre_Cost_Centre,Profit_Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_ID,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period,Invoice_No_ER_No_Adjustment_No,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date,Created_By,CREATED_BY__DESC,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator_Name,Receipt_Number,Vendor_PAN_No,Customer_PAN_No,Vendor_Address,Customer_Address,Vendor_Service_Tax_RegNo,Customer_Service_Tax_RegNo,Requisition_Number,Requisition_Description,MI_Mapping,MI_Mapping_1,Additional_Comments,Source_1,Spoc,Remarks,Date,Type,CC_Geo,Affiliate_Operating_Unit,Affliate_MI_Grouping,Inter_Entity,Common_Cost_Entity,Common_Cost_Location,1_Check___MI_Mapping,2_Opearting_Unit_check__per_MI_Mapping,Bonus_In_Percent,Markup_In_Percent,Bonus,Gratuity___LE,Total_Incl_Bonus___Gratuity,Total_Incl_Markup,Remarks_1,Revenue,GEO_Final,BU_Wise,MI_Mapping_2,BU,GEO,GEO_revised,Geo_mapping_for_total,revised_BU,Category_1,Category_2,BU_mapping_as_per_console,YTD_Sep,PY_CY,Service_Line_Final,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_cost_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_recruitment_cost_plan')
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
        
        sampleDF=spark.sql('select Entity,Ledger_Name,Operating_Unit,Business_Category,Client_Geo,GDC_Future,GDC_Future_1,Inter_Operating_Unit,User_Type,Source,Function,Sub_Function,Service_Line,Profit_Centre_Cost_Centre,Profit_Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,Location,MI_Grouping,Project_ID,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period,Invoice_No_ER_No_Adjustment_No,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date,Created_By,CREATED_BY__DESC,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator_Name,Receipt_Number,Vendor_PAN_No,Customer_PAN_No,Vendor_Address,Customer_Address,Vendor_Service_Tax_RegNo,Customer_Service_Tax_RegNo,Requisition_Number,Requisition_Description,MI_Mapping,MI_Mapping_1,Additional_Comments,Source_1,Spoc,Remarks,Date,Type,CC_Geo,Affiliate_Operating_Unit,Affliate_MI_Grouping,Inter_Entity,Common_Cost_Entity,Common_Cost_Location,1_Check___MI_Mapping,2_Opearting_Unit_check__per_MI_Mapping,Bonus_In_Percent,Markup_In_Percent,Bonus,Gratuity___LE,Total_Incl_Bonus___Gratuity,Total_Incl_Markup,Remarks_1,Revenue,GEO_Final,BU_Wise,MI_Mapping_2,BU,GEO,GEO_revised,Geo_mapping_for_total,revised_BU,Category_1,Category_2,BU_mapping_as_per_console,PY_CY,Service_Line_Final,Dated_On,Month,Calendar_Year,Financial_Year,Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

# DBTITLE 1,Functional Reports - Recruitment dimensions
if (reportName =='fr') and (processName =='recruitment'):
    print('true')
    if table == 'trusted_curr_fr_recruitment_dim_account_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_recruitment_dim_account_mapping')
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
        
        sampleDF=spark.sql('select Account_Code,Account_Name_1,Category,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_dim_category_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_recruitment_dim_category_mapping')
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
        
        sampleDF=spark.sql('select Category_2,Category_Mapping, Dated_On, File_Year, File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_dim_designation':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_recruitment_dim_designation')
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
        
        sampleDF=spark.sql('select Designation,Designation_for_Cost_line,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_dim_geo_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_recruitment_dim_geo_mapping')
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
        
        sampleDF=spark.sql('select Geo,Geo_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_dim_mi_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_recruitment_dim_mi_mapping')
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
        
        sampleDF=spark.sql('select MI_Grouping,Trimmed,MI_Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_recruitment_dim_source_mapping':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_recruitment_dim_source_mapping')
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
        
        sampleDF=spark.sql('select Source,Mapping,Dated_On,File_Year,File_Month from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

# DBTITLE 1,Functional Reports - IT
if (reportName =='fr') and (processName =='it'):
    print('true')
    if table == 'trusted_curr_fr_it_base_data':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_base_data')
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
        
        sampleDF=spark.sql('select Entity,Ledger_Name,Operating_Unit,Business_Category,Client_Geo,GDC_Future,GDC_Future_1,Inter_Operating_Unit,User_Type,Source,Function,Sub_Function,Service_Line,Profit_Centre_Cost_Centre,Profit_Cost_Centre_Code,Function_Code,Debit_Amt,Credit_Amt,Net_Amount,mi_map,MI_Grouping,Project_ID,Account_Code,Account_Name,Account_Flex_Field,Category,Account_Grouping,Project_Org,Project_Name,Project_Description,Vendor_Name,Customer_Name,Period,Invoice_No_ER_No_Adjustment_No,Description_Full,GL_Description,Batch_Name,Journal_Desc,Journal_Name,Justification_from_ER,PO_Number,Payment_Doc_No,Payment_Date,Created_By,CREATED_BY__DESC,Creation_Date,Creation_Time,GL_Date,DD_UTR_Number,Receipt_Creator_Name,Receipt_Number,Vendor_PAN_No,Customer_PAN_No,Vendor_Address,Customer_Address,Vendor_Service_Tax_RegNo,Customer_Service_Tax_RegNo,Requisition_Number,Requisition_Description,MI_Mapping,MI_Mapping_2,Additional_Comments,Source_1,Spoc,Reviewer,Remarks,Date,Type,CC_Geo,Affiliate_Operating_Unit,Affliate_MI_Grouping,Inter_Entity,Common_Cost_Entity,Common_Cost_Location,1_Check___MI_Mapping,2_Opearting_Unit_check__per_MI_Mapping,Bonus___1,Markup,Bonus,Gratuity___LE,Total_Incl_Bonus___Gratuity,Total_Incl_Markup,Remarks_1,Revenue,GEO_Final,BU_Wise,MI_Mapping_1,MI_Gourping_Basis_Account_Code,Entity_Name_as_per_OU,Description_Length,IT_Mapping,Commetns,Concatenate,IT_Mapping_1,Dated_On,Month,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_it_cfit_cost_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cfit_cost_actual')
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
        
        sampleDF=spark.sql('select Particulars,Adjustments,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_cfit_cost_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cfit_cost_plan')
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
        
        sampleDF=spark.sql('select Particulars,Adjustments,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_cfit_cost_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cfit_cost_forecast')
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
        
        sampleDF=spark.sql('select Particulars,Adjustments,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_cfit_hc_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cfit_hc_actual')
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
        
        sampleDF=spark.sql('select Designations,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)
        

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_it_cfit_hc_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cfit_hc_plan')
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
        
        sampleDF=spark.sql('select Designations,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)
        

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if table == 'trusted_curr_fr_it_cfit_hc_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cfit_hc_forecast')
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
        
        sampleDF=spark.sql('select Designations,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)
        

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_cost_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cost_actual')
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
        
        sampleDF=spark.sql('select Cost_Description,Month,Net_Amount,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save() 

    if table == 'trusted_curr_fr_it_cost_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cost_plan')
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
        
        sampleDF=spark.sql('select IT_lines,Account_Name,Vendor_Name,Type,Month, Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save() 

    if table == 'trusted_curr_fr_it_cost_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_cost_forecast')
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
        
        sampleDF=spark.sql('select IT_lines,Account_Name,Vendor_Name,Type,Month, Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_from_niraj':
        year= spark.sql('select distinct Attribute from kgsfinancedb.trusted_curr_fr_it_from_niraj')
        fy=year.select('Attribute').rdd.flatMap(lambda x: x).collect()
        fy=tuple(fy)
        print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query=(""" DELETE FROM dbo.{} where Attribute in {} """).format(hist_table,fy)
            conn.execute(query)
            conn.commit()
            print('delete executed')
        
        sampleDF=spark.sql('select Category,Attribute,Value,Financial_Year,Dated_On from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_hc_actual':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_hc_actual')
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
        
        sampleDF=spark.sql('select Entity,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)
        

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_hc_plan':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_hc_plan')
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
        
        sampleDF=spark.sql('select Entity,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_hc_forecast':
        year= spark.sql('select distinct Financial_year from kgsfinancedb.trusted_curr_fr_it_hc_forecast')
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
        
        sampleDF=spark.sql('select Entity,Month,Value,Dated_On,MMM,Month_Key,Calendar_Year,Financial_Year from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


# COMMAND ----------

# DBTITLE 1,Functional Report - IT - Dimensions
if (reportName =='fr') and (processName =='it'):
    print('true')
    if table == 'trusted_curr_fr_it_dim_lookup_category':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_it_dim_lookup_category')
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
        
        sampleDF=spark.sql('select Super_Category,Category,Sub_Category,Category_Priority,Super_Category_Priority,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_dim_lookup_cfit_category':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_it_dim_lookup_cfit_category')
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
        
        sampleDF=spark.sql('select Category,Sub_Category,Sub_Category_2,Category_Priority,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_dim_lookup_cfit_designation':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_it_dim_lookup_cfit_designation')
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
        
        sampleDF=spark.sql('select Category,Sub_Category,Sub_Category_2,Category_Priority,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if table == 'trusted_curr_fr_it_dim_month_lookup_usd':
        # year= spark.sql('select distinct concat(File_Year,File_Month) as YearMonth from kgsfinancedb.trusted_curr_fr_it_dim_month_lookup_usd')
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
        
        sampleDF=spark.sql('select Month,Priority,FY,USD_Rate,Quarter,Dated_On,File_Year,File_Month,concat(File_Year,File_Month) as Month_Key from kgsfinancedb.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()


# COMMAND ----------

# sampleDF=spark.sql("select * from kgsfinancedb.trusted_curr_fr_lnd_training_cost_per_entity") 
# sampleDF.display()

# COMMAND ----------

# print(sampleDF.columns)

# COMMAND ----------

