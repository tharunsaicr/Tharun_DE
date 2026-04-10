# Databricks notebook source
from pyspark.sql.functions import col, lit,lower,upper,format_number,to_timestamp

# COMMAND ----------

dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName").lower()

# dbutils.widgets.text(name = "ReportName", defaultValue = "")
# reportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

table = "trusted_curr_"+tableName
hist_table = "trusted_hist_"+tableName

print(tableName)
print(processName)
print(table)
print(hist_table)
# print(reportName)

# COMMAND ----------

# DBTITLE 1,Call connection module
# MAGIC %run /Workspace/kgsonedataedw/common_utilities/connection_configuration

# COMMAND ----------

# DBTITLE 1,Call common components module
# MAGIC %run /Workspace/kgsonedataedw/common_utilities/common_components

# COMMAND ----------

# MAGIC %run /Workspace/kgsonedataedw/common_utilities/install_mssql

# COMMAND ----------

# DBTITLE 1,Dimensions Tables
if (processName=='dim'):
    if(tableName=='dim_business_category'):
        print('Data Loaded for dim_business_category')
        sampleDF=spark.sql('select CODE,DESCRIPTION,Dated_On,File_Date from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    if (tableName=='dim_gl_account_master'):
        print('Data Loaded for dim_gl_account_master')
        sampleDF=spark.sql('select DIM_ACCOUNT_MASTER_KEY,FLEX_VALUE_SET_ID,FLEX_VALUE_ID,BUSINESS_UNIT,FLEX_VALUE, LOOKUP_CODE,ACCOUNT_GROUPING,FLEX_DESCRIPTION,FLEX_SET_DESCRIPTION,CREATION_DATE,LAST_UPDATE_DATE, FLEX_VALUE_SET_NAME,RECORD_INSERT_DATE,RECORD_INSERTED_BY,Dated_On,File_Date from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if (tableName=='dim_vendor_supplier_details'):
        print('Data Loaded for dim_vendor_supplier_details')
        sampleDF=spark.sql('select VENDOR_ID, PARTY_ID, VENDOR_NAME, SUPPLIER_NUMBER, SUPPLIER_TYPE, VENDOR_USAGE, NATURE_OF_SERVICES, SUPPLIER_CREATION_DATE, LAST_UPDATE_DATE, SUPPLIER_INACTIVE_DATE, PAY_GROUP, VENDOR_SITE_ID, SITE_NAME, SITE_INACTIVE_DATE, ADDRESS_LINE_1, ADDRESS_LINE_2, ADDRESS_LINE_3, CITY, STATE, COUNTRY, ZIP_CODE, INVOICE_CURRENCY, PAYMENT_CURRENCY, SHIP_TO_LOCATION_ID, BILL_TO_LOCATION_ID, ENTITY_ID, SAN, VAT_REG_NUM, VENDOR_PARTY_ID, ANTI_BRIBERY_DECLARATION, LAST_UPDATED_BY, SUPPLIER_CREATED_BY, VENDOR_SITE_CODE_ALT, SITE_TYPE, SHIP_TO_LOCATION, BILL_TO_LOCATION, PAN_NO, TAN_NO, WARD_NO, TDS_TYPE, TAX_NAME, DEFAULT_TDS_SECTION, LIABILITY_A_C, PREPAYMENT_A_C, PRINCIPAL_NAME, MISSION_STATEMENT, EMAIL_ADDRESS, PHONE, FAX, OWNER_TABLE_NAME, STATUS, PRIMARY_FLAG, BANK_ACCOUNT_NUMBER, BANK_ACCOUNT_NAME, BRANCH_NUMBER, BANK_BRANCH_NAME, BANK_NAME, PAYMENT_TERMS, OPERATING_UNIT_ID, BUSINESS_GROUP_ID, CODE_COMBINATION_ID, SEGMENT1, SEGMENT2, SEGMENT3, SEGMENT4, SEGMENT5, SEGMENT6, SEGMENT7, SEGMENT8, SEGMENT9, SEGMENT10, SEGMENT11, PAYMENT_METHOD_CODE, EMPLOYEE_NUMBER, SERVICE_TAX_REGNO, ATTRIBUTE10, Dated_On, File_Date from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_category'):
        print('Data Loaded for dim_category')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if (tableName=='dim_client_geography'):
        print('Data Loaded for dim_client_geography')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_customer'):
        print('Data Loaded for dim_customer')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_employee'):
        print('Data Loaded for dim_employee')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_employee_address'):
        print('Data Loaded for dim_employee_address')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_employee_assignment_details_base'):
        print('Data Loaded for dim_employee_assignment_details_base')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_employee_bank_details_iexpense'):
        print('Data Loaded for dim_employee_bank_details_iexpense')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_employee_bank_transfer_format_iexpense'):
        print('Data Loaded for dim_employee_bank_transfer_format_iexpense')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_employee_grade'):
        print('Data Loaded for dim_employee_grade')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if (tableName=='dim_employee_group'):
        print('Data Loaded for dim_employee_group')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_employee_referral_details'):
        print('Data Loaded for dim_employee_referral_details')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if (tableName=='dim_entity_master'):
        print('Data Loaded for dim_entity_master')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if (tableName=='dim_et_mis_designations'):
        print('Data Loaded for dim_et_mis_designations')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_et_mis_headcount_overall'):
        print('Data Loaded for dim_et_mis_headcount_overall')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_events'):
        print('Data Loaded for dim_events')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_expenditure_types'):
        print('Data Loaded for dim_expenditure_types')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_fa_location_master'):
        print('Data Loaded for dim_fa_location_master')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_financial_app_debtors_ageing'):
        print('Data Loaded for dim_financial_app_debtors_ageing')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_fiscal_cal'):
        print('Data Loaded for dim_fiscal_cal')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_gdcfuturemaster1'):
        print('Data Loaded for dim_gdcfuturemaster1')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_gdcfuturemaster2'):
        print('Data Loaded for ')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_gl_function'):
        print('Data Loaded for dim_gl_function')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_gl_ledger_master'):
        print('Data Loaded for dim_gl_ledger_master')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_gl_locations'):
        print('Data Loaded for dim_gl_locations')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_hr_jobs'):
        print('Data Loaded for dim_hr_jobs')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_hr_kics'):
        print('Data Loaded for dim_hr_kics')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_hr_kicseit'):
        print('Data Loaded for dim_hr_kicseit')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_hr_locations'):
        print('Data Loaded for dim_hr_locations')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_hr_operating_units'):
        print('Data Loaded for dim_hr_operating_units')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_hr_phone_type'):
        print('Data Loaded for dim_hr_phone_type')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_hr_positions'):
        print('Data Loaded for dim_hr_positions')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_hr_staff_account_trainee_conversion_date'):
        print('Data Loaded for dim_hr_staff_account_trainee_conversion_date')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_interoperating_unit'):
        print('Data Loaded for dim_interoperating_unit')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_mis_profit_centre_condensed'):
        print('Data Loaded for dim_mis_profit_centre_condensed')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_monthly_asset_depreciation'):
        print('Data Loaded for dim_monthly_asset_depreciation')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_operating_unit'):
        print('Data Loaded for dim_operating_unit')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_org_hierarchy_master'):
        print('Data Loaded for dim_org_hierarchy_master')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_org_units'):
        print('Data Loaded for dim_org_units')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_p_project_master_final'):
        print('Data Loaded for dim_p_project_master_final')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_p_project_task_master_final'):
        print('Data Loaded for dim_p_project_task_master_final')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_pa_labor_multipliers'):
        print('Data Loaded for dim_pa_labor_multipliers')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_pa_lookups'):
        print('Data Loaded for dim_pa_lookups')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_pa_summary_project_fundings'):
        print('Data Loaded for dim_pa_summary_project_fundings')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_pa_tasks'):
        print('Data Loaded for dim_pa_tasks')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_partner_staff'):
        print('Data Loaded for dim_partner_staff')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_people_group_master'):
        print('Data Loaded for dim_people_group_master')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_per_all_people_f'):
        print('Data Loaded for dim_per_all_people_f')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_pml_balance'):
        print('Data Loaded for dim_pml_balance')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_ppc_designations'):
        print('Data Loaded for dim_ppc_designations')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_ra_customer_trx_all'):
        print('Data Loaded for dim_ra_customer_trx_all')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_secondee_status_ppc'):
        print('Data Loaded for dim_secondee_status_ppc')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_tb_gl_period'):
        print('Data Loaded for dim_tb_gl_period')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_tb_mi_group'):
        print('Data Loaded for dim_tb_mi_group')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_transaction_source'):
        print('Data Loaded for dim_transaction_source')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_unspecfied_profit_centre'):
        print('Data Loaded for dim_unspecfied_profit_centre')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_user_type'):
        print('Data Loaded for dim_user_type')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_users'):
        print('Data Loaded for dim_users')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='dim_vendor'):
        print('Data Loaded for dim_vendor')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()    

# COMMAND ----------

# DBTITLE 1,Fact Tables
if (processName=='fact'):
    if(tableName=='fact_fa_master'):
        print('Data Loaded for fact_fa_master')
        sampleDF=spark.sql('select LEASE_ID, BUSINESS_GROUP_ID, LINKENTITYFAMASTER, ASSET_ID, LIFE_IN_MONTHS, PRORATE_DATE, ASSET_NUMBER, ASSET_CURRENT_STATUS, UNASSIGNED_ASSET_NUMBER, BOOK_TYPE_CODE, CREATION_DATE, LEASEBOOKTYPE, ASSIGN_TO, STOCK_ASSET_NUMBER, RETIRED_ASSET, ACTIVE_ASSETS, ASSET_COUNT, GBVADDITION, GBVDELETION, DATE_PLACED_IN_SERVICE, ASSET_NAME, ASSET_TYPE, ASSET_CATEGORY_ID, SERIAL_NUMBER, MANUFACTURER_NAME, CURRENT_COST, ORIGINAL_COST, VENDOR_ID, ENTITY, ENTITY_ID, MODEL_NUMBER, ASSET_OWNERSHIP, MAJOR_CATEGORY, MINOR_CATEGORY, PO_NUMBER, PO_DATE, ASSET_INVOICE_NUMBER, INVOICE_DATE, CATEGORY_TYPE, LOCATION_ID, ASSET_CATEGORY, PERIOD_ENTERED, UNITS, CURRENT_UNITS, DEPRN_RUN_DATE, MAJORCATEGORY, MINORCATEGORY, LINE_OF_BUSINESS, PARTNER, PROJECT, OFFICE_LOCATION, ACCOUNT, FUNCTION, OPERATING_UNIT, CLIENT_GEOGRAPHY_ID, USER_TYPE, BUSINESS_CATAGORY_ID, INTEROPERATING_UNIT, FUTURE1, FUTURE2,LINKLEASEFAASSETLSP, Dated_On, File_Date from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if(tableName=='fact_gl_q10'):
        print('Data Loaded for fact_gl_q10')
        sampleDF=spark.sql('select QUERY, LEDGER_ID, DIM_ACCOUNT_MASTER_KEY, INVOICE_ID, CURRENCY_CODE, JOURNAL_DESCRIPTION, JOURNAL_NAME, PERIOD_NAME, GL_DOC_NO, SOURCE, BATCH_NAME, DESCRIPTION, CATEGORY, ACCOUNT_FLEX_FIELD, GL_DATE, NARRATION, DEBIT_AMOUNT, CREDIT_AMOUNT, INVOICE_NO, CUSTOMER_ID, BILL_TO_SITE_USE_ID, CREATION_DATE, CREATED_BY, COMPANY, ACCOUNT, PROFIT_CENTRE, PROJECT, LOCATION, LOB, PARTNER, CLIENT_GEOGRAPHY, USER_TYPE, BUSINESS_CATEGORY, INTEROPERATING_UNIT, FUTURE1, FUTURE2, BUSINESS_GROUP_ID, POSTED_DATE, LAST_UPDATED_BY, PARENT_INVOICE_NO, RUNNING_TOTAL_DR, Dated_On, File_Date from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    if (tableName=='fact_ap_amex'):
        print('Data Loaded for fact_ap_amex')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ap_invoice_register'):
        print('Data Loaded for fact_ap_invoice_register')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ap_prepayment_status_invoice_payment'):
        print('Data Loaded for fact_ap_prepayment_status_invoice_payment')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ap_prepayment_status_invoice_query'):
        print('Data Loaded for fact_ap_prepayment_status_invoice_query')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ap_prepayment_transaction'):
        print('Data Loaded for fact_ap_prepayment_transaction')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ap_trial_invoice'):
        print('Data Loaded for fact_ap_trial_invoice')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ap_trial_payment'):
        print('Data Loaded for fact_ap_trial_payment')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_apar'):
        print('Data Loaded for fact_apar')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ar_billing'):
        print('Data Loaded for fact_ar_billing')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ar_collection'):
        print('Data Loaded for fact_ar_collection')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ar_customer_contact_country'):
        print('Data Loaded for fact_ar_customer_contact_country')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ar_echargeback'):
        print('Data Loaded for fact_ar_echargeback')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_ar_history'):
        print('Data Loaded for fact_ar_history')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_cenvat_billed'):
        print('Data Loaded for fact_cenvat_billed')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_cenvat_collected'):
        print('Data Loaded for fact_cenvat_collected')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()
    
    if (tableName=='fact_cwk_employee_details'):
        print('Data Loaded for fact_cwk_employee_details')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_cwk_trmi_employee_details'):
        print('Data Loaded for fact_cwk_trmi_employee_details')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_employee_assignments'):
        print('Data Loaded for fact_employee_assignments')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_employee_qualification'):
        print('Data Loaded for fact_employee_qualification')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_gst10'):
        print('Data Loaded for fact_gl_gst10')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q1'):
        print('Data Loaded for fact_gl_q1')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q11'):
        print('Data Loaded for fact_gl_q11')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q12'):
        print('Data Loaded for fact_gl_q12')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q13'):
        print('Data Loaded for fact_gl_q13')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q18'):
        print('Data Loaded for fact_gl_q18')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q2'):
        print('Data Loaded for fact_gl_q2')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q21'):
        print('Data Loaded for fact_gl_q21')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q23'):
        print('Data Loaded for fact_gl_q23')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q26'):
        print('Data Loaded for fact_gl_q26')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q3'):
        print('Data Loaded for fact_gl_q3')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q4'):
        print('Data Loaded for fact_gl_q4')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q5'):
        print('Data Loaded for fact_gl_q5')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q6'):
        print('Data Loaded for fact_gl_q6')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q7'):
        print('Data Loaded for fact_gl_q7')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_q8'):
        print('Data Loaded for fact_gl_q8')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_gl_tb_kgs'):
        print('Data Loaded for fact_gl_tb_kgs')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_hr_head_count'):
        print('Data Loaded for fact_hr_head_count')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_hr_leave_details'):
        print('Data Loaded for fact_hr_leave_details')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_lockup_data'):
        print('Data Loaded for fact_lockup_data')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_lsp'):
        print('Data Loaded for fact_lsp')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_p_expenditure'):
        print('Data Loaded for fact_p_expenditure')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_p_kgs_opes'):
        print('Data Loaded for fact_p_kgs_opes')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_p_kgs_staff_utilization'):
        print('Data Loaded for fact_p_kgs_staff_utilization')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_p_project_invoice'):
        print('Data Loaded for fact_p_project_invoice')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_p_su_std_hours'):
        print('Data Loaded for fact_p_su_std_hours')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("append") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_p_wip_nfr'):
        print('Data Loaded for fact_p_wip_nfr')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_payment_register'):
        print('Data Loaded for fact_payment_register')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_po_details_with_cheque_numbers'):
        print('Data Loaded for fact_po_details_with_cheque_numbers')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_reciept_register'):
        print('Data Loaded for fact_reciept_register')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_requisition_purchase_status'):
        print('Data Loaded for fact_requisition_purchase_status')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_tds_details'):
        print('Data Loaded for fact_tds_details')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_timesheet_status_final'):
        print('Data Loaded for fact_timesheet_status_final')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

    if (tableName=='fact_uar'):
        print('Data Loaded for fact_uar')
        sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

        sampleDF.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url",jdbcUrl) \
        .option("dbtable",hist_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

# COMMAND ----------

# df=spark.sql('select * from kgsonedatadbedw.trusted_curr_fact_gl_q10')
# print(df.columns)


# COMMAND ----------

# dim_business_category
# dim_category
# dim_client_geography
# dim_customer
# dim_employee
# dim_employee_address
# dim_employee_assignment_details_base
# dim_employee_bank_details_iexpense
# dim_employee_bank_transfer_format_iexpense
# dim_employee_grade
# dim_employee_group
# dim_employee_referral_details
# dim_entity_master
# dim_et_mis_designations
# dim_et_mis_headcount_overall
# dim_events
# dim_expenditure_types
# dim_fa_location_master
# dim_financial_app_debtors_ageing
# dim_fiscal_cal
# dim_gdcfuturemaster1
# dim_gdcfuturemaster2
# dim_gl_account_master
# dim_gl_function
# dim_gl_ledger_master
# dim_gl_locations
# dim_hr_jobs
# dim_hr_kics
# dim_hr_kicseit
# dim_hr_locations
# dim_hr_operating_units
# dim_hr_phone_type
# dim_hr_positions
# dim_hr_staff_account_trainee_conversion_date
# dim_interoperating_unit
# dim_mis_profit_centre_condensed
# dim_monthly_asset_depreciation
# dim_operating_unit
# dim_org_hierarchy_master
# dim_org_units
# dim_p_project_master_final
# dim_p_project_task_master_final
# dim_pa_labor_multipliers
# dim_pa_lookups
# dim_pa_summary_project_fundings
# dim_pa_tasks
# dim_partner_staff
# dim_people_group_master
# dim_per_all_people_f
# dim_pml_balance
# dim_ppc_designations
# dim_ra_customer_trx_all
# dim_secondee_status_ppc
# dim_tb_gl_period
# dim_tb_mi_group
# dim_transaction_source
# dim_unspecfied_profit_centre
# dim_user_type
# dim_users
#dim_vendor
# dim_vendor_supplier_details

# fact_ap_amex
# fact_ap_invoice_register
# fact_ap_prepayment_status_invoice_payment
# fact_ap_prepayment_status_invoice_query
# fact_ap_prepayment_transaction
# fact_ap_trial_invoice
# fact_ap_trial_payment
# fact_apar
# fact_ar_billing
# fact_ar_collection
# fact_ar_customer_contact_country
# fact_ar_echargeback
# fact_ar_history
# fact_cenvat_billed
# fact_cenvat_collected
# fact_cwk_employee_details
# fact_cwk_trmi_employee_details
# fact_employee_assignments
# fact_employee_qualification
# fact_fa_master
# fact_gl_gst10
# fact_gl_q1
# fact_gl_q10
# fact_gl_q11
# fact_gl_q12
# fact_gl_q13
# fact_gl_q18
# fact_gl_q2
# fact_gl_q21
# fact_gl_q23
# fact_gl_q26
# fact_gl_q3
# fact_gl_q4
# fact_gl_q5
# fact_gl_q6
# fact_gl_q7
# fact_gl_q8
# fact_gl_tb_kgs
# fact_hr_head_count
# fact_hr_leave_details
# fact_lockup_data
# fact_lsp
# fact_p_expenditure
# fact_p_kgs_opes
# fact_p_kgs_staff_utilization
# fact_p_project_invoice
# fact_p_su_std_hours
# fact_p_wip_nfr
# fact_payment_register
# fact_po_details_with_cheque_numbers
# fact_reciept_register
# fact_requisition_purchase_status
# fact_tds_details
# fact_timesheet_status_final
#fact_uar

# COMMAND ----------


    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()

    # if (tableName==''):
    #     print('Data Loaded for ')
    #     sampleDF=spark.sql('select * from kgsonedatadbedw.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("overwrite") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()    
