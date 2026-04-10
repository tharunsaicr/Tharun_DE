# Databricks notebook source
dbutils.widgets.text(name = "DeltaTableName", defaultValue = "")
tableName = dbutils.widgets.get("DeltaTableName")

dbutils.widgets.text(name = "ReportName", defaultValue = "")
reportName = dbutils.widgets.get("ReportName")

dbutils.widgets.text(name = "ProcessName", defaultValue = "")
processName = dbutils.widgets.get("ProcessName")

table = "trusted_curr_"+reportName+"_"+processName+"_"+tableName
hist_table = "trusted_hist_bu_report_hyperion_dg_data_validation"

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

#load trusted hist tables for cf_pack from delta to sql 
if (reportName =='bu_report') and (processName =='hyperion_dg'):
    print('true')

    #CF Function
    # if table == 'trusted_curr_bu_report_hyperion_dg_hcplan_forecast_pbi_report':
    print(table)
    query=f"select distinct Financial_Year,File_Name from kgsfinancedb."+ table
    distinct_values= spark.sql(query)
    for row in distinct_values.collect():
        financial_year=row.Financial_Year
        file_name=row.File_Name
        # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
        # fy=fy[0]
        # print(fy)
        sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
        cursor.execute(sql_command)
        result = cursor.fetchone()[0]
        if result == 1:
            query="DELETE FROM dbo.{} where Financial_Year = '{}' and file_name='{}'".format(hist_table,financial_year,file_name)
            print(query)
            conn.execute(query)
            conn.commit()
            print('delete executed')
    
    sampleDF=spark.sql('select * from kgsfinancedb.'+table)

    sampleDF.write \
    .format("jdbc") \
    .mode("append") \
    .option("url",jdbcUrl) \
    .option("dbtable",hist_table) \
    .option("user", username) \
    .option("password", password) \
    .save()
    print("table :"+ hist_table)


    # if table == 'trusted_curr_bu_report_hyperion_dg_opex_forecast_pbi_report':
    #     print(table)
    #     distinct_values= spark.sql('select distinct Financial_Year,File_Name from kgsfinancedb.trusted_curr_bu_report_hyperion_dg_opex_forecast_pbi_report')
    #     for row in distinct_values.collect():
    #         financial_year=row.Financial_Year
    #         file_name=row.File_Name
    #         # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
    #         # fy=fy[0]
    #         # print(fy)
    #         sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
    #         cursor.execute(sql_command)
    #         result = cursor.fetchone()[0]
    #         if result == 1:
    #             query="DELETE FROM dbo.{} where Financial_Year = '{}' and file_name='{}'".format(hist_table,financial_year,file_name)
    #             print(query)
    #             conn.execute(query)
    #             conn.commit()
    #             print('delete executed')
        
    #     sampleDF=spark.sql('select * from kgsfinancedb.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("append") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()
    #     print("table :"+ hist_table)

    # if table == 'trusted_curr_bu_report_hyperion_dg_projects_forecast_pbi_report':
    #     print(table)
    #     distinct_values= spark.sql('select distinct Financial_Year,File_Name from kgsfinancedb.trusted_curr_bu_report_hyperion_dg_projects_forecast_pbi_report')
    #     for row in distinct_values.collect():
    #         financial_year=row.Financial_Year
    #         file_name=row.File_Name
    #         # fy=year.select('YearMonth').rdd.flatMap(lambda x: x).collect()
    #         # fy=fy[0]
    #         # print(fy)
    #         sql_command = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{hist_table}'"
    #         cursor.execute(sql_command)
    #         result = cursor.fetchone()[0]
    #         if result == 1:
    #             query="DELETE FROM dbo.{} where Financial_Year = '{}' and file_name='{}'".format(hist_table,financial_year,file_name)
    #             print(query)
    #             conn.execute(query)
    #             conn.commit()
    #             print('delete executed')
        
    #     sampleDF=spark.sql('select * from kgsfinancedb.'+table)

    #     sampleDF.write \
    #     .format("jdbc") \
    #     .mode("append") \
    #     .option("url",jdbcUrl) \
    #     .option("dbtable",hist_table) \
    #     .option("user", username) \
    #     .option("password", password) \
    #     .save()
    #     print("table :"+ hist_table)


# COMMAND ----------

