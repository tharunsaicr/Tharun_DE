# Databricks notebook source
# MAGIC %run
# MAGIC /kgsfinance/common_utilities/connection_configuration

# COMMAND ----------

# MAGIC %run 
# MAGIC /kgsfinance/common_utilities/common_components

# COMMAND ----------

from pyspark.sql.functions import col, ltrim
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import expr

def unpivotdf(df):
    #overall columns - spaces removed from column names
    overalldf = df.select([col(c).alias(c.replace(' ', '_')) for c in df.columns])

    #list of columns for unpivoting
    Accounts = ["Base_Compensation","Base_Compensation_Additional","Personnel_Cost___Others_Additional","Personnel_Cost___Others","Insurance_premium","Gratuity_and_Leave_Encashment","Gratuity_and_Leave_Encashment_Additional","Bonus","Bonus_Additional","Placement_Agency_Cost","Employee_Referral_Cost","Recruitment_Cost","Recruitment_Cost_additional","Background_Verification_Charges","Direct_Cost","Adhoc_Cost","Company_Transport","Company_Transport_Additional","Adhoc_Transport","Contractors_cost","Professional_Fees-_Loaned_Staff","Professional_Fees__Loaned_Staff_Additional","House_Allowance","Secondee_Salary","Stewardship_cost_Additional","Secondee_Transport","Professional_Fees___Secondee","Secondee_Other_Expenses","Opening_HC","Revenue_Generating_Opening_Headcount","Business_Development_Opening_Headcount","Revenue_Generating_Secondee_Opening_Headcount","Contractor_Headcount","Chargeable_Headcount","BD_Headcount","NAP_Headcount","Support_Headcount","Revenue_Generating_Utilization__","Statutory_Holidays_Hour","Leave_Hours","Training_Hours","NAP_Hours","Business_Development_Hours","SGI_NAP_Hours","SGI_External_Hours","Support_Hours","Chargeable_Hours","ReChargeable_Hours","BD_Hours","NDU_Hours","Nationally_Approved_Projects_Hours","Contractor_Hours","Lent_Hours","Borrowed_Hours","Borrowed_BD_Hours","Borrowed_NAP_Hours","FTE","Shift_Allowance","Internet_Allowance","Joining_Bonus","Relocation_Bonus","Stewardship_cost","Lent_BD_Hours","Lent_NAP_Hours","New_Joiners_Chargeable","New_Joiners_Support","New_Recruits_HC","Revenue_Generating_BackFill","Support_BackFill","WAH_Allowance","Retention_Bonus","Staff_Welfare___Offsite","Staff_Welfare___Offsite_Additional","Staff_Welfare___Others","Staff_Welfare___Others_Additional","Staff_Welfare___Others_SL","Printing___Stationery","Printing___Stationery_Additional","Professional_Fees___Other","Professional_Fees___Other_Additional","TP_Assessment_related____Accountants_report","Mark_up_on_Total_Costs","Mark_up_on_Total_Costs_Additional", "GTP Bonus","PC Event Additional","PC Event","Encore","R_R","R_R_SL","CSR","KIN","Other Training","Training","Training Cost Additional","Training_SL","CLP","Subscriptions","Subscriptions_SL","Subscriptions_Entity","Database","Database Additional","Communication Cost","Communication Cost Additional","Audit Expenses","IFRS cost Conversion","TP Assessment related","Secretarial Audit fees","Actuarial Valuation LE and Gratuity","Service tax refund related costs","PF Admin charges","Consultant Fees PF","Legal fees","Fees and subscription","Other Misc","Marketing Budget","Miscellaneous","Miscellaneous Additional","Management Allocation","Management Allocation Additional","IT Project Spend","CMT Cost","CMT Cost Additional","OMT Cost","Shared Services","Shared Services Tax","Shared Services Additional","Facilities and Seat cost","Facilities and Seat cost Additional","Hyderabad Facilities","IT Strategy Cost Additional","Transformations","Transformations Additional","Arrow Toll","Technical Training","Enabling Training","All Year Meal Allowance"
]
    unpivot_cols = [x for x in overalldf.columns if any(x in y or y in x for y in Accounts)]

    #cast the month columns to string type 
    overalldf = colcaststring(overalldf,unpivot_cols)

    #other columns apart from unpivot columns
    othercolsdf = overalldf.drop(*unpivot_cols)
    other_cols = othercolsdf.columns

    #find number of columns for unpivoting
    columnCount = str(len(unpivot_cols))

    #preparing dynamic string for stack function - unpivot string
    stackcols = ",".join(["'" + x + "'," + x for x in unpivot_cols])
    finalstackstring = columnCount + "," + stackcols
    finalstackstring = 'stack({}) as (ACCOUNT,DATA)'.format(finalstackstring)

    #unpivot function using the finalstackstring created
    unpivotdf = overalldf.select(*other_cols, expr(finalstackstring))

    # unpivotdf.groupBy("Entity").pivot('Entity').agg(F.first("DATA"))
    
    return unpivotdf

# COMMAND ----------

# from pyspark.sql.functions import expr, col

# # Define the columns to be unpivoted
# columns_to_unpivot = df_curr.columns

# # Create the expression for unpivoting
# unpivot_expr = ",".join([f"('{col}', {col})" for col in columns_to_unpivot])

# # Unpivot the dataframe
# unpivoted_df = df_curr.selectExpr("*", "stack({0}) as (ACCOUNT, DATA)".format(unpivot_expr))

# # Print the row and column counts
# print("new Row count:", unpivoted_df.count())
# print("new Column count:", len(unpivoted_df.columns))

# COMMAND ----------

from pyspark.sql.functions import col, ltrim
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import expr

def unpivotded(df):
    #overall columns - spaces removed from column names
    overalldf = df.select([col(c).alias(c.replace(' ', '')) for c in df.columns])

    #list of columns for unpivoting
    Accounts = ["Base_Compensation","Base_Compensation_Additional","Personnel_Cost___Others_Additional","Personnel_Cost___Others","Insurance_premium","Gratuity_and_Leave_Encashment","Gratuity_and_Leave_Encashment_Additional","Bonus","Bonus_Additional","Placement_Agency_Cost","Employee_Referral_Cost","Recruitment_Cost","Recruitment_Cost_additional","Background_Verification_Charges","Direct_Cost","Adhoc_Cost","Company_Transport","Company_Transport_Additional","Adhoc_Transport","Contractors_cost","Professional_Fees-_Loaned_Staff","Professional_Fees__Loaned_Staff_Additional","House_Allowance","Secondee_Salary","Stewardship_cost_Additional","Secondee_Transport","Professional_Fees___Secondee","Secondee_Other_Expenses","Opening_HC","Revenue_Generating_Opening_Headcount","Business_Development_Opening_Headcount","Revenue_Generating_Secondee_Opening_Headcount","Contractor_Headcount","Chargeable_Headcount","BD_Headcount","NAP_Headcount","Support_Headcount","Revenue_Generating_Utilization__","Statutory_Holidays_Hour","Leave_Hours","Training_Hours","NAP_Hours","Business_Development_Hours","SGI_NAP_Hours","SGI_External_Hours","Support_Hours","Chargeable_Hours","ReChargeable_Hours","BD_Hours","NDU_Hours","Nationally_Approved_Projects_Hours","Contractor_Hours","Lent_Hours","Borrowed_Hours","Borrowed_BD_Hours","Borrowed_NAP_Hours","FTE","Shift_Allowance","Internet_Allowance","Joining_Bonus","Relocation_Bonus","Stewardship_cost","Lent_BD_Hours","Lent_NAP_Hours","New_Joiners_Chargeable","New_Joiners_Support","New_Recruits_HC","Revenue_Generating_BackFill","Support_BackFill","WAH_Allowance","Retention_Bonus"]
    unpivot_cols = [x for x in overalldf.columns if any(x in y or y in x for y in Accounts)]

    #cast the month columns to string type 
    overalldf = colcaststring(overalldf,unpivot_cols)

    #other columns apart from unpivot columns
    othercolsdf = overalldf.drop(*unpivot_cols)
    other_cols = othercolsdf.columns

    #find number of columns for unpivoting
    columnCount = str(len(unpivot_cols))

    #preparing dynamic string for stack function - unpivot string
    stackcols = ",".join(["'" + x + "'," + x for x in unpivot_cols])
    finalstackstring = columnCount + "," + stackcols
    finalstackstring = "stack({}) as (ACCOUNT,DATA)".format(finalstackstring)

    #unpivot function using the finalstackstring created
    unpivoted_df = overalldf.selectExpr(*other_cols, expr(finalstackstring).alias("ACCOUNT"), expr("DATA"))
    
    return unpivotdf

# COMMAND ----------

def unpivotdf_bu(df):
    
    #list of cols NOT to be unpivoted
    other_cols = ["BusinessCategory", "Entity", "Employee_Type", "Role", "Operating Unit", "Geo", "Location", "CostCenter", "Version", "Years", "Scenario", "Currency", "Period"]

    #list of columns for unpivoting 
    unpivotcolsdf = df.drop(*other_cols)
    unpivot_cols = unpivotcolsdf.columns

    #find number of columns for unpivoting
    columnCount = str(len(unpivot_cols))

    #preparing dynamic string for stack function - unpivot string
    stackcols = ",".join(["'" + x + "'," + x for x in unpivot_cols])
    finalstackstring = columnCount + "," + stackcols
    finalstackstring = "stack({}) as (ACCOUNT,DATA)".format(finalstackstring)

    #unpivot function using the finalstackstring created
    unpivotdf = df.select(*other_cols, expr(finalstackstring))
    #display(unpivotdf)
    
    return unpivotdf