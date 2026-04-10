# Databricks notebook source
# DBTITLE 1,Headcount Weekly

select * from trusted_hist_headcount_contingent;
select * from trusted_hist_headcount_contingent_worker;
select * from trusted_hist_headcount_contingent_worker_resigned;
select * from trusted_hist_headcount_employee_details;
select * from trusted_hist_headcount_employee_dump;
select * from trusted_hist_headcount_leave_report;
select * from trusted_hist_headcount_loaned_staff_from_ki;
select * from trusted_hist_headcount_loaned_staff_resigned;
select * from trusted_hist_headcount_maternity_cases;
select * from trusted_hist_headcount_maternity_cases;
select * from trusted_hist_headcount_resigned_and_left;
select * from trusted_hist_headcount_sabbatical;
select * from trusted_hist_headcount_secondee_outward;
select * from trusted_hist_headcount_talent_konnect_resignation_status_report;
select * from trusted_hist_headcount_termination_dump;





truncate table trusted_hist_headcount_contingent;
truncate table trusted_hist_headcount_contingent_worker;
truncate table trusted_hist_headcount_contingent_worker_resigned;
truncate table trusted_hist_headcount_employee_details;
truncate table trusted_hist_headcount_employee_dump;
truncate table trusted_hist_headcount_leave_report;
truncate table trusted_hist_headcount_loaned_staff_from_ki;
truncate table trusted_hist_headcount_loaned_staff_resigned;
truncate table trusted_hist_headcount_maternity_cases;
truncate table trusted_hist_headcount_maternity_cases;
truncate table trusted_hist_headcount_resigned_and_left;
truncate table trusted_hist_headcount_sabbatical;
truncate table trusted_hist_headcount_secondee_outward;
truncate table trusted_hist_headcount_talent_konnect_resignation_status_report;
truncate table trusted_hist_headcount_termination_dump;

# COMMAND ----------

# DBTITLE 1,Headcount Monthly
select * from  trusted_hist_headcount_monthly_contingent_worker;
select * from  trusted_hist_headcount_monthly_contingent_worker_resigned;
select * from  trusted_hist_headcount_monthly_employee_details;
select * from  trusted_hist_headcount_monthly_loaned_staff_from_ki;
select * from  trusted_hist_headcount_monthly_loaned_staff_resigned;
select * from  trusted_hist_headcount_monthly_resigned_and_left;
select * from  trusted_hist_headcount_monthly_sabbatical;
select * from  trusted_hist_headcount_monthly_secondee_outward;
select * from  trusted_hist_headcount_monthly_academic_trainee;
select * from  trusted_hist_headcount_monthly_maternity_cases;


truncate table trusted_hist_headcount_monthly_contingent_worker;
truncate table trusted_hist_headcount_monthly_contingent_worker_resigned;
truncate table trusted_hist_headcount_monthly_employee_details;
truncate table trusted_hist_headcount_monthly_loaned_staff_from_ki;
truncate table trusted_hist_headcount_monthly_loaned_staff_resigned;
truncate table trusted_hist_headcount_monthly_resigned_and_left;
truncate table trusted_hist_headcount_monthly_sabbatical;
truncate table trusted_hist_headcount_monthly_secondee_outward;
truncate table trusted_hist_headcount_monthly_academic_trainee;
truncate table trusted_hist_headcount_monthly_maternity_cases;

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Compensation
select * from trusted_hist_compensation_additional_pay;
select * from trusted_hist_compensation_paysheet;
select * from trusted_hist_compensation_yec;
select * from trusted_hist_compensation_finance_metrics;


truncate table trusted_hist_compensation_additional_pay;
truncate table trusted_hist_compensation_paysheet;
truncate table trusted_hist_compensation_yec;
truncate table trusted_hist_compensation_finance_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select EMP_NO,NAME,NJ_INDICATOR,SALARY_INDICATOR,ACTIVE_IND,BIRTH_DATE,JOINING_DATE,DATE_OF_JOINING_FOR_GRATUITY,LEAVING_DATE,LOCATION,SUB_LOCATION,STATE,DESIGNATION,DIVISION_NAME,COMPANY,COMPANY_NAME,OPERATING_UNIT,GRADE_NAME,COSTCENT,COSTCENT_NAME,CC_PAY_MOD,DEPARTMENT,PF_NO,UAN,ADHAR,PRAN,CATEGORY,GEO,USER_TYPE,BUSS_CTGY,ORG_ID,EMP_PAN,STANDARD_DAYS,LWP_DAYS,DAYS_PAYABLE,PAYMENT_MODE,BANK_NAME,BANK_ACCOUNT,IFSC_CODE,BENE_NAME,M_BASIC,M_H_R_A_,M_SPECIAL_ALLOWANCE,M_CONVEYANCE_ALLW,M_CHILD_EDUCATION,M_STIPEND,CTC,MCTC,BASIC,ARREAR_BASIC,H_R_A_,ARREAR_HRA,SPECIAL_ALLOW,ARREAR_SPECIAL_ALLOW,CONVEYANCE_ALLOW,ARREAR_CONVEYANCE_ALLOW,CHILD_EDUCATION,ARREAR_CHILD_EDUCATION,STIPEND,ARREAR_STIPEND,COMPENSATORY_OFF_,LOYALTY_BONUS,PERFORMANCE_BONUS,EXAM_BONUS,SECONDMENT_IAP,INCENTIVE,NOTICE_PAY_BUYOUTS,OTHER_EARNINGS___NT,HOT_SKILL_ALLOWANCE,OTHER_EARNINGS,REFERRAL_BONUS,ONE_TIME_BONUS,RELOCATION_ALLOWANCE,RETENTION_BONUS,SHIFT_ALLOWANCE,SIGN_ON_BONUS,EXGRATIA,EXTRA_DAY_PAYMENT,BUSY_SEASON_BONUS,JOINING_BONUS,TAX_PERQUISITE,PT_PERQUISITE,PF_PERQUISITE,One_Time_WAH_Allowance,TIME_SHEET_NETPAY_REVERSAL,PAN_NO_DEFAULT_REVERSAL,BANK_DEFAULT_NETPAY_REVERSAL,RESIGNED_NETPAY_REVERSAL,SALARY_REJECTION,GROSS,P_F_,V_P_F_,PROFESSION_TAX,ESIC,L_W_F_,INCOME_TAX,SALARY_ADVANCE,DONATION,TRAVEL_ADVANCE_RECOVERY,RISK_PENALTY,OTHER_DEDUCTION,MEDICLAIM_FOR_SELF_SPOUSE___CH,KPMG_FOUNDATION,COVID_KAWACH,ID_CARD_RECOVERY,TIMESHEET_DEFAULTER_NET_PAY_HO,PAN_DEFAULTER_NET_PAY_HOLD,BANK_ACCOUNT_DEFAULT_HOLD,RESIGNED_HOLD,ADVANCE,Adhoc_Loan_Principal,TOT_DED,NET_PAY,EMPLOYER_PF,EMPLOYER_ESIC,EMPLOYER_LWF,NPS,Dated_On,File_Date,left(File_Date,6) as Month_Key from kgsonedatadb.trusted_hist_compensation_paysheet

# COMMAND ----------

# DBTITLE 1,BGV
select * from trusted_hist_bgv_offer_release;
select * from trusted_hist_bgv_progress_sheet;
select * from trusted_hist_bgv_kcheck;	
select * from trusted_hist_bgv_upcoming_joiners;



truncate table trusted_hist_bgv_offer_release;
truncate table trusted_hist_bgv_progress_sheet;
truncate table trusted_hist_bgv_kcheck;	
truncate table trusted_hist_bgv_upcoming_joiners;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select distinct Business_Unit___Name from kgsonedatadb.trusted_hist_bgv_offer_release

# COMMAND ----------

# DBTITLE 1,TA
select * from trusted_hist_talent_acquisition_kgs_joiners_report;
select * from trusted_hist_talent_acquisition_requisition_dump; 


truncate table trusted_hist_talent_acquisition_kgs_joiners_report;
truncate table trusted_hist_talent_acquisition_requisition_dump; 

# COMMAND ----------

# DBTITLE 1,lnd
select * from trusted_hist_lnd_glms_kva_details

truncate table trusted_hist_lnd_glms_kva_details

# COMMAND ----------

# DBTITLE 1,Global Mobility
truncate table trusted_hist_global_mobility_l1_visa_tracker;
truncate table trusted_hist_global_mobility_non_us_tracker;
truncate table trusted_hist_global_mobility_opportunity_dump;
truncate table trusted_hist_global_mobility_secondment_tracker;
truncate table trusted_hist_global_mobility_secondment_details;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select Dated_On,count(1) from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned group by Dated_On -- 2023-04-18T15:23:22.191+0000
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned where Dated_On in (select max(Dated_On) from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned group by Dated_On)
# MAGIC
# MAGIC -- select * from kgsonedatadb.trusted_hist_headcount_loaned_staff_resigned where Dated_On like '%2023-04-18T15:23:22%'
# MAGIC

# COMMAND ----------

# DBTITLE 1,JML
truncate table  trusted_hist_jml_germany_joiner;
truncate table  trusted_hist_jml_germany_leaver;
truncate table  trusted_hist_jml_germany_mover;
truncate table  trusted_hist_jml_nl_joiner;
truncate table  trusted_hist_jml_nl_leaver;
truncate table  trusted_hist_jml_nl_mover;
truncate table  trusted_hist_jml_uk_joiner;
truncate table  trusted_hist_jml_uk_leaver;
truncate table  trusted_hist_jml_uk_mover;
truncate table  trusted_hist_jml_us_joiner;
truncate table  trusted_hist_jml_us_leaver;
truncate table  trusted_hist_jml_us_mover;  

# COMMAND ----------

# DBTITLE 1,EE
truncate table trusted_hist_employee_engagement_thanks_dump;
truncate table trusted_hist_employee_engagement_encore_output;
truncate table trusted_hist_employee_engagement_gps;
truncate table trusted_hist_employee_engagement_rock;
truncate table trusted_hist_employee_engagement_year_end;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES LIKE 'trusted_hist*'

# COMMAND ----------

