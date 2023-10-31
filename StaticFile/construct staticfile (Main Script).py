# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 14:40:15 2023

@author: yfahmy
"""
#%%
from STATIC import STATIC
import pandas as pd
from DATABASE import DB

fiscal_year=2023
previous_fiscal_year = fiscal_year-1
run='PrelimV6'
server_name='AACTASTPDDBVM02'
passed_integrity=False
raw_folder=None
static_folder=None
remove_jteds=False
remove_private_schools=False
exclude_tuittion_payer_code_2 = False
count_fay_in_schooltype=True

## define dates for Azella
kg_placement = pd.Timestamp(fiscal_year, 1, 1)
first_placement = pd.Timestamp(previous_fiscal_year, 10, 1)
end_window = pd.Timestamp(fiscal_year, 3, 18)

#define dates for state assessment window
science_window =  pd.Timestamp(fiscal_year, 3, 20)
act_window =  pd.Timestamp(fiscal_year, 4, 4)
msaa_window = pd.Timestamp(fiscal_year, 3, 13)
aasa_window =  pd.Timestamp(fiscal_year, 4, 3)
aspire_window = pd.Timestamp(fiscal_year, 4, 3)
# values should match in case and spelling to values in AssessmentFamily column in student assessment table in DB
reg_assessement_types = ['AASA', 'ACTASPIRE', 'AZACT', 'AZSCI']
alt_assessement_types = ['MSAA']

#subject grades to consider, Keys should match in case and spelling AcademicSubject column in student assessment table
subject_grade_map = {'English Language Arts':[3,4,5,6,7,8,9,11]
                           ,'Mathematics':[3,4,5,6,7,8,9,11]
                           ,'Science': [5,8,11]}
##
test_type_grade_map = {'ACTASPIRE':[9]
                        ,'AZACT':[11]
                        ,'AASA':[3,4,5,6,7,8]
                        ,'MSAA':[3,4,5,6,7,8,9,11]}
## make a list of grades not eligible for testing to be marked as ineligible in assessement family col
not_eligible_testing_grades = [66,77,-1,1,2,10,12]

#define raw and staticfolders to save a copy of tables
raw_folder = r'H:\ACCT\ACCOUNTABILITY\2023\Yassin\snapshot backup'

#%%
stat = STATIC(fiscal_year= fiscal_year
              ,run= run
              ,server_name= server_name
             ,raw_folder= raw_folder
             ,static_folder= static_folder
             ,passed_integrity= passed_integrity
             ,kg_placement= kg_placement
             ,first_placement= first_placement
             ,end_window= end_window 
             ,act_window = act_window
             ,science_window = science_window
             ,reg_assessement_types= reg_assessement_types
             ,alt_assessement_types= alt_assessement_types
             ,subject_grade_map= subject_grade_map
             ,test_type_grade_map= test_type_grade_map
             ,not_eligible_testing_grades=not_eligible_testing_grades
             ,msaa_window = msaa_window
             ,aasa_window = aasa_window
             ,aspire_window=aspire_window
             ,remove_jteds=remove_jteds
             ,remove_private_schools=remove_private_schools
             ,exclude_tuittion_payer_code_2=exclude_tuittion_payer_code_2
             ,count_fay_in_schooltype=count_fay_in_schooltype)

#%% Only use if snapshot doesn't already exist in DB
stat.snapshot_raw_data()

stat.format_upload_chronic_absenteeisim()

#read in growth file from Dr.B and upload it to DB
file_path = r"H:\ACCT\ACCOUNTABILITY\2023\A-F\Final 2023\Growth\Arizona_SGP_LONG_Data_2023.Rdata"
stat.format_upload_sgp(file_path=file_path)
#%%
#This functions grabs raw data from DB and formats it (depending on the server defined in instance)
##data is available as attributes of the STATIC object instance (stat in this case)
stat.format_basefiles()
stat.format_staticfile(keep_all_schools=False)

#%%
stat.upload_school_type()
stat.upload_staticfile()
#%% Upload  growth based staticfile if made
fy= 2023
table_name='StaticFileData'
run = ''
DB(fiscal_year = fy
    ,run = run
    ,schema = 'dbo'
    ,database = 'AccountabilityArchive').upload_table_to_db(df=stat.staticfile, table_name=table_name)
#%%

stat.drop_raw_tables(table_prefix=run)
stat.drop_chronic_absenteeisim(table_prefix=run)
stat.drop_sgp(table_prefix=run)

stat.drop_staticfile(table_prefix=run)
stat.drop_school_type(table_prefix=run)







