# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 15:59:11 2023

@author: yfahmy
"""
import pandas as pd
from datetime import date
import os

class SOURCES:
    '''

    Parameters
    ----------
    fiscal_year : int, The default is
        DESCRIPTION: defines the fiscal year to extract from the sql server (consecutively defines, act_cohort, aspire_cohort)
    run_type : str, The default is 'Prelim'
        DESCRIPTION: defines the type of run and so the prefix of the table names in the sql server
    server_name : str, The default is 'AACTASTPDDBVM02'
        DESCRIPTION: defines the name of the server to connect to in the SQL database
    raw_folder : str, The default is None. Optional
        DESCRIPTION: defines the raw string path to save all raw datasets by their corresponding names
    static_folder : str, The default is None. Optional
        DESCRIPTION: defines the raw string path to save all raw datasets by their corresponding names.
    passed_integrity : bool, The default is True.
        DESCRIPTION: If True, grabs only records from FYE table that passed ADMIntegrity
    kg_placement : pandas.TimeStamp, The default is pd.Timestamp(fiscal_year, 1, 1)
        DESCRIPTION: defines the KG placment date cut-off that a kg student must have taken their placement test prior to in order to be considered for EL calculation
    first_placement : pandas.TimeStamp, The default is pd.Timestamp(previous_fiscal_year, 10, 1)
        DESCRIPTION: defines the  placment date cut-off that a any student (other than KG) must have taken their placement test prior to in order to be considered for EL calculation
    end_window : pandas.TimeStamp, The default is pd.Timestamp(fiscal_year, 3, 18)
        DESCRIPTION: defines the end of the AZELLA testing window, after which AZELLA test records will not be considered valid
    math_ela_window : pandas.TimeStamp, pd.Timestamp(fiscal_year, 4, 4)
        DESCRIPTION: defines the statewide assessment window for Math and ELA. Used to prioritze enrollment records for students with multiple records in same school in FYE
    science_window : pd.Timestamp(self.fiscal_year, 3, 21)
        DESCRIPTION: defines the statewide assessment window for Science. Used to prioritze enrollment records for students with multiple records in same school in FYE
    reg_assessement_types : list of str, The default is ['AASA', 'ACTASPIRE', 'AZACT', 'AZSCI']
        DESCRIPTION: defines the assessments names to be included from the AssessmentFamily column in the student assessement table. Names should match DB in spelling and case
    alt_assessement_types : list of str, The default is ['MSAA']
        DESCRIPTION: defines the assessments names to be included from the AssessmentFamily column in the student assessement table. Names should match DB in spelling and case
    academic_subjects : dict, The default is {'English Language Arts':'ELA'
                                              , 'Mathematics':'Math'
                                              , 'Science': 'Sci'}
        DESCRIPTION: defines the subjects to consider from the student assessment table's AcademicSubject column in the DB. Names should match DB in spelling and case
    subject_grade_map : dict, The default is {'English Language Arts':[3,4,5,6,7,8,9,11]
                                               ,'Mathematics':[3,4,5,6,7,8,9,11]
                                               ,'Science': [5,8,11]}
        DESCRIPTION: keys should match in case and spelling student assessment table's AcademicSubject column in the DB.
                     Values is the grade level assessments to consider for each subject.
    performance_level_map : dict, The default is {"Minimally Proficient": 1
                                                ,"Level1": 1
                                                ,"Partially Proficient" :2
                                                , "Level2": 2
                                                ,"Proficient": 3
                                                , "Level3": 3
                                                ,"Highly Proficient": 4
                                                , "Level4": 4}
        DESCRIPTION: a map to convert performance level col in the DB to ordinal values. Keys must match in case and spelling the DB
    azella_proficiency_map : dict, The default is {3711:1
                                                   ,3712:2
                                                   ,3379:2
                                                   ,3380:3
                                                   ,3381:4}
        DESCRIPTION: maps Azella proficiency level codes to ordinal values used in static file, EL growth and prof calc. Keys must match DB 
    federal_n_count : int, The default is 10
        DESCRIPTION: defines the N count rule applied to federal model membership below which a grade enrollment is not considered for the school. determines federal model types.

    Returns
    -------
    None.


    '''
    def __init__(self
                 ,fiscal_year=None, run ='Prelim'
                 , server_name='AACTASTPDDBVM02'
                 , raw_folder=None
                 , static_folder=None
                 , passed_integrity=True
                 , exclude_tuittion_payer_code_2 = False
                 , kg_placement =None
                 , first_placement=None
                 , end_window=None
                 , act_window=None
                 , science_window = None
                 , msaa_window = None
                 , aasa_window = None
                 , aspire_window = None
                 , reg_assessement_types=None
                 , alt_assessement_types=None
                 , academic_subjects=None
                 ,subject_grade_map=None
                 ,test_type_grade_map=None
                 , performance_level_map=None
                 , azella_proficiency_map=None
                 ,federal_n_count=20
                 ,state_n_count = 10
                 , not_eligible_testing_grades=None
                 ,remove_jteds=False
                 ,remove_private_schools=False
                 ,print_status = True
                 ,count_fay_in_schooltype=True):
       
        if fiscal_year is None:
           self.fiscal_year = date.today().year
        else:
            self.fiscal_year = fiscal_year
        
        ## define class variables   
        self.act_grade = 11
        self.aspire_grade = 9
        self.previous_fiscal_year = self.fiscal_year-1
        self.aspire_cohort = self.fiscal_year + 3
        self.act_cohort = self.fiscal_year + 1
        self.raw_folder = raw_folder
        self.static_folder = static_folder
        self.passed_integrity = passed_integrity
        self.exclude_tuittion_payer_code_2 = exclude_tuittion_payer_code_2
        self.remove_jteds=remove_jteds
        self.remove_private_schools=remove_private_schools
        self.count_fay_in_schooltype=count_fay_in_schooltype
        #define run type
        self.run = run.capitalize()
        self.server_name = server_name.upper()
        self.federal_n_count = federal_n_count
        self.state_n_count = state_n_count
        ## define variables for Azella
        if kg_placement is None:
           self.kg_placement =  pd.Timestamp(self.fiscal_year, 1, 1)
        else:
            self.kg_placement = kg_placement
        if first_placement is None:
           self.first_placement =  pd.Timestamp(self.previous_fiscal_year, 10, 1)
        else:
            self.first_placement = first_placement
        if end_window is None:
           self.end_window =  pd.Timestamp(self.fiscal_year, 3, 18)
        else:
            self.end_window = end_window
            
        ##define last school day
        self.last_school_day = pd.Timestamp(self.fiscal_year, 5, 2)
        # print(F'''Closing window set to --> {self.end_window}\
        #       \nKG placement deadline set to --> {self.kg_placement}\
        #       \nAll other placment deadlines set t --> {self.first_placement}''')
        
        #define ACT assessment windows
        if act_window is None:
            self.act_window =  pd.Timestamp(self.fiscal_year, 4, 4)
        else:
            self.act_window = act_window
            
        #define AASA assessment windows
        if aasa_window is None:
            self.aasa_window =  pd.Timestamp(self.fiscal_year, 3, 3)
        else:
            self.aasa_window = aasa_window
        
        ##for the k2 student record coming for prior year
        self.py_aasa_window = pd.Timestamp(self.previous_fiscal_year, 4, 3)
        
            
        #define science assessment window 
        if science_window is None:
            self.science_window =  pd.Timestamp(self.fiscal_year, 3, 20)
        else:
            self.science_window = science_window
            
        #define alt assessment window 
        if msaa_window is None:
            self.msaa_window =  pd.Timestamp(self.fiscal_year, 3, 13)
        else:
            self.msaa_window = msaa_window
        
        # define aspire window
        if aspire_window is None:
            self.aspire_window =  pd.Timestamp(self.fiscal_year, 3, 3)
        else:
            self.aspire_window = aspire_window
        #define reg. assessments type
        # should match in case and spelling to values in AssessmentFamily column in student assessment table in DB
        
        if reg_assessement_types is None:
            self.reg_assessement_types = ['AASA', 'ACTASPIRE', 'AZACT', 'AZSCI']
        else:
            self.reg_assessement_types = reg_assessement_types
        
        #define Alt. assessments type 
        if alt_assessement_types is None:
            self.alt_assessement_types = ['MSAA']
        else:
            self.alt_assessement_types = alt_assessement_types
        # print('Assessments to be included are: ', list(self.assessments_map.keys()))
        
        # Keys should match in case ans spelling AcademicSubject column in student assessment table in DB
        if academic_subjects is None:
            self.academic_subjects = {'English Language Arts':'ELA'
                                       ,'Mathematics':'Math'
                                       ,'Science': 'Sci'}
        else:
            self.academic_subjects = academic_subjects
        
        #define grades for each subject
        if subject_grade_map is None:
            self.subject_grade_map = {'English Language Arts':[3,4,5,6,7,8,9,11]
                                       ,'Mathematics':[3,4,5,6,7,8,9,11]
                                       ,'Science': [5,8,11]}
        else:
            self.subject_grade_map = subject_grade_map
            
        ##define testing grades by test type
        if test_type_grade_map is None:
           self.test_type_grade_map = {'ACTASPIRE':[9]
                                        ,'AZACT':[11]
                                        ,'AASA':[3,4,5,6,7,8]
                                        ,'MSAA':[3,4,5,6,7,8,9,11]}
        else:
              self.test_type_grade_map = test_type_grade_map
        
        
        ## make a list of not tested grades to add records to static file as ineligible
        if not_eligible_testing_grades is None:
            self.not_eligible_testing_grades = [66,77,-1,1,2,10,12]
        else:
            self.not_eligible_testing_grades =not_eligible_testing_grades
            
        ## Keys should match in case and spelling PerformanceLevelDescription column in student assessment table in DB
        if performance_level_map is None:   
            self.performance_level_map = {"Minimally Proficient": 1
                                        ,"Level1": 1
                                        ,"Partially Proficient" :2
                                        , "Level2": 2
                                        ,"Proficient": 3
                                        , "Level3": 3
                                        ,"Highly Proficient": 4
                                        , "Level4": 4}
        else:
            self.performance_level_map = performance_level_map
        
        #define AZELLA prof cuts
        if azella_proficiency_map is None: 
            self.azella_proficiency_map = {3711:1
                               ,3712:2
                               ,3379:2
                               ,3380:3
                               ,3381:4}
        else:
            self.azella_proficiency_map = azella_proficiency_map
            
        ##define a map for SchooltypeF
        self.federal_school_type_map = {1:'k2'
                                        ,2:'k8'
                                        ,3:'912'
                                        ,4:'k11'
                                        ,5:'k12'}
        

        ## to be able to print status if using individual classes outside of static
        if print_status:
            print(F'\nFiscal year defined as {self.fiscal_year}')
            print(F'\nserver set to---> {self.server_name}')
            print(F'\nRun Type set to---> {self.run}')
            print(F'\nADMIntegrity restriction --> {self.passed_integrity}')
            print(F'\nremove_jteds --> {self.remove_jteds}')
            print(F'\nremove_private_schools--> {self.remove_private_schools}')
            print(F'\nexclude_tuittion_payer_code_2 --> {self.exclude_tuittion_payer_code_2}')
            print(f'Sci assessment window --> {self.science_window }')
            print(f'Math and ELA ACT assessment window --> {self.act_window }')
            print(f'Math and ELA AASA assessment window --> {self.aasa_window }')
            print(f'Alternative assessment window --> {self.msaa_window }')
            print(F'AZELLA closing window set to --> {self.end_window}')
            print(F'AZELLA KG placement deadline set to --> {self.kg_placement}')
            print(F'All other placment deadlines set to --> {self.first_placement}')
    
    def save_data(self, df, path_to_save, file_name):
        try:
            path_to_save = os.path.join(path_to_save, file_name)
            df.to_csv(path_to_save, index=False, na_rep='')
        except Exception as ex:
            print('Error in saving data')
            print(ex)