# -*- coding: utf-8 -*-
"""
Created on Wed May 10 10:50:37 2023

@author: YFahmy
"""

import pandas as pd
from SOURCES import SOURCES
from DATABASE import DB
from FYE import FYE
from ASSESSMENTS import ASSESSMENTS
import numpy as np

class K2(SOURCES):
        
    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        
        
        
    def format_data(self, py_enrollment, school_type, assessments):
# =============================================================================
#         cy_db =  DB(fiscal_year=self.fiscal_year, run=self.run)
#         school_type = cy_db.read_table(table_name='SchoolType')
#         
#         ## for_k2 fetches prior year record from table for 2nd grades only
#         py_enrollment = FYE(fiscal_year=self.fiscal_year, run='Prelim', print_status=False).format_enrollment(for_k2=True)
#         assessments = ASSESSMENTS(fiscal_year=self.fiscal_year, run='Prelim', print_status=False).format_assessments()
# =============================================================================
        
        ## isolate k2 schools in the current year and fetch their records from prior year
        k2_schools = school_type.loc[(school_type.SchoolTypeF == 1), 'SchoolCode'].copy()
        
        enrollment = pd.merge(py_enrollment, k2_schools, on='SchoolCode', how='inner')
        ##change these records grade to 3rd so as to merge with 3rd grade assessment
        enrollment['StudentGrade'] = 3
        
        ##drop fiscal year since we use the one in fye
        assessment = assessments.copy()
        assessment.drop('FiscalYear', axis=1, inplace=True)
        
        ## merge in their 3rdgrade assessment to their prior year record at the k2 school
        staticfile = pd.DataFrame()
        for subject in self.subject_grade_map.keys():
            sub = self.academic_subjects[subject]
            sub_assessment = assessment[assessment.Subject==sub]
            sub_enrollment = enrollment [enrollment.StudentGrade.isin(self.subject_grade_map[subject])]
            sub_static = pd.merge(left=sub_enrollment, right=sub_assessment, left_on=['SAISID','StudentGrade']
                                   , right_on=['SAISID','AssessmentGrade'], how='left', suffixes=('','_assessments'))
            #set subject for all records to suject in loop
            sub_static.loc[:,'Subject'] = sub
            if 'sci' in  sub.lower():
                window = 'SciWindow'
            else:
                window = 'ELAMathWindow'
            
            ##adjust the ELAMathWindow / SciWindow accordingly to accomodate MSAA window for kids tested in MSAA
            ###first make all MSAA takers window 0 then only 1 if they were enrolled during MSAA window
            ## the logic here being, if kids tested in MSAA then we hold the school accountable to their MSAA testing wndow (for percent tested)
            ## oterwise if not tested then we hold school accountable to regular testing windows for their grade level
            msaa_window_mask = (sub_static.EntryDate <= self.msaa_window) & (
                (sub_static.ExitDate >= self.msaa_window) | (sub_static.ExitDate.isnull())) & (sub_static.AssessmentFamily.isin(self.alt_assessement_types))
            sub_static.loc [sub_static.AssessmentFamily.isin(self.alt_assessement_types), window] = 0
            sub_static.loc [msaa_window_mask, window] = 1
            
            #de-duplicate to keep one record per kid per school enrollment per grade giving priority to tested records
            #Make a tested indicator to give priority to keep records with a test score 
            sub_static['Tested'] = sub_static.ScaleScore.notnull()
            sub_static.sort_values(['SAISID','SchoolCode','Tested', window, 'FAY'], ascending=False, inplace=True)
            sub_static = sub_static[~sub_static[['SAISID', 'SchoolCode']].duplicated(keep='first')]
            # if ScaleScore.isnull() mark record as NotTested
            sub_static.loc[sub_static.ScaleScore.isnull() , 'AssessmentFamily'] = 'NotTested'
            ## concat the inner joined temp static file to static file
            staticfile = pd.concat([staticfile, sub_static])        
        ### change student Grade back to 2
        staticfile['StudentGrade'] = 101
        #add statewide tested indicator
        staticfile['StateWideTested'] = 0
        staticfile.loc[staticfile.ScaleScore.notnull(), 'StateWideTested'] = 1
        ## add passing indicator
        staticfile['Passing'] = 0
        staticfile.loc[staticfile.Performance.isin([3,4]), 'Passing'] = 1
        staticfile.loc[staticfile['ScaleScore'].isnull(), 'Passing'] = np.nan
        return staticfile
        

        
        
        