# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 18:39:27 2022

@author: yfahmy
"""
import pandas as pd
from datetime import date
from termcolor import colored
from GROWTH import GROWTH
from CENSUS import CENSUS
from SCHOOLTYPE import EDORG
from FYE import FYE
from AZELLA import AZELLA
from ASSESSMENTS import ASSESSMENTS
from DATABASE import DB
from CHRONIC_ABSENTEEISIM import CA
import os
import pyreadr
import numpy as np
from SOURCES import SOURCES
from K2 import K2 as K2

class STATIC(SOURCES):
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
    def __init__(self, fiscal_year=None, run='Prelim',  **kwargs):
         
        ##pass all arguments to superclass
        super().__init__(fiscal_year=fiscal_year, run=run,**kwargs)

        ## make instances of accessor classes
        self.fye    = FYE(fiscal_year=self.fiscal_year, run=self.run, print_status=False, **kwargs)

        self.ed_org = EDORG(fiscal_year=self.fiscal_year, run=self.run, print_status=False,**kwargs)

        self.azella = AZELLA(fiscal_year=self.fiscal_year, run=self.run, print_status=False,**kwargs)

        self.assess = ASSESSMENTS(fiscal_year=self.fiscal_year, run=self.run, print_status=False,**kwargs)

        self.census = CENSUS(fiscal_year=self.fiscal_year, run=self.run, print_status=False,**kwargs)
        
        if self.server_name == 'AACTASTPDDBVM01':
            live_server = True
        else:
            live_server = False
        self.db     = DB(fiscal_year=self.fiscal_year, run=self.run, live_server=live_server)

 
        self.ca     = CA(fiscal_year=self.fiscal_year, run=self.run, print_status=False,**kwargs)

        self.growth = GROWTH(fiscal_year=self.fiscal_year, run=self.run, print_status=False,**kwargs)

        self.k2 = K2(fiscal_year=self.fiscal_year, run=self.run, print_status=False, **kwargs)
    def format_schooltype(self):
        self.enrollment = self.fye.format_enrollment()
        self.school_type = self.ed_org.format_school_type(enrollment=self.enrollment, remove_jteds=self.remove_jteds, remove_private_schools=self.remove_private_schools)
        
    def format_basefiles(self):
        
        ## produce static folder components
        self.jted = self.ed_org.format_jteds()
        
        self.enrollment = self.fye.format_enrollment()
        print('\nGetting PY Enrollment records for k2 schools')
        self.py_enrollment = self.fye.format_enrollment(for_k2=True)
        
        self.school_type = self.ed_org.format_school_type(enrollment=self.enrollment, remove_jteds=self.remove_jteds, remove_private_schools=self.remove_private_schools)
        
        self.assessments = self.assess.format_assessments()
        
        self.el = self.azella.format_azella()
        
        self.sped = self.census.format_census()
        
        self.k2_sf = self.k2.format_data(self.py_enrollment, self.school_type, self.assessments)
        
        if self.server_name == 'AACTASTPDDBVM02':
            self.chronic_absenteeisim = self.db.read_table(table_name='ChronicAbsenteeisim')
            self.sgp = self.db.read_table(table_name='SGP')
            print(f'SGP file shape: {self.sgp.shape}')
            
        elif self.server_name == 'AACTASTPDDBVM01':
            try:
                self.chronic_absenteeisim = self.db.read_table(table_name='ChronicAbsenteeisim')
            except:
                self.chronic_absenteeisim = self.ca.format_raw_ca()
            try:
                self.sgp = self.db.read_table(table_name='SGP')
                print(f'SGP file shape: {self.sgp.shape}')
            except:
                print('SGP File table not found on 02')
                
        print(f'''
            Enrollment file shape: {self.enrollment.shape}
            school_type file shape: {self.school_type.shape}
            assessments file shape: {self.assessments.shape}
            EL file shape: {self.el.shape}
            Sped Enrollment file shape: {self.sped.shape}
            chronic_absenteeisim file shape: {self.chronic_absenteeisim.shape}
            k2_staticfile shape: {self.k2_sf.shape}''')
        
        
    def snapshot_raw_data(self):
        self.db.take_snapshot()
        
    def produce_raw_growth_file(self, path_to_save=None):
        # get data from assessment and enrollment tables and produce raw growth
        pass
    
    def format_upload_sgp(self, file_path, upload_to_db=True, all_nvarchar=False):
        #read r file
        print('Reading raw R-SGP File')
        result = pyreadr.read_r(file_path)
        d = list(result.keys())[0]
        raw_sgp = result[d]
        #format sgp
        self.sgp = self.growth.format_sgp(raw_sgp)
        # upload SGP
        if upload_to_db:
            table_name = 'SGP'
            self.db.upload_table_to_db(df = self.sgp, table_name=table_name, all_nvarchar=all_nvarchar)
        
# =============================================================================
#     def format_sgp(self):
#         raw_sgp = self.db.read_table(table_name='SGP')
#         self.sgp = self.growth.format_sgp(raw_sgp)
# =============================================================================
        
        
    def drop_raw_tables(self, table_prefix=None):
        raw_tables = ['FiscalYearEnrollment', 'Assessments', 'Azella', 'EdOrg', 'Census', 'DropOut', 'GradRate', 'PersistRate', 'CE', 'OTG', 'TradCCRI', 'AltCCRI']
        self.db.drop_tables_in_run(*raw_tables, table_prefix=table_prefix.capitalize())
        
    def drop_school_type(self, table_prefix=None):
        table_name = 'SchoolType'
        self.db.drop_tables_in_run(table_name, table_prefix=table_prefix.capitalize())
    
    def drop_chronic_absenteeisim(self, table_prefix=None):
        table_name = 'ChronicAbsenteeisim'
        self.db.drop_tables_in_run(table_name, table_prefix=table_prefix.capitalize())
        
    def drop_sgp(self, table_prefix=None):
        table_name = 'SGP'
        self.db.drop_tables_in_run(table_name, table_prefix=table_prefix.capitalize())
        
    def drop_staticfile(self, table_prefix=None):
        table_name = 'StaticFile'
        self.db.drop_tables_in_run(table_name, table_prefix=table_prefix.capitalize())
    
    def upload_staticfile(self):
        table_name = 'StaticFile'
        self.db.upload_table_to_db(df = self.staticfile, table_name=table_name)
    
    def upload_school_type(self):
        table_name = 'SchoolType'
        self.db.upload_table_to_db(df = self.school_type, table_name=table_name)
        
    def format_upload_chronic_absenteeisim(self, upload_to_db=True):
        chronic_absenteeisim = self.ca.format_raw_ca()
        
        ##upload CA to db
        if upload_to_db:
            table_name = 'ChronicAbsenteeisim'
            self.db.upload_table_to_db(df = chronic_absenteeisim, table_name=table_name) 
        ## return result if upload_to_db is false
        else:
            return chronic_absenteeisim
        
        
 
    def format_staticfile(self, keep_all_schools=False):
        ##-------------------------------------------------------------------- merge enrollment to assessments
        ##inner join on saisid and grade from enrollment and saisid and assessed grade from assessment.
        ## this allows to match enrollment record to assessment record in case kid got promoted between testing windows
        ## and also allows us to keep only the kids test record for a certain grade that corrosponds to a certain enrollment grade
        print('Starting staticFile formatting')
        print('Merging in Assessments')
        self.staticfile = pd.DataFrame()
        for subject in self.subject_grade_map.keys():
            sub = self.academic_subjects[subject]
            sub_assessments = self.assessments[self.assessments.Subject==sub]
            sub_enrollment = self.enrollment [self.enrollment.StudentGrade.isin(self.subject_grade_map[subject])]
            sub_static = pd.merge(left=sub_enrollment, right=sub_assessments, left_on=['FiscalYear','SAISID','StudentGrade']
                                   , right_on=['FiscalYear','SAISID','AssessmentGrade'], how='left', suffixes=('','_assessments'))
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
            ## concat the inner joined static file to enrollment records
            self.staticfile = pd.concat([self.staticfile, sub_static])
        #remove cached files
        del sub_static, sub_enrollment, sub_assessments
        
        ## add in 1 record for each student in grades not eligible for testing
        grades_mask = self.enrollment.StudentGrade.isin(self.not_eligible_testing_grades)
        remainder_records = self.enrollment[grades_mask].copy()
        ## deduplicate enrollment records to keep one entry per student per school
        remainder_records.sort_values(['SAISID','SchoolCode','FAY', 'StudentGrade', 'EntryDate', 'AOIFTE']
                           ,ascending=False, inplace=True)
        remainder_records = remainder_records[~remainder_records[['SAISID', 'SchoolCode']].duplicated(keep='first')]
        remainder_records.loc[:,'AssessmentFamily'] = 'NotEligible'
        
        ## Exclude records where kids is in 2 grades in same school by outer merging then taking records not in the staticfile already (vectorized method)
        remainder_records = pd.merge(left=self.staticfile[['SAISID', 'SchoolCode']], right=remainder_records, on=['SAISID', 'SchoolCode'], how='outer', indicator=True)
        remainder_records = remainder_records[remainder_records._merge=='right_only']
        ## concat the constructed static file to enrollment records
        self.staticfile = pd.concat([self.staticfile, remainder_records])
        del remainder_records
        
        ##if the cohort is outside the testing cohort and grade is >9 and assessment family is not tested => noteligible (because they might be in 11thgrade but not in the right cohort)
        mask =(self.staticfile.StudentGrade.between(9,12)) & (~self.staticfile.Cohort.isin([self.act_cohort, self.aspire_cohort])) & (self.staticfile.AssessmentFamily=='NotTested')
        self.staticfile.loc[mask, 'AssessmentFamily'] = 'NotEligible'
        
        ## add passing indicator
        self.staticfile['Passing'] = 0
        self.staticfile.loc[self.staticfile.Performance.isin([3,4]), 'Passing'] = 1
        self.staticfile.loc[self.staticfile['ScaleScore'].isnull(), 'Passing'] = np.nan
        #statewide tested indicator
        self.staticfile['StateWideTested'] = 0
        self.staticfile.loc[self.staticfile.ScaleScore.notnull(), 'StateWideTested'] = 1
        ##---------------------add growth and prior year test results
        try:
            print('Merging in Growth')
            self.sgp.sort_values(['FiscalYear', 'SAISID', 'Subject', 'StudentGrade', 'ScaleScore'], inplace=True, ascending=False)
            self.sgp = self.sgp [~self.sgp.duplicated(['FiscalYear', 'SAISID', 'Subject', 'StudentGrade'])]
            self.staticfile = pd.merge(self.staticfile, self.sgp, on=['FiscalYear', 'SAISID', 'Subject', 'StudentGrade'], how='left', suffixes=('','_sgp'))
        except:
            extra_cols = ['ScaleScore_sgp', 'Performance_sgp']
            self.staticfile[extra_cols] = np.nan
            print(colored('SGP Data was not found', 'red'))
        
        ## --------------------------------------------------------------------bring in EL
        try:
            print('Merging in AZELLA data')
            self.staticfile = pd.merge(self.staticfile, self.el, on=['FiscalYear', 'SAISID'], how='left', suffixes=('','_azella'))
            ##update EL col
            ############## do we want to keep that behavior?
            # student enrollment doesnt show an EL need but their latest EL score was not proficient (old code changes their EL status to 1)
            not_proficient_py_not_el_mask = (self.staticfile.PYELProf.between(1,3))
            self.staticfile.loc [not_proficient_py_not_el_mask, 'EL'] = 1
            ## student enrollment doesnt show an EL need but student scored a result on AZELLA this year
            ##indicating they were recieving EL services
            not_proficient_cy_not_el_mask = (self.staticfile.ELProf.between(1,3))
            self.staticfile.loc [not_proficient_cy_not_el_mask, 'EL'] = 1
            
            ##update ELFAY and ELFEP  based on updated EL
            self.staticfile.loc[self.staticfile.EL==1, 'ELFEP'] = 1
            self.staticfile.loc [self.staticfile.PYELProf==4, 'ELFEP'] = 1
            self.staticfile.loc [self.staticfile.FAY==0, 'ELFAY'] = 0
            ## if kid doesn't have EL need remove EL data (since we don't merge on schoolcode, double enrolled kids will get el data for schools where they don't recieve el services)
            self.staticfile.loc[ self.staticfile.EL != 1, ['ELTested', 'ELProf', 'ELGrowth', 'ELFAY']] = np.nan
        except:
            extra_cols = ['ELFAY', 'ELTested']
            self.staticfile[extra_cols] = np.nan
            print(colored('AZELLA Data was not found', 'red'))
        
        ##-------------------------------------------------------------------- bring in sped inclusion
        print('Merging in SPED inclusion')
        self.staticfile = pd.merge(self.staticfile, self.sped, on=['FiscalYear', 'SAISID', 'SchoolCode'], how='left')
        ##remove Sped inclusion indicator if not OCTOBER 1 sped
        self.staticfile.loc[self.staticfile.SPED!=1, 'SPEDInclusion'] = 0
        
        ##---------------------add chronic abseentisim
        print('Merging in CA')
        ## convert to numeric
        for col in self.chronic_absenteeisim.columns:
            self.chronic_absenteeisim[col] = pd.to_numeric(self.chronic_absenteeisim[col], errors='coerce')
        #merge with static file
        self.staticfile = pd.merge(self.staticfile, self.chronic_absenteeisim, on=['FiscalYear', 'SAISID', 'SchoolCode'], how='left', suffixes=('','_ca'))
        '''do we want to exclude chronic values for those that failed ADMIntegrityAttend?'''
        
        ##---------------------add K2 data
        print('Adding in k2 data')
        self.staticfile = pd.concat([self.staticfile, self.k2_sf], axis=0)
        
        ##---------------------- only keep schools in schooltype file
        if keep_all_schools is True:
            self.staticfile = pd.merge(self.school_type, self.staticfile, how='outer', on=['SchoolCode', 'DistrictCode'], suffixes=('_SchoolType', ''))
        else:
            self.staticfile = pd.merge(self.school_type, self.staticfile, how='inner', on=['SchoolCode', 'DistrictCode'], suffixes=('_SchoolType', ''))
            
        ##---------------------add utility cols
        self.staticfile['TestingWindow'] = 0
        mask = ((self.staticfile.ELAMathWindow==1) & (self.staticfile.Subject.isin(['Math','ELA']))) | ((self.staticfile.SciWindow==1) & (self.staticfile.Subject=='Sci'))
        self.staticfile.loc[mask, 'TestingWindow'] = 1
        
        #---------------------- drop unneeded cols
        drop_cols = [ 'EntryDate', 'ExitDate', 'ScaleScore_sgp', 'Performance_sgp'
                     , '_merge', 'Tested', 'EnrolledCount9-12', 'EnrolledCountk-8'
                     ,'MinGrade', 'MaxGrade', 'Type', 'GradesServed', 'Active'
                     , 'FiscalYear_SchoolType', 'Year1School']
        self.staticfile.drop(drop_cols, axis=1, inplace=True)
        #fill nan values with zeros for these indicators
        fill_na_cols_zeros = ['SPEDInclusion', 'G8Math', 'G3ELA', 'ELFAY', 'ELTested', 'ChronicAbsent', 'SPED', 'ADMIntegrity', 'Migrant', 'Accommodation']
        fill_na_cols_zeros = {col:0 for col in fill_na_cols_zeros}
        self.staticfile.fillna(fill_na_cols_zeros, inplace=True)
        
        #rearrange cols
        self.staticfile = self.staticfile[ [ col for col in self.staticfile.columns if col != 'SnapShotDate' ] + ['SnapShotDate']]
        
        #print staticfile size
        print('StaticFile Shape: ', self.staticfile.shape)
        
        if self.static_folder is not None:
            file_name = 'StaticFile'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(self.staticfile, self.static_folder, file_name)
        