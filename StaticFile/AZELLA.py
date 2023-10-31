# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 09:31:01 2022

@author: yfahmy

"""
import pandas as pd
import os
from SOURCES import SOURCES
from CONNECTION import CONNECTION as con
from termcolor import colored

class AZELLA(SOURCES):

    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)

        #define End Azella window last year
        self.py_end_window = pd.Timestamp(self.previous_fiscal_year, 3, 18)
    
    
    def get_azella_from_database(self):
        print('\nGetting azella data from database')
        #define table to pull from depending on connected server
        if self.server_name == 'AACTASTPDDBVM02':
            table_name = F'AccountabilityArchive.Static.{self.run}Azella{self.fiscal_year}'
        elif self.server_name == 'AACTASTPDDBVM01':
            table_name = '[Accountability].[Legacy].[AZELLAStudentOverallAssessment]'
        else:
            raise ValueError(F'Server {self.server_name} is not a valid Database server. Set "server_name" in class constructor with proper server name')
            
        sql_statment = F'''SELECT 
                              [PublicSAISID]
                              ,[RefProficiencyLevelID]
                              ,[Proficiency]
                              ,[AssessmentDate]
                              ,[Grade]
                              ,[Isplacement]
                              ,[IsReAssessment]
                              ,[EntityID]
                              ,[FiscalYear]
                          FROM {table_name}
                          WHERE FiscalYear IN ({self.fiscal_year}, {self.previous_fiscal_year})'''
        cnxn = con().__call__(server_name = self.server_name)
        azella = pd.read_sql(sql_statment,cnxn)
        cnxn.close()
        
        if self.raw_folder is not None:
            file_name = f'{self.run}Azella'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(azella, self.raw_folder, file_name)
        
        return azella
        
    def format_azella(self, raw_data_azella=None):
        '''
        self.end_window
        self.kg_placement
        self.first_placement
        '''
        
        if raw_data_azella is None:
            
            azella = self.get_azella_from_database()
        else:   
            azella = raw_data_azella

        print(colored('Starting AZELLA data formating', 'green')) 
        ##retrieve some variables
        fiscal_year = self.fiscal_year
        previous_fiscal_year = self.previous_fiscal_year
        end_window = self.end_window 
        py_end_window = self.py_end_window
        kg_placement = self.kg_placement 
        first_placement = self.first_placement
        proficiency_map = self.azella_proficiency_map
        #--------------------------------------------------------------------- some data wrangling
        ### rename columns
        col_ren = {'PublicSAISID':'SAISID'
                   ,'Grade':'StudentGrade'
                   ,'EntityID': 'SchoolCode'}
        azella.rename(col_ren, axis=1, inplace=True)
        
        ##-------------------------------------------------------------------- Recode AZELLA proficiency levels ordinal values
        azella['ProfLevel'] = pd.to_numeric(azella.RefProficiencyLevelID, errors='coerce').map(proficiency_map, na_action='ignore')
        
        ### ------------------------------------------------------------------format grade levels and dates
        azella.replace({'StudentGrade':{'KG':-1}}, inplace=True)
        #convert grades to numeric and only keep numeric values
        azella['StudentGrade'] = pd.to_numeric(azella['StudentGrade'], errors='coerce') 
      
        ## set assessmentdate to datetime type
        azella['AssessmentDate'] = pd.to_datetime(azella['AssessmentDate'], errors='coerce')

        #---------------------------------------------------------------------format PY AZELLA Results from DB 
        ### this chunk keeps the latest assesssment result  and latest assessment date for the previous fiscal year 
        
        azella_py = azella[azella.FiscalYear == previous_fiscal_year].copy()
        ## delete tests taken after end of azella testing window
        eligible_mask = (azella_py.AssessmentDate > py_end_window)
        azella_py['PriorToEndWindow'] = eligible_mask
        
        # order records by SAISID, AssessmentDate and ProfLevel
        azella_py.sort_values(['SAISID', 'PriorToEndWindow', 'AssessmentDate', 'ProfLevel'], inplace=True, ascending=False)
        # remove entries of duplicate 'SAISID' to keep  the latest records before and after the py_end_window if exists
        azella_py = azella_py [~azella_py[['SAISID', 'PriorToEndWindow']].duplicated(keep='first')]
        # pivot table to wide
        azella_py = pd.pivot(azella_py, index='SAISID', columns='PriorToEndWindow', values='ProfLevel').reset_index()
        # True column holds scores taken last year prior to last years' testing window
        ## this is our main PYELPROF but if not present we take the most recent score even if after the testing window last year
        azella_py['PYELProf'] = azella_py[True]
        azella_py.loc[azella_py['PYELProf'].isnull(), 'PYELProf'] = azella_py[False]
        ### keep relevant columns only
        azella_py = azella_py[['SAISID', 'PYELProf']]
        
        #---------------------------------------------------------------------identify CY AZELLA Assessment
        #isolate current year records
        azella_all_cy = azella[azella.FiscalYear == fiscal_year].copy()
        azella_long = azella_all_cy.copy()
        
        #delete azella to free ram
        del azella
        
        #---------------------------------------------------------------------format CY AZELLA Assessment
        ## delete tests taken after end of azella testing window
        not_eligible_mask = (azella_long.AssessmentDate > end_window)
        azella_long = azella_long[~not_eligible_mask].copy()
        try:
            # sort by date to label earliest test and latest tests
            azella_long.sort_values(['SAISID', 'AssessmentDate'], inplace=True, ascending=True)
            azella_long.loc[(~azella_long['SAISID'].duplicated(keep='first')), 'TestType'] = 'earliest_test'
            
            azella_long.sort_values(['SAISID', 'AssessmentDate'], inplace=True, ascending=False)
            azella_long.loc[(~azella_long['SAISID'].duplicated(keep='first')), 'TestType'] = 'latest_test'
        
            ### at this point if student had more than one test we have different labels for them
            ## and if student has only one test then it will be called the lates_test
            # so its safe to say that we can apply the cutoff placement dates to all entries with 'earliest_test' designation
            ## mark tests valid as a first test for growth
            azella_long['ValidPlacement'] = 0
            ## if kid is in kindergarden and took placement test must be prior to kg_placement date
            eligible_kg_first_placement = (azella_long.StudentGrade == -1) & (azella_long.AssessmentDate < kg_placement) & (azella_long.TestType == 'earliest_test')
            azella_long.loc[eligible_kg_first_placement, 'ValidPlacement'] = 1
            ## if kid took placemnt test after first_placement date mark entry
            eligible_first_placement = (azella_long.AssessmentDate <= first_placement) & (azella_long.TestType == 'earliest_test')
            azella_long.loc[eligible_first_placement, 'ValidPlacement'] = 1
            ## remove other tests between earliest and latest test
            azella_long = azella_long [azella_long.TestType.notnull()].copy()
            # remove duplicates (there shouldn't be any)
            azella_long = azella_long[~azella_long[['SAISID','TestType']].duplicated()]
            
            # pivot table to provide data in wide format
            indices = ['FiscalYear','SAISID']
            azella_wide = azella_long.pivot(index=indices, columns=['TestType','ValidPlacement'], values='ProfLevel').reset_index()
            #resolve mult-iindex
            names = [str(i[0])+str(i[1]) for i in azella_wide.columns]
            azella_wide = azella_wide.droplevel(1, axis=1)
            azella_wide.columns = names
            
            #------------------------------------------------------------------------ bring in prior year Azella and format table
            ##ones and zeros after testtype column name signify whether its valid for growth or not
            azella_wide = pd.merge(azella_wide, azella_py, 'outer', 'SAISID')
            
    
            #build ELProf and PYELProf
            #wherever a prior year record doesnt exist, look for placment
            azella_wide.loc [azella_wide.PYELProf.isnull() ,'PYELProf'] = azella_wide.loc [azella_wide.PYELProf.isnull() ,'earliest_test1']
            # El prof is just the latest test
            ##if kid took one test only it will be marked as latest_test0
            azella_wide['ELProf'] = azella_wide['latest_test0']
            #calc growth
            azella_wide['ELGrowth'] = azella_wide['ELProf'] - azella_wide['PYELProf']
            azella_wide['ELTested'] = 1
            azella_wide.loc[(azella_wide.ELGrowth < 0), 'ELGrowth'] = 0
            #----------------------------------------------------------------------------------- make utlity cols
            # azella_wide.loc [azella_wide.StudentGrade.between(-1,8), 'GradeLevel'] = 'K-8'
            # azella_wide.loc [azella_wide.StudentGrade.between(9,12), 'GradeLevel'] = '9-12'
            
            ## make a reclass column
            # azella_wide['ReClass'] = 0
            # azella_wide.loc[(azella_wide.ELFAY==1) & (azella_wide.ELProf == 4), 'ReClass']=1
    
            ### only keep columns of interest
            azella_wide = azella_wide[['FiscalYear', 'SAISID', 'ELTested', 'PYELProf', 'ELProf', 'ELGrowth']]
            ##======> needs update for 2024
            # azella_wide['FiscalYear'] = self.fiscal_year
            
            if self.static_folder is not None:
                file_name = 'elstatic'+ str(fiscal_year)[-2:] + '.csv'
                self.save_data(azella_wide, self.static_folder, file_name)
                
            return azella_wide
        
        except :
            print(colored('AZELLA file formating was not completed, an empty AZELLA table will be returned', 'red'))
            return azella_all_cy[['FiscalYear', 'SAISID']]
            
          
    
# self = AZELLA(2023, 'PrelimV6')