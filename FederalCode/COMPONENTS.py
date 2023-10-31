# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 15:07:51 2023

@author: yfahmy
"""
from datetime import date
import pandas as pd

class COMPONENTS:
    def __init__(self
                 ,fiscal_year=None
                 ,run='Prelim'
                 ,n_count=20
                 ,el_grade_map = None
                 ,ca_grade_map = None
                 ,growth_grade_map=None
                 ,federal_model_weights=None
                 ,growth_subjects=['Math', 'ELA']
                 ,subgroups=None
                 ,cy_act_grade = 11):
        # define fiscalYear
        if fiscal_year is None:
           self.fiscal_year = date.today().year
        else:
            self.fiscal_year = fiscal_year
        # print('Fiscal year defined as {}'.format(self.fiscal_year))        
        self.previous_fiscal_year = self.fiscal_year-1
        
        #set run type
        self.run = run.capitalize()
        # print(F'Run Type set to----> {self.run}')
        
        ##define server name
        self.server_name = 'AACTASTPDDBVM02'
        
        # define n_count
        self.n_count = n_count
        
        self.cy_act_grade = cy_act_grade
        self.cy_act_cohort = self.fiscal_year+1
        #define growth subjects and subgroup improvment subjects
        self.growth_subjects = growth_subjects
        
        # define growth grades to apply components to
        if growth_grade_map is None:
            self.growth_grade_map = {2:[4,5,6,7,8]
                                     ,5:[4,5,6,7,8,11]
                                     ,4:[4,5,6,7,8,11]}
        else:
            self.growth_grade_map = growth_grade_map
            
        # define CA grades to apply components to
        if ca_grade_map is None:
            self.ca_grade_map = {1:[1,2]
                                ,2:[1,2,3,4,5,6,7,8]
                                ,5:[1,2,3,4,5,6,7,8]
                                ,4:[1,2,3,4,5,6,7,8]}
        else:
            self.ca_grade_map = ca_grade_map
            
        ##grade map for EL
        self.el_grade_map= {1:[-1,1,2]
                            ,2:[-1,1,2,3,4,5,6,7,8]
                            ,3:[9,10,11,12]
                            ,4:[-1,1,2,3,4,5,6,7,8,9,10,11]
                            ,5:[-1,1,2,3,4,5,6,7,8,9,10,11,12]}
        
        ## define model weights for each
        if federal_model_weights is None:
            self.federal_model_weights = {1:{'Proficiency':80
                                             ,'EL':10
                                             ,'CA':10}
                                   
                                           ,2:{'Proficiency':60
                                                ,'EL':10
                                                ,'Growth':20
                                                ,'CA':10}
                                           
                                           ,3:{'Proficiency':60
                                                ,'EL':10
                                                ,'Graduation Rate':20
                                                ,'Dropout Rate':10}
                                           
                                           ,5:{'Proficiency':60
                                                ,'Growth':15
                                                ,'EL':10
                                                ,'Graduation Rate':5
                                                ,'Dropout Rate':5
                                                ,'CA':5}
                                           
                                           ,4:{'Proficiency':60
                                                ,'Growth':20
                                                ,'EL':10
                                                ,'Dropout Rate':5
                                                ,'CA':5}
                                           }
        else:
            self.federal_model_weights = federal_model_weights
            
        ##define a map for SchooltypeF
        self.federal_school_type_map = {1:'k-2'
                                        ,2:'k-8'
                                        ,3:'9-12'
                                        ,4:'k-11'
                                        ,5:'k-12'}
            
        # Define subgroups
        if subgroups is None:
            self.subgroups = ['Ethnicity', 'SPED', 'IncomeEligibility1and2', 'ELFEP']
        else:
            self.subgroups = subgroups
            
    
            
        #define aggregation names for ATSI
        self.atsi_subgroups_name_changes = {'SPED':{1:'SWD'}
                                           ,'IncomeEligibility1and2':{1:'IE12'}
                                           ,'ELFEP':{1:'ELFEP14'}
                                           ,'Ethnicity':{'H':'HispanicLatino'
                                                        ,'W':'White'
                                                        ,'B':'AfricanAmerican'
                                                        ,'I':'NativeAmerican'
                                                        ,'A':'Asian'
                                                        ,'P':'PacificIslander'
                                                        ,'R':'TwoorMoreRaces'}}
        
        self.proficiency_grade_map = {1:[101]
                                     ,2: [3,4,5,6,7,8]
                                     ,3: [11]
                                     ,4:[3,4,5,6,7,8,11]
                                     ,5:[3,4,5,6,7,8,11]}
        ##--------------------- Chronic Absenteeisim module variables
        self.py_ca_dd_tables_by_model = {4:'ChronicAbsenteeismKThru11'
                                      ,2:'ChronicAbsenteeismKThru8'
                                      ,5:'ChronicAbsenteeismKThru12'}
        
        self.py_prof_dd_tables_by_model = {1:'ProficientK2'
                                      ,2:'ProficientKThru8'
                                      ,3:'Proficient9Thru12'
                                      ,4:'ProficientKThru11'
                                      ,5:'ProficientKThru12'}
        
        
        ###-------------------- All modules Variables (CA)
        ## to pull in historic data
        self.py_summary_tables_by_model = {4:'SummaryKThru11'
                                        ,2:'SummaryKThru8'
                                        ,5:'SummaryKThru12'}
        self.py_atsi_tables = {'atsi_1':'SummaryProfATSI'
                            ,'atsi_2':'GrowthCAGRDRATSI'}
        ##---------------------- grad rate tables
        self.py_gradrate_tables = {5:'GraduationRateKThru12'
                                   ,3:'GradRate9Thru12'}
        ##---------------------- dropout rates table
        self.py_dropout_tables = {5:'DropoutRateKThru12'
                                  ,4:'DropoutRateKThru11'
                                  ,3:'DropoutRate9Thru12'}
        ##------------------- define a list of subgroup names in database for graduation and dropout calculations
        ## keys crrospond to names in db, values corrospond to names in gui tables
        self.db_subgroups = {'Black/African American':'AfricanAmerican'
                             ,'Asian':'Asian'
                             ,'Hispanic or Latino':'HispanicLatino'
                             ,'American Indian or Alaska Native':'NativeAmerican'
                             ,'Native Hawaiian or Pacific Islander':'PacificIslander'
                             ,'Multiple Races':'TwoorMoreRaces'
                             ,'White':'White'
                             ,'ELL Fep':'ELFEP14'
                             ,'SPED':'SWD'
                             ,'Low SES':'IE12'
                             ,'All':'All'}
        
        
    
    def calculate_component(self):
        pass
    
    def columns_to_numeric(self, df, columns):
        ## confirm numeric cols are numeric
        t = ['float', 'int']
        for i in columns:
            if t[0] in str(df.dtypes[i]):
                pass
            elif t[1] in str(df.dtypes[i]):
                pass
            else:
                df[i] = df[i].astype(str).str.replace('%','')
                df[i] = pd.to_numeric(df[i], errors='coerce')

        return df
    
    def round_numeric_cols(self, *df, sigfigs=2):
        '''
        

        Parameters
        ----------
        *df : this variable can be a single DF or a list of DFs that you unpack in the call
            Example Code:
                data = [df1, df2, df3]
                df1, df2, df3 = self.round_numeric_cols(*data)
        
        Returns
        -------
        A list containg rounded input dfs or each individual df depending on # of variables the output is assigned to

        '''
        ##round all numeric cols
        for i in df:
            i[i.select_dtypes(include='number').columns] = i.select_dtypes(include='number').round(sigfigs).copy()
        return df
    

    # converts any columns that have all integer values to integer dtype
    def count_cols_to_integer(self, *dfs):
        for df in dfs:
            for col in df.columns[(((df % 1)==0) | df.isna()).all(0)]:
                df[col] = df[col].astype("Int64")

        