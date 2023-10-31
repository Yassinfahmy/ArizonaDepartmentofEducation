# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 10:52:15 2023

@author: YFahmy
"""

import pandas as pd
import numpy as np
from GROWTH import GROWTH
from proficiency import PROFICIENCY
from SGI import SGI
from GRADUATION import GRADUATION
from EL import EL
from DATABASE import DATABASE
from CCRI import CCRI
from AR import AR
from COMPONENTS import COMPONENTS
from GTG import GTG
from bonus_points import Bonus_Points
from functools import reduce, partial
            
class ADEConnect(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', null_components=None, **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        self.db_schema = '[REDATA_UAT].[grading].'
        self.model_table = {'k-8':'KThru8'
                            ,'9-12':'9Thru12'
                            ,'Alt 9-12':'9Thru12'
                            ,'Non-Typical':'NTSCLG'}
        
        #setup dictionary to hold components for the current year
        indicators = {'AcademicPersistence':'StateGTG'
                      ,'CreditsEarned':'StateGTG'
                      ,'OnTracktoGraduate':'StateGTG'
                      ,'Growth':'StateGrowth'
                      ,'ELProficiencyandGrowth':'StateEL'
                      ,'AccelerationReadiness':'StateAR'
                      ,'Proficiency':'StateProficiency'
                      ,'GraduationRate':'StateGradRate'
                      ,'GradRateImprovement':'StateGradRate'
                      ,'CollegeandCareerReady_SRSS':'StateCCRI'
                      ,'SGPI':'StateSGI'
                      ,'SGGRI':'StateSGI'
                      ,'SGDRI':'StateSGI'}
        #make a dictionary to hold which components to calc or retrieve
        self.calculations = {}
        for model_name, components in self.models_component_weights.items():
            for component in components.keys():
                if indicators[component] not in self.calculations:
                    self.calculations[indicators[component]] = True
        ##add bonus points calculations
        self.calculations['StateBonusPoints'] = True
        #if there is calculations to avoid then remove from dict
        if null_components is not None:
            for i in null_components:
                del self.calculations[i]
                
        ## define entity table name
        self.entity_table = f'[REDATA_UAT].[grading].[AThruFEntityData{self.fiscal_year}]'
                
        ## make instances of accessor classes
        self.growth    = GROWTH(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.prof    = PROFICIENCY(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.sgi    = SGI(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.grad    = GRADUATION(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.el    = EL(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.ccri    = CCRI(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.ar    = AR(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.proficiency = PROFICIENCY(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.gtg = GTG(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        self.bp = Bonus_Points(fiscal_year=self.fiscal_year, run=self.run, **kwargs)
        #make a dict to hold all accessor classes with target table name
        self.results_modules = {'StateGrowth':self.growth
                                  ,'StateProficiency':self.proficiency
                                  ,'StateSGI':self.sgi
                                  ,'StateGradRate':self.grad
                                  ,'StateEL':self.el
                                  ,'StateCCRI':self.ccri
                                  ,'StateAR':self.ar
                                  ,'StateGTG':self.gtg
                                  ,'StateBonusPoints':self.bp}
        ## make a dict to hold cut score
        self.cuts = {'Alt 9-12': {'A': [130, 83],
                                  'B': [82.99, 65],
                                  'C': [64.99, 47],
                                  'D': [46.99, 29],
                                  'F': [28.99, 0]},
                     '9-12': {'A': [130, 82],
                              'B': [81.99, 65],
                              'C': [64.99, 48],
                              'D': [47.99, 31],
                              'F': [30.99, 0]},
                     'k-8': {'A': [130, 84],
                            'B': [83.99, 72],
                            'C': [71.99, 60],
                            'D': [59.99, 47],
                            'F': [46.99, 0]}}
        ## remove schools that don't qualify based on points eligible cutoff    
        self.threshold = {'Alt 9-12':60
                          ,'9-12':50
                          ,'k-8':80}

        
    def get_cy_staticfile(self, database = 'AccountabilityArchive', schema = 'Static', prefix=None, table_name ='StaticFile'):
        print('Retrieving cy_staticfile')
        # if a specific prefix is not provided revert to the self.run
        if prefix is None:
            prefix = self.run
        # bring in staticfile
        staticfile = DATABASE(fiscal_year = self.fiscal_year
                                    ,run = prefix
                                    ,schema = schema
                                    ,database = database).read_table(table_name =table_name)
        #filter out tuittion payer code 2 , jteds and private schools 
        mask = (staticfile.TuitionPayerCode==2) | (staticfile.JTED==1) | (staticfile.Private==1)
        staticfile = staticfile[~mask].copy()
        
        return staticfile
        
    def get_schooltype_file(self, database = 'AccountabilityArchive', schema = 'Static', prefix=None, table_name ='SchoolType'):
        print('Retrieving SchoolType')
        # if a specific prefix is not provided revert to the self.run
        if prefix is None:
            prefix = self.run
        # bring in schooltype
        schooltype = DATABASE(fiscal_year = self.fiscal_year
                            ,run = prefix
                            ,schema = schema
                            ,database = database).read_table(table_name =table_name)
        # #filter out jteds and private schools 
        # mask = (schooltype.JTED==1) | (schooltype.Private==1)
        # schooltype = schooltype[~mask].copy()
        return schooltype
    
    #will need to be updated in 2024 to get data from AccountabilityArchive
    def get_py_staticfile(self, database = 'REDATA', schema = 'dbo', prefix='', table_name='StaticFileData'):
        print('Retrieving py_staticfile')
        # bring in py staticfile
        py_staticfile = DATABASE(fiscal_year = self.previous_fiscal_year
                                ,run = prefix
                                ,schema = schema
                                ,database = database).read_table(table_name =table_name)
        return py_staticfile  

    def get_grad_rate(self, database = 'AccountabilityArchive', schema = 'Static', prefix=None, table_name ='GradRate'):
        print('Retrieving grad_rate')
        # if a specific prefix is not provided revert to the self.run
        if prefix is None:
            prefix = self.run
        # bring in staticfile
        grad_rate = DATABASE(fiscal_year = self.fiscal_year
                            ,run = prefix
                            ,schema = schema
                            ,database = database).read_table(table_name =table_name)
        return grad_rate
    
    def get_dropout_rate(self, database = 'AccountabilityArchive', schema = 'Static', prefix=None, table_name ='DropOut'):
        print('Retrieving drop_out')
        # if a specific prefix is not provided revert to the self.run
        if prefix is None:
            prefix = self.run
        # bring in staticfile
        drop_out = DATABASE(fiscal_year = self.fiscal_year
                            ,run = prefix
                            ,schema = schema
                            ,database = database).read_table(table_name =table_name)
        return drop_out
    
    def get_ccri(self, database = 'AccountabilityArchive', schema = 'Static', prefix=None):
        print('Retrieving CCRI')
        # if a specific prefix is not provided revert to the self.run
        if prefix is None:
            prefix = self.run
        # bring in data
        db = DATABASE(fiscal_year = self.fiscal_year
                              ,run = prefix
                              ,schema = schema
                              ,database = database)
        trad_table_name = 'TradCCRI'
        trad_ccri = db.read_table(table_name =trad_table_name, cy_data_only=True, suffix_fy=True)
        alt_table_name = 'AltCCRI'
        alt_ccri = db.read_table(table_name =alt_table_name, cy_data_only=True, suffix_fy=True)
        return trad_ccri, alt_ccri
        
    def calculate_results (self):
        
        staticfile = self.get_cy_staticfile()
        py_staticfile = self.get_py_staticfile()
        drop_out = self.get_dropout_rate()
        grad_rate = self.get_grad_rate()
        schooltype = self.get_schooltype_file()
        trad_ccri, alt_ccri = self.get_ccri()
        
        #make a dict with each modules argument in order to be used to call modules systemically
        dependancies_dict = {'StateGrowth':[staticfile]
                            ,'StateEL':[staticfile, schooltype]
                            ,'StateProficiency':[staticfile]
                            ,'StateAR':[staticfile, py_staticfile]
                            ,'StateGradRate': [grad_rate, schooltype]
                            ,'StateCCRI': [trad_ccri, alt_ccri, schooltype]
                            ,'StateSGI': [staticfile, py_staticfile, grad_rate, drop_out, schooltype]
                            ,'StateGTG': [staticfile]
                            ,'StateBonusPoints':[staticfile]}
        #fill results dictionary with calculated results
        self.results = {}
        for component in self.calculations.keys():
            print(f'Calculating {component}')
            temp = self.results_modules[component].calculate_component(*dependancies_dict[component])
            temp = self.round_numeric_cols(temp)[0]
            self.results[component] = temp
            print(f'{component} shape: {self.results[component].shape}\n')
        
    def upload_results(self):
        #check to see if self.results is there (if not run the calculations)
        if not hasattr(self, 'results'):
            self.calculate_results()
        #make instance of DB module
        db = DATABASE(fiscal_year = self.fiscal_year
                    ,run = self.run
                    ,schema = 'Results'
                    ,database = 'AccountabilityArchive')
        #upload results
        for name, table in self.results.items():
            db.upload_table_to_db(df=table, table_name=name)
        
    def retrieve_results(self):
        ## make instance of DB module
        db =  DATABASE(fiscal_year = self.fiscal_year
                                    ,run =  self.run
                                    ,schema = 'Results'
                                    ,database = 'AccountabilityArchive')
        def try_to_read(table_name):
            try:
                data = db.read_table(table_name = table_name)
            except:
                pass
            return data
        
        #read tables from database into self.results = {}
        self.results = {}
        for component in self.calculations.keys():
            temp = try_to_read(table_name = component)
            temp = self.round_numeric_cols(temp)[0]
            self.results[component] = temp
        
    def drop_results(self):
        ## make instance of DB module
        db =  DATABASE(fiscal_year = self.fiscal_year
                                    ,run = self.run
                                    ,schema = 'Results'
                                    ,database = 'AccountabilityArchive')
        db.drop_tables_in_run(*list(self.calculations.keys()))
    
    def produce_drilldowns (self):
        ##==============================Put all drilldown tables in dict to get them ready to upload to db
        #pick the drill down cols from each of the results tables
        tables_map = {'StateGrowth':'Growth'
                      ,'StateProficiency':'Proficient'
                      ,'StateSGI':'SubGroupImprovement'
                      , 'StateEL':'EL'
                      ,'StateAR':'AccelerationReadiness'}
        #make a dict to hold drilldown tables
        dd_tables = {}
        
        #iterate through table list
        for archive_name, gui_name in tables_map.items():
            if archive_name in self.results:
                #make an empty list to hold DD col names
                dd_cols = []
                cleaned_dd_cols=[]
                #iterate through cols
                for col in self.results[archive_name].columns:
                    if '_all'  in col.lower():
                        dd_cols.append(col)
                        cleaned_dd_cols.append(col[:-len('_all')])
                    elif '_drilldown' in col.lower():
                        dd_cols.append(col)
                        cleaned_dd_cols.append(col[:-len('_drilldown')])
                    elif 'model_extra' in col.lower():
                        dd_cols.append(col)
                        cleaned_dd_cols.append(col[:-len('_extra')])
                    # elif 'model_all' in col.lower():
                    #     dd_cols.append(col)
                    #     cleaned_dd_cols.append(col[:-len('_all')])
                ##fill in the dd_tables dict with tables
                dd_tables[gui_name] = self.results[archive_name][dd_cols].copy()
                #change col names to remove suffices
                dd_tables[gui_name].columns = cleaned_dd_cols
            
        ##========================= 
        ##This section checks the model types in the results and generates table names with associated DFs in a dict
        drilldowns = {}
        for table_name, df in dd_tables.items():
            for model_name, model_synonym in self.model_table.items():
                if model_name.lower() in df.Model.str.lower().unique():
                    # if the model is alternative add alt to db table name
                    if 'Alt'.lower() in model_name.lower():
                        alt = 'ALT'
                    else:
                        alt=''
                    ## build db table name
                    db_table_name = self.db_schema + f'[{alt}{table_name}{model_synonym}{self.fiscal_year}]'
                    drilldowns[db_table_name] = df [df.Model==model_name].copy()
        return drilldowns
    
    def fill_drilldowns (self, drilldowns=None):
        if drilldowns is None:
            drilldowns = self.produce_drilldowns()
            
        ## make instance of DB module
        db =  DATABASE(fiscal_year = self.fiscal_year)
        #start filling in tables
        for gui_table, df in drilldowns.items():
            db.fill_table(dataframe=df, table_name=gui_table, all_data=True)
            
    
    def produce_summaries(self, produce_grades=False):
        ##==============================Put all summary tables in dict to get them ready to upload to db
        summary_only_modules = ['StateBonusPoints', 'StateCCRI', 'StateGradRate', 'StateGTG']
        summary_regex = ['_all', '_summary']
        #make a dict to hold tables
        summary_tables = {}
        #iterate through table list
        for archive_name, df in self.results.items():
            #make an empty list to hold summary col names
            summary_cols = []
            cleaned_summary_cols=[]
            ##if archive_name same in summary only modules then append all col names (these modules don't have drilldowns)
            if archive_name in summary_only_modules:
                summary_cols = self.results[archive_name].columns.to_list()
                cleaned_summary_cols = summary_cols
            else:
                for col in self.results[archive_name].columns:
                    for reg in summary_regex:
                        if reg.lower() in col.lower():
                            summary_cols.append(col)
                            cleaned_summary_cols.append(col[:-len(reg)])
                            break
            ##fill in the summary_tables dict with df
            summary_tables[archive_name] = self.results[archive_name][summary_cols].copy()
            #change col names to remove suffices
            summary_tables[archive_name].columns = cleaned_summary_cols
            
        ##=========================
        #make a wrapper around the merge function to fix its arguments
        fixed_merge = partial(pd.merge, on=['FiscalYear', 'EntityID', 'Model'], how='outer')
        #use reduce to merge all df in summary_tables dict
        data = reduce(fixed_merge, summary_tables.values())
        
        ## get schooltype file
        schooltype = self.get_schooltype_file()
        
        ## add option to calculate total points
        data = self.calculate_total_points(data, schooltype, produce_grades)
        
        #produce the hybrid summary table data
        data = self.produce_nontypical_summary(data, schooltype, produce_grades)
        #fill entity table from the data
        self.fill_entity_table(data, schooltype, produce_grades)
        
        ##This section generates a dict that holds the gui table names and dfs associated [summaries]
        summaries = {}
        for model_name, model_synonym in self.model_table.items():
            ## build GUI_table_name
            # if the model is alternative add alt to GUI_table_name
            if 'alt' in model_name.lower():
                alt = 'ALT'
            else:
                alt=''
            if 'Non-Typical'.lower() in model_name.lower():
                summary = 'AThruFEntityData_'
            else:
                summary = 'Summary'
            ## build GUI_table_name
            gui_table_name = self.db_schema + f'[{alt}{summary}{model_synonym}{self.fiscal_year}]'
            ##select out the right model
            model_data = data[data.Model.str.lower() == model_name.lower()].copy()
            ##add data to dict with key as table name and df as value
            summaries[gui_table_name] = model_data
        return summaries
    
    def produce_nontypical_summary(self, data, schooltype, produce_grades):
      '''[FiscalYear]
      ,[EntityID]

      ,[PercentOfStudentsK8]
	  ,[TotalPointsK8Model]
      ,[TotalPointsEligibleK8Model]

      ,[PercentOfStudents912]
	  ,[TotalPoints912Model]
      ,[TotalPointsEligible912Model]

      ,[TotalpointsHybridModel]
      ,[TotalpointsEligibleHybridModel]
      ,[PercentageEarned]

      ,[HybridLetterGrade]'''
      ## make a non-typical dataset
      schooltype = schooltype[schooltype.StateModel.str.lower()=='Non-Typical'.lower()].copy()
      nontypical = pd.merge(data, schooltype['SchoolCode'], left_on='EntityID', right_on='SchoolCode', suffixes=('','_y'))
      ## alter names to match gui
      nontypical.rename({'TotalPointSum':'TotalPoints'}, axis=1, inplace=True)
      nontypical['Model'] = nontypical['Model'].astype(str).str.replace('-','', regex=True).str.replace('k','K', regex=True)
      
      ## pivot to create one row per school
      index=['EntityID', 'FiscalYear' ]
      columns = 'Model'
      values = ['TotalPointsEligible', 'TotalPoints', 'TotalBonusPoints']
      nontypical = pd.pivot(data=nontypical, index=index, columns=columns, values=values)
      nontypical.columns = [i[0]+i[1]+'Model' for i in nontypical.columns]
      
      ## get enrolled counts from schooltype
      nontypical = pd.merge(nontypical.reset_index(), schooltype, left_on='EntityID', right_on='SchoolCode', suffixes=('','_y'))
      
      #calculate PercentOfStudents912 and PercentOfStudentsK8
      nontypical['PercentOfStudentsK8'] = (nontypical['EnrolledCountk-8']*100 / nontypical[['EnrolledCountk-8' , 'EnrolledCount9-12']].sum(axis=1, min_count=1)).round(2)
      nontypical['PercentOfStudents912'] = (nontypical['EnrolledCount9-12']*100 / nontypical[['EnrolledCountk-8' , 'EnrolledCount9-12']].sum(axis=1, min_count=1)).round(2)
      
      ## calculate TotalpointsHybridModel, TotalpointsEligibleHybridModel, PercentageEarned
      nontypical['TotalpointsHybridModel'] = (nontypical.TotalPointsK8Model * nontypical.PercentOfStudentsK8/100).add((nontypical.TotalPoints912Model * nontypical.PercentOfStudents912/100), fill_value=0).round(2)
      ## calculate eligible points
      nontypical['TotalpointsEligibleHybridModel'] = (nontypical.TotalPointsEligibleK8Model * nontypical.PercentOfStudentsK8/100).add((nontypical.TotalPointsEligible912Model * nontypical.PercentOfStudents912/100), fill_value=0).round(2)
      ##calculate PCT earned
      nontypical['PercentageEarned'] = (nontypical['TotalpointsHybridModel']*100 / nontypical['TotalpointsEligibleHybridModel']).round(2)
      ##sum bonus points
      nontypical['TotalHybridBonusPoints'] = (nontypical.TotalBonusPointsK8Model * nontypical.PercentOfStudentsK8/100).add((nontypical.TotalBonusPoints912Model * nontypical.PercentOfStudents912/100), fill_value=0).round(2)
      ##calculate total points earned
      nontypical['TotalHybridPointsEarned'] = nontypical[['PercentageEarned', 'TotalHybridBonusPoints']].sum(axis=1, min_count=1).round(2)
      
      ## assign letter grades
      grade_cols = ['LetterGrade', 'HybridLetterGrade']
      if produce_grades:
          ## if Pct earned falls within cuts, then fill grade
          for grade, bounds in self.cuts['9-12'].items():
              #assign grades based on cutscores
              g_mask = nontypical.TotalHybridPointsEarned.between(bounds[1], bounds[0])
              nontypical.loc[g_mask, grade_cols] = grade
      else:
          nontypical[grade_cols] = 'P'
              
      ## assign NR for Schools that don't have minimum eligible points based on 912 model
      nr_mask = (nontypical.TotalpointsEligibleHybridModel < self.threshold['9-12']) | (nontypical.TotalpointsEligibleHybridModel.isnull())
      nontypical.loc[nr_mask, grade_cols] = 'NR'
      
      ### drop schooltype cols
      nontypical.drop(schooltype.columns.to_list(), axis=1, inplace=True)
      
      ##add the nontypicals to data to be uploaded to their summary table
      nontypical['Model'] = 'Non-Typical'
      nontypical['FiscalYear'] = self.fiscal_year
      
      data = pd.concat([data, nontypical], axis=0)
      
      ## when assigning letter grade assign it to HybridLetterGrade and LetterGrade to be compatible with entitytable fill method
      return data
      
    def calculate_total_points(self, data, schooltype, produce_grades):  
        ## calculate totals
        for model in self.models_component_weights.keys():
            #select all cols that are in data
            comp_columns_mask = data.columns.isin(list(self.models_component_weights[model].keys()))
            ## ============================sum eligible points
            #get available col names
            components = data.loc[:,comp_columns_mask].columns
            #make a list of comp weight values for those that are not missing and fill in the weights
            component_weights = []
            model_mask = (data.Model==model)
            for component in components:
                data.loc[model_mask, component+'Weights'] = data.loc[model_mask, component].notnull().apply(lambda x: self.models_component_weights[model][component] if x==True else np.nan)
                component_weights.append(component+'Weights')
            #sum weights
            data.loc[model_mask, 'TotalPointsEligible'] = data.loc[model_mask, component_weights].sum(axis=1, min_count=1)
            ##=============================sum points
            data.loc[model_mask, 'TotalPointSum'] = data.loc[model_mask, components].sum(axis=1, min_count=1).round(2)
            #========================= get percentage earned
            data.loc[model_mask, 'PercentageEarned'] = (data.loc[model_mask, 'TotalPointSum']*100 / data.loc[model_mask, 'TotalPointsEligible']).round(2)
            
            ##======================== sum bonus points
            data.loc[model_mask,'TotalBonusPoints'] = data.loc[model_mask,['TotalBonusPoints', 'CCRIBonusPoint']].sum(axis=1, min_count=1)
            # get total points Earned
            data.loc[model_mask,'TotalPointsEarned'] = data.loc[model_mask,['PercentageEarned', 'TotalBonusPoints']].sum(axis=1, min_count=1).round(2)
            
            if produce_grades:
                ## if Pct earned falls within cuts, then fill grade
                for grade, bounds in self.cuts[model].items():
                    #assign grades based on cutscores
                    g_mask = (model_mask) & (data.TotalPointsEarned.between(bounds[1], bounds[0]))
                    data.loc[g_mask, 'LetterGrade'] = grade
            else:
                data.loc[model_mask, 'LetterGrade'] = 'P'
                
            ## assign NR for Schools that don't have minimum eligible points based on model
            nr_mask = (model_mask) & ((data.TotalPointsEligible < self.threshold[model]) | (data.TotalPointsEligible.isnull()))
            data.loc[nr_mask, 'LetterGrade'] = 'NR'
        ##=================================================================mark ineligible schools as such
        ##merge schooltype 
        data = pd.merge(schooltype[['SchoolCode', 'StateModel']], data, left_on='SchoolCode', right_on='EntityID', how='right')
        ## assign NR for Schools that are marked as ineligible (based on n-count)
        nr_mask = data.StateModel.str.lower()=='InEligible'.lower()
        data.loc[nr_mask, 'LetterGrade'] = 'NR'
        ## if school is non-typical, remove their grades from summary pages
        hybrid_mask = data.StateModel.str.lower()=='Non-Typical'.lower()
        data.loc[hybrid_mask, 'LetterGrade'] = 'P'
        
        return data
        
    def fill_summaries(self, produce_grades=False):
        summaries = self.produce_summaries(produce_grades=produce_grades)
            
        ## make instance of DB mo♦dule
        db =  DATABASE(fiscal_year = self.fiscal_year)
        #start filling in tables
        for gui_table, df in summaries.items():
            db.fill_table(dataframe=df, table_name=gui_table, all_data=True)
            
    def fill_entity_table (self, data, schooltype, produce_grades):
        
        #make a DF to hold entity table data
        ### if LetterGrade col has been produced, use it, else, produce 'P' grades
        cols = ['EntityID', 'Model', 'LetterGrade']
        entity = data[cols].copy()

        ### ======================identify state school types based on schooltype file statemodel
        ## make the grade into grade-alt for alternative schools
        mask = entity['Model'].astype(str).str.contains('Alt', regex=True)
        entity.loc[mask, 'LetterGrade'] =  entity.loc[mask, 'LetterGrade'] + '-Alt'
        
        #make a fields col
        entity.replace({'Model':{'k-8':'K8LetterGrade'
                               ,'9-12':'LetterGrade912'
                               ,'Alt 9-12':'LetterGrade912'
                               ,'Non-Typical':'NonTypicalLetterGrade'}}, inplace=True)
        
        entity = pd.pivot(data=entity, index='EntityID', columns ='Model', values='LetterGrade' ).reset_index()
        
        ##merge schooltype 
        entity = pd.merge(schooltype[['SchoolCode', 'StateModel', 'DistrictCode']], entity, left_on='SchoolCode', right_on='EntityID', how='left')
        
        ##################################      build table
        ##make sure only the correct model shows a value
        entity.loc[entity.StateModel.str.lower()=='k8', 'LetterGrade912'] = None
        entity.loc[entity.StateModel.str.lower()=='912', 'K8LetterGrade'] = None
        entity.loc[entity.StateModel.str.lower()=='Alternative'.lower(), 'K8LetterGrade'] = None
        
        entity['FiscalYear'] = self.fiscal_year
        
        ##============= add district letter grade col
        ## convert Grades to numeric
        grade_map = {'A':4
                     ,'B':3
                     ,'C':2
                     ,'D':1
                     ,'F':0}
        for col in ['K8LetterGrade', 'LetterGrade912', 'NonTypicalLetterGrade']:
            alt_grades = [i+'-Alt' for i in list(grade_map.keys())]
            mask = (entity[col].isin(list(grade_map.keys()))) | (entity[col].isin(alt_grades))
            entity.loc[mask, 'Grade'] = entity.loc[mask, col]
        #remove the '-Alt' from grade suffix
        entity['Grade'] = entity['Grade'].str.replace('-Alt','', regex=True)
        # map grades to GPA
        entity['GPA'] = entity['Grade'].map(grade_map)
        
        ##take average GPA of all schools in a district
        district_grades = entity.groupby('DistrictCode').agg(DistrictGPA=('GPA','mean')
                                                             ,LEASchoolsCount=('SchoolCode','nunique')).round(0).reset_index()
        # convert back to grades
        gpa_map = {value:key for key,value in grade_map.items()}
        district_grades['LEAGrade'] = district_grades.DistrictGPA.map(gpa_map)
        
        ## merge back to entity table
        entity = pd.merge(entity,district_grades, on='DistrictCode', how='left' )
        ##rename DistrictCode to "LEAEntityID'
        entity.rename({'DistrictCode':'LEAEntityID'}, inplace=True, axis=1)
        # if LEA letter grade was null then they are not rated
        if produce_grades:
            entity.loc[entity.LEAGrade.isnull(), 'LEAGrade']= 'NR'
        else:
            entity.loc[entity.LEAGrade.isnull(), 'LEAGrade']= 'P'
                                 
        #========================== Upload data
        ## make instance of DB module
        db =  DATABASE(fiscal_year = self.fiscal_year)
        #start filling in tables
        db.fill_table(dataframe=entity, table_name=self.entity_table, all_data=True)
            
          

# growth_band_weights = {'PYPCYHighGrowth':1
#                               ,'PYPPCYHighGrowth':1
#                               ,'PYMPCYHighGrowth':1
#                               ,'PYMPCYLowGrowth':0
#                               ,'PYPPCYLowGrowth':0
#                               ,'PYPCYLowGrowth':0
#                               ,'PYHPCYLowGrowth':0
#                               ,'PYMPCYAverageGrowth':0.5
#                               ,'PYPPCYAverageGrowth':0.5
#                               ,'PYPCYAverageGrowth':0.5
#                               ,'PYHPCYAverageGrowth':0.5
#                               ,'PYHPCYHighGrowth':1}
# proficiency_weights = {1:0
#                       ,2:0.33
#                     ,3:0.66
#                     ,4:1}
# self = ADEConnect(2023, run='PrelimV6', growth_band_weights=growth_band_weights, proficiency_weights=proficiency_weights)



# null_components=['StateEL']
# self = ADEConnect(2022, null_components=['StateEL'], run='Prelim')

self = ADEConnect(2023, run='PrelimV6')
# self.drop_results()
# self.upload_results()
self.retrieve_results()
self.fill_drilldowns()

self.fill_summaries(produce_grades=True)
