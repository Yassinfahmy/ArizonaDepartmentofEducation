# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 16:06:55 2023

@author: yfahmy
"""
from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np

class SGI(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        self.subject_map = {'675':'ELA'
                            ,'677':'Math'
                            ,'678':'Sci'}
        ## make list of cols that need to be numeric in static files
        self.staticfile_numeric_cols = ['SchoolCode','Performance', 'FAY' , 'StudentGrade'                    
                           ,'ChronicAbsent','SPED', 'Foster', 'IncomeEligibility1and2'
                           ,'Military', 'ELFEP', 'Homeless', 'Cohort', 'RAEL']
        ## make list of cols that need to be numeric in dropout and grad rate
        self.dropout_numeric_cols = ['EntityId', 'FiscalYear', 'NumEnrolled', 'DropoutRate']
        self.grad_rate_numeric_cols = ['EntityId', 'CohortYear', 'GradRateType', 'GradRate', 'NumCohort']
        ## a dictionary to adjust dropout col names
        self.dropout_cols = {'DropoutRate':'P'
                              ,'NumEnrolled':'EnrolledCount'
                              ,'EntityId':'EntityID'}
        self.gradrate_cols = {'GradRate':'P'
                              ,'NumCohort':'CohortCount'
                              ,'EntityId':'EntityID'}
        ## establish a dictionary to map subgroupnames from server to gui
        self.db_subgroups = {'SPED':'SPED'
                             ,'Foster Care':'Foster'
                             ,'Low SES':'ED'
                             ,'Military':'ParentInMilitary'
                             ,'ELL Fep':'ELFEP1-4'
                             ,'Homeless':'Homeless'
                             ,'Hispanic or Latino':'Hispanic'
                             ,'White':'White'
                             ,'Black/African American':'AfricanAmerican'
                             ,'American Indian or Alaska Native':'NativeAmerican'
                             ,'Asian':'Asian'
                             ,'Native Hawaiian or Pacific Islander':'PacificIslander'
                             ,'Multiple Races':'2orMoreRaces'}
        ## define a dict to replace subgroup names to macth sql server
        self.subgroup_name_change = {'SPED':{1:'SPED'}
                                    ,'Foster':{1:'Foster'}
                                    ,'IncomeEligibility1and2':{1:'ED'}
                                    ,'Military':{1:'ParentInMilitary'}
                                    ,'ELFEP':{1:'ELFEP1-4'}
                                    ,'Homeless':{1:'Homeless'}
                                    ,'Ethnicity':{'H':'Hispanic'
                                                 ,'W':'White'
                                                 ,'B':'AfricanAmerican'
                                                 ,'I':'NativeAmerican'
                                                 ,'A':'Asian'
                                                 ,'P':'PacificIslander'
                                                 ,'R':'2orMoreRaces'}}
        ##get weights
        comps = ['SGPI', 'SGGRI', 'SGDRI']
        self.weights = {}
        for model, components in self.models_component_weights.items():
            for indicator, weight in components.items():
                for ind in comps:
                    if ind.lower() in indicator.lower():
                        self.weights[indicator] = weight
        
        
    def calculate_component(self, staticfile, py_staticfile, grad_rate, drop_out, schooltype):
        ###make sure cols from old staticfile are correct
        #====================== Some Data Wrangling ===========================
        try:
            ##Correct name  if wrong in Staticfile
            py_staticfile.rename({'EL_Fep':'ELFEP', 'RA_EL':'RAEL'}, inplace=True, axis=1)
            #code Subject col consistently
            if '677' in py_staticfile.Subject.astype(str).unique():
                py_staticfile.Subject = py_staticfile.Subject.astype(str).map(self.subject_map)
        except:
            pass
        
        #convert col data to numeric
        staticfile = self.columns_to_numeric(staticfile, self.staticfile_numeric_cols)
        py_staticfile = self.columns_to_numeric(py_staticfile, self.staticfile_numeric_cols)
        drop_out = self.columns_to_numeric(drop_out, self.dropout_numeric_cols)
        grad_rate = self.columns_to_numeric(grad_rate, self.grad_rate_numeric_cols)
            
        ## perform computation
        sgi_data = self.calculate_sgi_per_subject(staticfile, py_staticfile)
        gr_data = self.calculate_sg_grad_rate_improvement(grad_rate)
        do_data = self.calculate_sg_dropout_improvement(drop_out)
        
        ## add summary data
        sgi_data, gr_data, do_data = self.add_summary_cols(sgi_data, gr_data, do_data)
        ##add suffices
        data = self.add_suffices(sgi_data, gr_data, do_data)
        ##add model col   
        mask = schooltype.StateModel!='K8'
        all_data = pd.merge(data, schooltype.loc[mask,['SchoolCode', 'Alternative']], left_on='EntityID_All', right_on='SchoolCode', how='inner')
        all_data['Model_All'] = '9-12'
        mask = all_data.Alternative==1
        all_data.loc[mask, 'Model_All'] = 'Alt '+ all_data.loc[mask, 'Model_All']

        #make a FiscalYear col
        all_data['FiscalYear_All'] = self.fiscal_year
        
        #round all numeric data
        all_data = self.round_numeric_cols(all_data)[0]
        
        return all_data
        
    def add_suffices(self, sgi_data, gr_data, do_data):
        ##------------------------------label drilldowns, all, extra and summary
        data_dict = dict(sgi = sgi_data, gr = gr_data, do=do_data)
        names_dict = {}
        for key, df in data_dict.items():
            names_dict[key] = []
            for col in df.columns:
                if 'Count' in col:
                  names_dict[key].append(col+'_Extra')
                elif 'EntityID' in col:
                    names_dict[key].append(col+'_All')
                elif 'SGPI' in col:
                    names_dict[key].append(col+'_Summary')
                elif 'SGGRI' in col:
                    names_dict[key].append(col+'_Summary')
                elif 'SGDRI' in col:
                    names_dict[key].append(col+'_Summary')
                else:
                    names_dict[key].append(col+'_DrillDown')
            df.columns = names_dict[key]
        ##------------------------------ merge data on entity ID
        data = pd.merge(data_dict['sgi'], data_dict['gr'], on='EntityID_All', how='outer')
        data = pd.merge(data, data_dict['do'], on='EntityID_All', how='outer')
        
        return data
    
    def add_summary_cols(self, sgi_data, gr_data, do_data):
        
        ##--------------------------------- makes summary columns for SGI data
        improved_sg = [i for i in sgi_data.columns if 'MI' in i]
        sgi_data['SGPINumOfSGMaintainedOrImproved'] = sgi_data[improved_sg].sum(axis=1, min_count=1)
        sgi_data['SGPINumOfEligibleSG'] = sgi_data[improved_sg].notnull().sum(axis=1)
        sgi_data['SGPIPointsEarned'] = (sgi_data.SGPINumOfSGMaintainedOrImproved / sgi_data.SGPINumOfEligibleSG)*(self.weights['SGPI'])
        sgi_data['SGPI'] = sgi_data['SGPIPointsEarned'].copy()
        
        improved_sg = [i for i in gr_data.columns if 'MI' in i]
        gr_data['SGGRINumOfSGMaintainedOrImproved'] = gr_data[improved_sg].sum(axis=1, min_count=1)
        gr_data['SGGRINumOfEligibleSG'] = gr_data[improved_sg].notnull().sum(axis=1)
        gr_data['SGGRIPointsEarned'] = (gr_data.SGGRINumOfSGMaintainedOrImproved / gr_data.SGGRINumOfEligibleSG)*(self.weights['SGGRI'])
        gr_data['SGGRI'] = gr_data['SGGRIPointsEarned'].copy()
        
        improved_sg = [i for i in do_data.columns if 'MI' in i]
        do_data['SGDRINumOfSGMaintainedOrImproved'] = do_data[improved_sg].sum(axis=1, min_count=1)
        do_data['SGDRINumOfEligibleSG'] = do_data[improved_sg].notnull().sum(axis=1)
        do_data['SGDRIPointsEarned'] = (do_data.SGDRINumOfSGMaintainedOrImproved / do_data.SGDRINumOfEligibleSG)*(self.weights['SGDRI'])
        do_data['SGDRI'] = do_data['SGDRIPointsEarned'].copy()
        
        return sgi_data, gr_data, do_data
    
        
    def calculate_sg_dropout_improvement(self, dropout):
        drop_out = dropout.copy()
            
        ## change col names
        drop_out.rename(self.dropout_cols, axis=1, inplace=True)
            
        
        drop_out['SubGroup'] =  drop_out.Type.map(self.db_subgroups)
        
        ## filter down to relevant data only
        mask = (drop_out.SubGroup.notnull()) & (drop_out.FiscalYear.isin([self.fiscal_year,  self.previous_fiscal_year]))
        do = drop_out [mask].copy()
        
        ## change cohort year values to CY and PY
        do.replace({'FiscalYear':{self.fiscal_year:'CY'
                                  ,self.previous_fiscal_year:'PY'}}, inplace=True)
        
        #pivot to put cy and py side by side
        index = ['EntityID', 'SubGroup'] 
        values = ['P', 'EnrolledCount']
        columns = ['FiscalYear']
        year_wide = pd.pivot(data=do, index=index, columns=columns, values=values).reset_index()
        #resolve multiindex
        year_wide.columns = [i[1]+i[0] for i in year_wide.columns]
        ## fill counts cols with zeros to be able to apply n-count later
        for i in ['CYEnrolledCount', 'PYEnrolledCount']:
            year_wide[i] = year_wide[i].fillna(0)
        
        ###---------------- Identify Subgroups that improved or have a zero dropout rate
        year_wide['MI'] = 0
        improved_mask = (year_wide.CYP.round(2) < year_wide.PYP.round(2)) | (year_wide.CYP.round(0) == 0)
        year_wide.loc[improved_mask, 'MI'] = 1
        
        ##---------------- apply n_count rule
        ineligible_mask = (year_wide.CYEnrolledCount < self.n_count) | (year_wide.PYEnrolledCount < self.n_count)
        cols_to_blanc = ['MI', 'CYP', 'PYP']
        year_wide.loc[ineligible_mask, cols_to_blanc] = np.nan
        
        ## pivot to creat wide data for each entity
        values = ['CYEnrolledCount', 'PYEnrolledCount', 'CYP', 'PYP', 'MI']
        do_data = pd.pivot(year_wide, index='EntityID', columns='SubGroup', values=values).reset_index()
        ## resolve multiindex
        do_data.columns = ['SGDropOutImprov'+i[1]+i[0]  if 'EntityID' not in i else i[1]+i[0] for i in do_data.columns]
        
        return do_data
        
    def calculate_sg_grad_rate_improvement(self, gradrate):
        grad_rate = gradrate.copy()
            
        ## change col names
        grad_rate.rename(self.gradrate_cols, axis=1, inplace=True)
            
        ## establish a subgroup col to hold the desired subgroups
        grad_rate['SubGroup'] =  grad_rate.Type.map(self.db_subgroups)
        
        ## filter down to relevant data only
        mask = (grad_rate.SubGroup.notnull()) & (grad_rate.CohortYear.isin([ self.cy_grad_cohort,  self.cy_grad_cohort-1])) & (grad_rate.GradRateType==4)
        gr = grad_rate [mask].copy()
        
        ## change cohort year values to CY and PY
        gr.replace({'CohortYear':{self.cy_grad_cohort:'CY'
                                  ,self.cy_grad_cohort-1:'PY'}}, inplace=True)
        
        #pivot to put cy and py side by side
        index = ['EntityID', 'SubGroup'] 
        values = ['P', 'CohortCount']
        columns = ['CohortYear']
        cohort_year_wide = pd.pivot(data=gr, index=index, columns=columns, values=values).reset_index()
        #resolve multiindex
        cohort_year_wide.columns = [i[1]+i[0] for i in cohort_year_wide.columns]
        ## fill counts cols with zeros to be able to apply n-count later
        for i in ['CYCohortCount', 'PYCohortCount']:
            cohort_year_wide[i] = cohort_year_wide[i].fillna(0)
        
        ###---------------- Identify Subgroups that improved or have a grad rate over 90%
        cohort_year_wide['MI'] = 0
        improved_mask = ((cohort_year_wide.CYP.round(2) > cohort_year_wide.PYP.round(2)) & (cohort_year_wide.CYP.round(0)!=0)) | (cohort_year_wide.CYP.round(2) > 90)
        cohort_year_wide.loc[improved_mask, 'MI'] = 1
        
        ##---------------- apply n_count rule
        ineligible_mask = (cohort_year_wide.CYCohortCount < self.n_count) | (cohort_year_wide.PYCohortCount < self.n_count)
        cols_to_blanc = ['MI', 'CYP', 'PYP']
        cohort_year_wide.loc[ineligible_mask, cols_to_blanc] = np.nan
        
        ## pivot to creat wide data for each entity
        values = ['CYCohortCount', 'PYCohortCount', 'CYP', 'PYP', 'MI']
        gr_data = pd.pivot(cohort_year_wide, index='EntityID', columns='SubGroup', values=values).reset_index()
        ## resolve multiindex
        gr_data.columns = ['SGGradRateImprov'+i[1]+i[0]  if 'EntityID' not in i else i[1]+i[0] for i in gr_data.columns]
        
        return gr_data
            
    def calculate_sgi_per_subject(self, staticfile, py_staticfile):
        #copy staticfiles to avoid altering original data & (staticfile.Alternative==0)
        sf = staticfile[(staticfile.ADMIntegrity==1) ].copy()
        py_sf = py_staticfile.copy()
        
        ## make sure all eligible cohort students are grade codded correctly
        py_sf.loc[py_sf.StudentGrade == self.act_grade, 'StudentGrade'] = 111
        py_sf.loc[py_sf.Cohort == (self.cy_act_cohort-1), 'StudentGrade'] = self.act_grade
        
        sf.loc[sf.StudentGrade == self.act_grade, 'StudentGrade'] = 111
        sf.loc[(sf.Cohort == self.cy_act_cohort), 'StudentGrade'] = self.act_grade
        
        ##select students applicable to SubgroupImp
        static_files = {}
        
        for file, name in zip([sf, py_sf], ['CY', 'PY']):
            #select applicable grades and subjects
            grades_mask = file.StudentGrade == self.act_grade
            file = file[grades_mask].copy()
            #select applicable subjects
            subjects_mask = file.Subject.isin(self.subgroup_improvement_subjects)
            file = file[subjects_mask].copy()
            # filter to fay kids only with a valid score
            fay_tested_mask = (file.FAY.astype(int) > 0) & (file.Performance.notnull())
            file = file [fay_tested_mask].copy()
            #filter out RAEL 1 and 2 from ELA records
            rael_mask = (file.RAEL.isin([1,2])) & (file.Subject=='ELA')
            file = file [~rael_mask].copy()
            #change values for subgroup info
            file.replace(self.subgroup_name_change, inplace=True)
            #rename col
            file.rename({'SchoolCode':'EntityID'}, axis=1, inplace=True)
            #insert processed staticfile into dict
            static_files[name] = file
        
        ##aggregate staticfile to build calculation structure
        counts = {}
        data = {}
        for key, df in static_files.items():
            counts[key] = pd.DataFrame()
            data[key] = pd.DataFrame()
            for subg in self.subgroups:
                #===================groupby 'EntityID', 'subgroup' to calc total counts for n-count
                grouped = df.groupby(['EntityID', 'Subject', subg])
                total_counts = grouped.agg(FAYTestedCounts = ('SAISID', 'nunique')).reset_index()
                ##only keep values that are of the intended aggregate
                total_counts = total_counts[total_counts[subg]!=0].copy()
                ##rename aggregation col and concat to df
                total_counts.rename({subg:'SubGroup'}, inplace=True, axis=1)
                ##add data to dict
                counts[key] = pd.concat([counts[key], total_counts])
                
                #===================use grouped object (by 'EntityID', 'Subject', 'subgroup') to calculate counts per prof level
                
                ## get number of student that are PP, P, HP
                perf = grouped['Performance'].value_counts().reset_index(name='PerfCnt')
                ##only keep values that are of the intended aggregate
                perf = perf[perf[subg]!=0].copy()
                ##rename aggregation col
                perf.rename({subg:'SubGroup'}, inplace=True, axis=1)
                ##add data to dict
                data[key] = pd.concat([data[key], perf])
                
        ##loop throught data to calculate prof pct for each subject 
        results = {}
        for yr, df in data.items():
            #apply proficiency weights
            df['Weigthts'] =  df.Performance.map(self.proficiency_weights)
            df['weighedCounts'] = df['Weigthts'] * df['PerfCnt']
            # groupby 'EntityID', 'Subject', 'SubGroup' and calculate prof PCT
            grouped = df.groupby(['EntityID', 'Subject', 'SubGroup'])
            perf_pct = grouped.agg(Numerator = ('weighedCounts', 'sum')
                                   ,Denominator = ('PerfCnt', 'sum')).reset_index()
            ## calculate the prof pct per subgroup
            name = yr + 'P'
            perf_pct[name] = (perf_pct.Numerator / perf_pct.Denominator)*100
            #drop num and denom cols
            perf_pct.drop(['Numerator', 'Denominator'], axis=1, inplace=True)
            ## add data to dict
            results[yr] = perf_pct
            
        #merge data of cy and py
        all_results = pd.merge(results['CY'], results['PY'], on=['EntityID', 'Subject', 'SubGroup'], how='left')
        
        #bring in total counts per subgroup and apply n-count rule to CY and PY counts
        n_counts = pd.merge(counts['CY'], counts['PY'], on = ['EntityID', 'Subject', 'SubGroup'], how='left', suffixes=('CY','PY'))
        ## fill in counts with zeros to be able to apply n-count rule later
        n_counts.fillna(0, inplace=True)
        all_results = pd.merge(all_results, n_counts, on=['EntityID', 'Subject', 'SubGroup'], how='outer')
        
        ###---------------- Identify Subgroups that improved
        all_results['MI'] = 0
        improved_mask = (all_results.CYP.round(2) >= all_results.PYP.round(2)) & (all_results.CYP.round(2)!=0)
        all_results.loc[improved_mask, 'MI'] = 1
        
        ##---------------- apply n_count rule
        ineligible_mask = (all_results.FAYTestedCountsCY < self.n_count) | (all_results.FAYTestedCountsPY < self.n_count)
        cols_to_blanc = ['MI', 'CYP', 'PYP']
        all_results.loc[ineligible_mask, cols_to_blanc] = np.nan
        ##Remove unknown ethinicity subgroup
        all_results = all_results[all_results.SubGroup!='U'].copy()
        
        #===================================================pivot for wide data
        values=['FAYTestedCountsCY', 'FAYTestedCountsPY', 'CYP', 'PYP', 'MI']
        sgi_data = pd.pivot(all_results, index='EntityID', columns=['Subject', 'SubGroup'], values=values).reset_index()
        #resolve multiindex
        sgi_data.columns = ['SGI'+i[1]+i[2]+i[0] if 'EntityID' not in i else i[1]+i[2]+i[0] for i in sgi_data.columns ]
        
        return sgi_data
        
        
#%% bring in staticfile

# cy= 2023
# run = 'PrelimV4'
# self = SGI(2023, run)
# staticfile = DATABASE(fiscal_year = cy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive').read_table(table_name ='StaticFile')

# ##bring in last year's staticfile
# py=2022
# prefix=''
# py_staticfile = DATABASE(fiscal_year = py
#                       ,run = prefix
#                       ,schema = 'dbo'
#                       ,database = 'REDATA').read_table(table_name ='StaticFileData')

# ##read grad data for the eligible cohorts (cy-1 and py-1)
# grad_rate = DATABASE().read_sql_query(sql= '''SELECT * 
#                                               FROM [AccountabilityArchive].[Static].[Prelimv4GradRate2023]
#                                               WHERE CohortYear in (2022, 2021)''')
                                              
# ####read dropout data for the cy and py
# drop_out = DATABASE().read_sql_query(sql= '''SELECT * 
#                                               FROM [AccountabilityArchive].[Static].[Prelimv4DropOut2023]
#                                               WHERE FiscalYear in (2022, 2023)''')
# #%% calculate  component
# sgi = SGI(2022)
# data = sgi.calculate_component(staticfile, py_staticfile, grad_rate, drop_out)

# #%% Upload data
# fy= 2022
# table_name='StateSGI'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').upload_table_to_db(df=data, table_name=table_name)

#%%drop table
# fy= 2022
# table_name='StateSGI'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').drop_tables_in_run(table_name, table_prefix=run)