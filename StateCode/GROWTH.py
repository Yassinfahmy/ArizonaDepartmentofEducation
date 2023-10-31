# -*- coding: utf-8 -*-
"""
Created on Thu Mar  2 09:46:46 2023

@author: yfahmy
"""

from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np


class GROWTH(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        ## define a list of cols that must be numeric for this module to succeed
        self.numeric_cols = ['FAY', 'SGP_CCR_Category', 'StudentGrade', 'EntityID', 'SAISID', 'Cohort', 'PYPerformance']
        ## convert subject to str in case we use old staticfile naming conventions
        self.subject_map = {'675':'ELA'
                            ,'677':'Math'
                            ,'678':'Sci'}
        ## rename cols in staticfile in case we use the old staticfile naming conventions
        self.col_rename={'PY_Performance':'PYPerformance'
                         ,'SchoolCode':'EntityID'}
        ## define a dict of str values for self.growth_categorys for sql server naming convention
        self.summmary_growth_category = {0:'LowGrowth'
                                       ,1:'AverageGrowth'
                                       ,2:'HighGrowth'}
        
        self.drilldown_growth_category={0:'LowGrowth'
                                       ,1:'AvgGrowth'
                                       ,2:'HighGrowth'}
        ## define a dict of str values for performance level
        self.prior_perf_map = {1:'MP'
                              ,2:'PP'
                              ,3:'P'
                              ,4:'HP'}
        #get component weights from super class
        self.growth_half_weights = {}
        for model, components in self.models_component_weights.items():
            for component, weight in components.items():
                if 'Growth'.lower() == component.lower():
                    self.growth_half_weights[model] = weight/(100*2)
        
    def calculate_component(self, staticfile_raw):
        ##------------------------------------------ Wrangling
        staticfile = staticfile_raw[staticfile_raw.ADMIntegrity==1].copy()
        ##rename cols in case we have to run old staticfile
        staticfile.rename(self.col_rename, axis=1, inplace=True)
        ##make sure numeric cols are numeric
        staticfile = self.columns_to_numeric(staticfile, self.numeric_cols)
        if '677' in staticfile.Subject.astype(str).unique():
            staticfile.Subject = staticfile.Subject.astype(str).map(self.subject_map)
        ##only include FAY records
        staticfile = staticfile [staticfile.FAY > 0]
        ## only include Math and ELA records
        staticfile = staticfile [staticfile.Subject.isin(self.growth_subjects)]
        ## only include students with SGP_CCR_Category value
        staticfile = staticfile[staticfile.SGP_CCR_Category.notnull()]
        ## turn all grade 11 to 111 then only turn right cohort into grade 11
        staticfile.loc[staticfile.StudentGrade==self.act_grade, 'StudentGrade']=111
        staticfile.loc[staticfile.Cohort==self.cy_act_cohort, 'StudentGrade']=self.act_grade
        ## select relevant grades for Growth according to buissness rules
        sf = pd.DataFrame()
        for model in self.growth_half_weights.keys():
            grades_list = self.growth_grade_map[model]
            sf_temp = staticfile [staticfile.StudentGrade.isin(grades_list)].copy()
            sf_temp['Model'] = model
            sf = pd.concat([sf, sf_temp], axis=0)
        #clean up cached variables
        del sf_temp
        
        ##---------------------------------- covert numeric to str values to match SQL col names
        ## establish needed cols for calculation and sql naming convention for summary tables
        sf['prior_perf'] = sf.PYPerformance.map(self.prior_perf_map)
        
        ##----------------------------------- get results
        sf['sgp_category'] = sf.SGP_CCR_Category.map(self.summmary_growth_category)
        summary = self.get_summary(sf)
        
        sf['sgp_category'] = sf.SGP_CCR_Category.map(self.drilldown_growth_category)
        drilldown = self.get_drilldown(sf)
        
        #================================================== Add suffices
        ##add '_DrillDown' to drilldown in order to identify drill down sql tables cols
        drilldown.columns = [name + '_DrillDown' for name in drilldown] 
        ##add '_Summary' to summary inorder to identify summary cols
        summary.columns = [name + '_Summary' for name in summary] 
        
        #============================== merge summary to drilldown data
        data = pd.concat([summary, drilldown], axis=1).reset_index()
        #============================== round all cols
        data = self.round_numeric_cols(data)[0]
        
        #change EntityID to entityID_All
        name_map = {'EntityID':'EntityID_All'
                     ,'Model':'Model_All'
                     ,'FAYSGPStudentCount_Summary':'FAYSGPStudentCount_Extra'}
        data.rename(name_map, axis=1, inplace=True)
        ##make a FiscalYear col
        data['FiscalYear_All'] = self.fiscal_year
        
        return data
        
    def get_summary(self, sf):
        sf = sf.copy()
        ## calculate number of students eligible for growth to apply n-count
        cols_to_grp = ['EntityID', 'Model']
        student_counts = sf.groupby(cols_to_grp).agg(FAYSGPStudentCount=('SAISID', 'nunique'))
        ##---------------------------------- Aggregate data by School and model

        sf['GrowthBands'] = 'PY' + sf.prior_perf + 'CY' + sf.sgp_category
        grouped = sf.groupby(['Model', 'EntityID', 'Subject'])
        gr_bands = (grouped['GrowthBands'].value_counts(normalize=True)*100).reset_index(name='Pct')
        
        ##----- apply BandWeights to calculate points per subject
        gr_bands['BandWeights'] = gr_bands.GrowthBands.map(self.growth_band_weights)
        gr_bands['WeighedBands'] = gr_bands.BandWeights * gr_bands.Pct
        
        #==========================produce total points per subject and for both Math and ELA
        ##group by ['EntityID', 'Model', 'Subject'] to sum points per subject per school per model
        index = ['EntityID', 'Model', 'Subject']
        raw_points = gr_bands.groupby(index).agg(BandsSum=('WeighedBands','sum')).reset_index()
        raw_points['Weights'] = raw_points['Model'].map(self.growth_half_weights)
        #calc point for each model
        raw_points['Points'] = raw_points.BandsSum * raw_points.Weights
        
        ##set maximum points in case its higher than allowed by weights
        subject_max_points = {model: weight*100 for model,weight in self.growth_half_weights.items()}
        raw_points['MaxPoints'] = raw_points['Model'].map(subject_max_points)
        mask = raw_points.Points > raw_points.MaxPoints
        raw_points.loc[mask, 'Points'] = raw_points.loc[mask, 'MaxPoints']
        ##pivot table to produce points
        points = pd.pivot(raw_points, index=['EntityID', 'Model'], columns='Subject', values='Points')
        
        # rename cols to match sql server
        ## fill empty points with zeros for additions
        points.loc[:,self.growth_subjects] = points.loc[:,self.growth_subjects].fillna(0)
        #rename cols
        points.columns = 'SGP' + points.columns + 'Points'
        ##calc total points
        points['TotalGrowthPoints'] = points[['SGPELAPoints', 'SGPMathPoints']].sum(axis=1, min_count=1)
        #======================================================================
        
        #======================= Produce breakdown of weighed growth bands per subject
        index=['EntityID', 'Model']
        cols = ['Subject','GrowthBands']
        distribution = pd.pivot(gr_bands, index=index, values='Pct', columns=cols)
        #resolve col names multi-index
        distribution.columns = [i[0]+i[1] for i in distribution.columns]
        ## fill empty data with zeros
        distribution = distribution.fillna(0)
        #======================================================================
        
        ##combine points and Growth bands distribution in one df
        summary_data = pd.concat([student_counts, points, distribution], axis=1)
        ## for schools that don't meet the n-count remove points
        points_col = ['SGPELAPoints', 'SGPMathPoints', 'TotalGrowthPoints']
        summary_data.loc [summary_data.FAYSGPStudentCount<self.n_count, points_col] = np.nan
        #make final total col
        summary_data['Growth'] = summary_data.TotalGrowthPoints.copy()
        
        return summary_data
    
    def get_drilldown(self, sf):
        
        sf = sf.copy()
        ##convert studentgrade to str
        sf.StudentGrade = sf.StudentGrade.astype(int).astype(str)
        ## make a column that makes unique growth bands for students based on current year growth and prior year performance
        sf['GrowthBands'] = sf.StudentGrade+'CY' + sf.sgp_category + 'PY' + sf.prior_perf
        grouped = sf.groupby(['EntityID', 'Model', 'Subject'])
        gr_bands = (grouped['GrowthBands'].value_counts(normalize=True)*100).reset_index(name='Pct')
        
        ## pivot table to produce drill down data
        index=['EntityID', 'Model']
        cols=['Subject', 'GrowthBands']
        drilldown = pd.pivot(data=gr_bands, index=index, columns=cols, values='Pct')
        ##resolve multi-index col names
        drilldown.columns = [i[0] + str(i[1]) for i in drilldown.columns]
        drilldown.fillna(0, inplace=True)
        
        return drilldown
        
#%% bring in the static file
# fy= 2022
# run = 'Final'
# staticfile = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive').read_table(table_name ='StaticFile')
# # #%% calculate growth component

# growth_grade_map = {'k-8': [4,5,6,7,8]
#                          ,'9-12':[11]}
# models_component_weights = {'k-8':{'Growth':50
#                                          ,'ELProficiencyandGrowth':10
#                                          ,'AccelerationReadiness':10
#                                          ,'Proficiency':30}  
                                  
#                                   ,'9-12':{'ELProficiencyandGrowth':10
#                                            ,'GraduationRate':10
#                                            ,'GradRateImprovement':10
#                                            ,'CollegeandCareerReady_SRSS':20
#                                            ,'Proficiency':30
#                                            ,'Growth':20}
                                  
#                                   ,'Alt 9-12':{'Proficiency':15
#                                                ,'ELProficiencyandGrowth':10
#                                                ,'CollegeandCareerReady_SRSS':35
#                                                ,'GraduationRate':10
#                                                ,'AcademicPersistence':10
#                                                ,'CreditsEarned':10
#                                                ,'OnTracktoGraduate':10}
#                                   }
# self = GROWTH(fy, run, growth_grade_map=growth_grade_map, models_component_weights=models_component_weights)
# gr_data = self.calculate_component(staticfile)


# fy= 2022
# table_name='StateGrowth'
# run = 'G11only'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').upload_table_to_db(df=gr_data, table_name=table_name)
# #%%drop table

# fy= 2022
# table_name='StateGrowth'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').drop_tables_in_run(table_name, table_prefix=run)
        