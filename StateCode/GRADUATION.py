# -*- coding: utf-8 -*-
"""
Created on Tue May  2 10:37:30 2023

@author: YFahmy
"""

from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np
from collections import OrderedDict


class GRADUATION(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        ## these cols are checked to be numeric prior to any wrangling
        self.numeric_cols = ['EntityId', 'CohortYear', 'NumCohort', 'NumGraduates', 'GradRateType', 'GradRate']

        ## this is used to filter out to values only and rename cols
        self.renamed_cols = {'GradRate':'GradRatePct'
                            ,'EntityId':'EntityID'
                            ,'NumCohort':'NumCohort'
                            ,'GradRateType':'RateType'
                            ,'CohortYear':'Cohort'}

        ## this is used to construct the points cols to match sql server
        self.points_cols = ['GradRatePonts', 'GraduationRate']
        #retrieve model weight for those that have grad rate
        self.grad_rate_weights = {}
        for model, components in self.models_component_weights.items():
            for component, weight in components.items():
                if 'GraduationRate'.lower() in component.lower():
                    self.grad_rate_weights[model] = weight/100

        ## define variable to hold highschool type values
        self.high_school_type = [3,4]
        #define map of gradrate with associated cohortts
        self.gradrate_type_map = {4:self.cy_grad_cohort
                                ,5:self.cy_grad_cohort-1
                                ,6:self.cy_grad_cohort-2
                                ,7:self.cy_grad_cohort-3}
        
        self.gradrate_improvment_type_map = {self.cy_grad_cohort:4
                                             ,self.cy_grad_cohort-1:4}
        
        self.gradrate_improv_cohort_map = {self.cy_grad_cohort:'CY'
                                            ,self.cy_grad_cohort-1:'PY'}
        
        self.grad_rate_impro_cols = {'PY':'GradRatePYPctYear4'
                                     ,'CY':'GradRateCYPctYear4'}


        
    def calculate_component(self, gradrates, schooltype):
        ##------------------------------------------- Data Wrangling & filtering
        ## make sure numeric cols are numeric
        gradrates = self.columns_to_numeric(gradrates, self.numeric_cols)
        ## filter to the 'all subgroup only
        mask = (gradrates.Type=='All')
        grad_rates = gradrates.loc[mask, list(self.renamed_cols.keys())].copy()
        ## rename cols
        grad_rates.rename(self.renamed_cols, axis=1, inplace=True)
        ## merge with SchoolType
        schooltype = schooltype.query(f'Type in tuple({self.high_school_type})')
        cols = ['SchoolCode','Alternative', 'Type']
        grad_rates = pd.merge(schooltype[cols], grad_rates, right_on=self.renamed_cols['EntityId'], left_on='SchoolCode', how='inner')
        
        ##prepare a dataset to be used for grad rate calculations
        gr = pd.DataFrame()
        for gradratetype, cohort in self.gradrate_type_map.items():
            temp = grad_rates.query(f'RateType=={gradratetype} & Cohort=={cohort}')
            #add data to dataframe
            gr = pd.concat([gr,temp], axis=0)        
            
        ## prepare another dataset to be used for gradrate_improvment
        gr_imp = pd.DataFrame()
        for cohort, gradratetype in self.gradrate_improvment_type_map.items():
            temp = grad_rates.query(f'RateType=={gradratetype} & Cohort=={cohort}')
            #add data to dataframe
            gr_imp = pd.concat([gr_imp,temp], axis=0)
        ##------------------------------------------
        ## get results grad rate
        trad_gr = self.get_trad_gradrates(gr)
        alt_gr = self.get_alt_gradrates(gr)
        #get the data for grad rate imp
        gr_improve = self.get_gradrate_improvement(gr_imp)
        
        ##round to 2 sigfigs
        trad_gr, alt_gr, gr_improve = self.round_numeric_cols(trad_gr, alt_gr, gr_improve)
        
        ##add suffices
        data = self.combine_and_suffix(trad_gr, alt_gr, gr_improve)
        return data
        
    def combine_and_suffix(self, trad_gr, alt_gr, gr_improve):
        ##combine alt and trad regular grad component as col names match
        data = pd.concat([trad_gr, alt_gr], axis=0)
        ##merge in gr_imp
        data = pd.merge(data, gr_improve, on =['Alternative', 'EntityID'], how='outer')
        
        ##make a model and fiscalYear cols
        data['FiscalYear'] = self.fiscal_year
        data['Model'] = data.Alternative.apply(lambda x: 'Alt 9-12' if x==1 else '9-12')
        
# =============================================================================
#         ## add suffices
#         extra = ['Alternative', 'GradRateCYPctYear4', 'Eligible']
#         summary_trad = ['PY', 'Imp']
#         new_names = []
#         #iterate thru all col in df
#         for col in data.columns:
#             added=0
#             #check if they are in extra col list
#             for extra_col in extra:
#                 if extra_col.lower() in col.lower():
#                     new_names.append(col+'_Extra')
#                     added =1
#                     break
#                 
#             #check if they are in summary col list
#             for sum_col in summary_trad:
#                 if sum_col.lower() in col.lower():
#                     new_names.append(col+'_Trad')
#                     added =1
#                     break
#             #check if name has been added already, if so skip to next iteration
#             if added ==1:
#                 continue
#             else:
#                 new_names.append(col+'_All')
#         ##assign names to data
#         data.columns = new_names
# =============================================================================
        return data
        
    def get_gradrate_improvement(self, gr_imp):
        ##if a school has not met ncount we simply remove entry (we don't care because it's not in a drill down)
        #kkep traditional schools only
        mask = gr_imp.Alternative==0
        trad = gr_imp[mask].copy()
        ##appy n-count
        mask = trad[self.renamed_cols['NumCohort']]<self.n_count
        trad = trad[~mask].copy()
        
        ##create wide data
        trad['Year'] = trad.Cohort.map(self.gradrate_improv_cohort_map)#+'PctYear' + trad.RateType.astype(int).astype(str)
        index = ['EntityID', 'Alternative']
        gr_improve = pd.pivot(trad, index=index, columns ='Year', values=self.renamed_cols ['GradRate']).reset_index()
        
        ## assign points 
        points_mask = OrderedDict([(0 , (gr_improve.CY.round(2) < gr_improve.PY.round(2)-2))
                                  ,(self.models_component_weights['9-12']['GradRateImprovement']/2 , (gr_improve.CY.round(2) <= gr_improve.PY.round(2)+2) & (gr_improve.CY.round(2) >= gr_improve.PY.round(2)-2))
                                  ,(self.models_component_weights['9-12']['GradRateImprovement'], (gr_improve.CY.round(2) > gr_improve.PY.round(2)+2) | (gr_improve.CY.round(2) >= 90))
                                ])
        points_cols = ['GradRateImpPoints4Y', 'GradRateImprovement']
        ##iterate thru ordered dict to assign points
        for points, mask in points_mask.items():
            gr_improve.loc[mask, points_cols] = points
            
        ## Rename Pct col
        gr_improve.rename(self.grad_rate_impro_cols, axis=1, inplace=True)
        return gr_improve
        
    def get_alt_gradrates(self, gr):
        ##if a school has not met ncount in all 4 years they will simply not appear in this result table (we don't care because it's not in a drilldown)
        mask = gr.Alternative==1
        alt = gr[mask].copy()
        ##appy n-count (if below n_count remove the whole row)
        mask = alt[self.renamed_cols['NumCohort']]<self.n_count
        alt = alt[~mask].copy()
        
        ##get highest grade rate per school and use it for points
        index=['Alternative', 'EntityID']
        highest = alt.groupby(index).agg(GradRatePct = (self.renamed_cols ['GradRate'], 'max')).reset_index()
        ## calculate points
        highest['GradRatePonts'] = highest[self.renamed_cols ['GradRate']]* self.grad_rate_weights['Alt 9-12']
        
        ## merge back to original table
        index = ['Alternative', 'EntityID', self.renamed_cols ['GradRate']]
        alt =pd.merge(alt, highest, on=index, how='left')
        
        ##sort values by cohort and only keep one value per school prioritizing the latest cohort
        cols = ['Alternative', 'EntityID', 'Cohort', 'GradRatePonts']
        alt.sort_values(cols, inplace=True, ascending=False, na_position='last')
        cols = ['Alternative', 'EntityID', 'GradRatePonts']
        alt.loc[alt.duplicated(cols, keep='first'), 'GradRatePonts'] = np.nan
        ##fill in empty points col with zeros
        alt['GradRatePonts'] = alt['GradRatePonts'].fillna(0)
        
        ## replicate points col for gui
        highest.rename({'GradRatePonts':'GradRatePoints4Thru7'}, axis=1, inplace=True)
        index = ['Alternative', 'EntityID']
        cols =['GradRatePoints4Thru7', 'Alternative', 'EntityID']
        alt =pd.merge(alt, highest[cols], on=index, how='left')
        
        ## pivot to create wide data
        #make GradRateType a str
        alt[self.renamed_cols['GradRateType']] = 'Year'+ alt[self.renamed_cols['GradRateType']].astype(int).astype(str)
        columns = self.renamed_cols['GradRateType']
        values = ['GradRatePonts', self.renamed_cols['GradRate']]
        index =['Alternative', 'EntityID', 'GradRatePoints4Thru7']
        alt_gr = pd.pivot(alt, index=index ,values=values, columns=columns).reset_index()
        ##resolve multiindex
        alt_gr.columns = [i[0]+i[1] for i in alt_gr.columns]
        
        #create points col
        alt_gr['GraduationRate'] = alt_gr['GradRatePoints4Thru7'].copy()
        ##cap points
        alt_gr.loc[alt_gr['GraduationRate']>(self.grad_rate_weights['Alt 9-12']*100), 'GraduationRate'] = (self.grad_rate_weights['Alt 9-12']*100)
        return alt_gr
        
        
    def get_trad_gradrates(self, gr):
        ##if a school has not met ncount in any of the 4 cohort years they will simply not appear in this result table (we don't care because it's not in a drill down)
        mask = gr.Alternative==0
        trad = gr[mask].copy()
        ##appy n-count
        mask = (trad[self.renamed_cols['NumCohort']]>=self.n_count)
        trad['Eligible'] = mask
        ## apply weights depending on rate type and multiply by grad rate to calculate points
        trad['GradRatePonts'] = (trad.RateType.map(self.trad_gradrate_weights)/100)* (trad[self.renamed_cols['GradRate']])
        ##remove points for schools cohorts that don't meet the n-count
        trad.loc[trad['NumCohort']<self.n_count, 'GradRatePonts']= np.nan
        
        ##sum points per school to calculate total points
        index=['Alternative', 'EntityID']
        totals = trad.groupby(index).agg(GraduationRate = ('GradRatePonts', 'sum')
                                         ,Eligible = ('Eligible', 'sum'))
        # cap points at 10
        totals.loc[totals.GraduationRate>(self.grad_rate_weights['9-12']*100), 'GraduationRate'] =self.grad_rate_weights['9-12']*100
        
        ## pivot to create wide data
        #make GradRateType a str
        trad[self.renamed_cols['GradRateType']] = 'Year'+ trad[self.renamed_cols['GradRateType']].astype(int).astype(str)
        columns = self.renamed_cols['GradRateType']
        values = ['GradRatePonts', self.renamed_cols['GradRate']]
        trad_gr = pd.pivot(trad, index=index ,values=values, columns=columns)
        ##resolve multiindex
        trad_gr.columns = [i[0]+i[1] for i in trad_gr.columns]
        
        #merge with totals
        trad_gr = pd.concat([totals, trad_gr], axis=1).reset_index()
        ## apply n-count
        trad_gr.loc[trad_gr['Eligible']!=4, 'GraduationRate'] = np.nan
        ## replicate points col for gui
        trad_gr['GradRatePoints4Thru7'] = trad_gr.GraduationRate.copy()
        
        return trad_gr
        

#%%
# fy= 2023
# run = 'PrelimV4'
# db = DATABASE(fiscal_year = fy
#             ,run = run
#             ,schema = 'Static'
#             ,database = 'AccountabilityArchive')
# gradrates = db.read_table(table_name ='GradRate')
# schooltype = db.read_table(table_name ='SchoolType')    

# self = GRADUATION(fy, run)
# data = self.calculate_component(gradrates, schooltype)

# # Upload data
# table_name='StateGradRateModified'
# run = 'PrelimV4'
# DATABASE(fiscal_year = fy
#         ,run = run
#         ,schema = 'Results'
#         ,database = 'AccountabilityArchive').upload_table_to_db(df=data, table_name=table_name)
# #%%drop table

# fy= 2022
# table_name='StateGradRate'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').drop_tables_in_run(table_name, table_prefix=run)
