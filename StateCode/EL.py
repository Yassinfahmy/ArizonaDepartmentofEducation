# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 11:04:49 2023

@author: yfahmy
"""
from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np

class EL(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        
        ## define a list of cols that must be numeric for this module to succeed
        self.numeric_cols = ['EL', 'ELFAY', 'SAISID', 'EntityID', 'StudentGrade', 'ELProf', 'ELGrowth', 'ELTested']
        ## rename cols in staticfile in case we use the old staticfile naming conventions
        self.col_rename={'ELPROF':'ELProf'
                         ,'SchoolCode':'EntityID'}
        
    def calculate_component(self, staticfile_raw, schooltype):
        staticfile = staticfile_raw[staticfile_raw.ADMIntegrity==1].copy()
        ##---------------------------- wrangling
        ##rename cols for consistency
        staticfile.rename(self.col_rename, axis=1, inplace=True)
        ## make sure numeric cols are numeric
        staticfile = self.columns_to_numeric(staticfile, self.numeric_cols)
        
        ## only keep el students who are ELFAY
        mask = (staticfile.EL==1) & (staticfile.ELFAY==1)
        staticfile = staticfile[mask].copy()
        
        ## since regardless of record subject, all EL score for same student should be same
        ## we want to keep only one record per kid per school, giving priority to highest grade
        staticfile.sort_values(['SAISID', 'EntityID', 'StudentGrade', 'ELProf'], ascending =False, inplace=True)
        staticfile = staticfile [~staticfile.duplicated(['SAISID', 'EntityID'])].copy()
        ## select relevant grades for EL according to buissness rules
        sf = pd.DataFrame()
        ## EL point for k8 and 912 are calculated separetly
        for i in self.el_grade_map.keys():
            sf_temp = staticfile [staticfile.StudentGrade.isin(self.el_grade_map[i])].copy()
            sf_temp['Model'] = i
            # create reclassified bool indic
            sf_temp['ReClassified'] = (sf_temp.ELProf==4)
            sf = pd.concat([sf, sf_temp], axis=0)
        #clean up cached variables
        del sf_temp
        
        #get proficiency
        el_prof = self.calculate_el_prof(sf)
        #turn pct to percentage
        pct_columns=[]
        pct_cols =['Percent', 'Mean', 'STD']
        for col in el_prof.columns:
            for regex in pct_cols:
                if regex.lower() in col.lower():
                    pct_columns.append(col)
        el_prof[pct_columns] = el_prof[pct_columns]*100
        
        #get growth
        el_growth = self.calculate_el_growth(sf)
        
        #combine into one table and return dataset
        el_data = pd.merge(left=el_prof, right=el_growth, on=['EntityID', 'Model'], how='outer')
        
        #remove unneeded cols
        el_data.drop('Numerator', axis=1, inplace=True)
        
        # add total EL points cols
        total_points_cols = ['ELProficiencyandGrowth', 'ELProficiencyandGP', 'ELProficiencyandGrowthPoints']
        for i in total_points_cols:
            el_data[i] = el_data.TotalELProficiencyPoints + el_data.TotalELGrowthPoints
            
        #make a fiscal year col
        el_data['FiscalYear']= self.fiscal_year
        #------------------------diffrentiate alt from trad schools
        schools = schooltype[['FiscalYear', 'SchoolCode', 'Alternative']]
        el_data = pd.merge(el_data, schools , left_on=['FiscalYear', 'EntityID'], right_on=['FiscalYear', 'SchoolCode'], how='left')
        el_data.loc[el_data.Alternative==True, 'Model'] = 'Alt '+ el_data.loc[el_data.Alternative==True, 'Model']
        
        ## add suffices
        el_data = self.add_suffices(el_data)
        
        #round numeric cols
        el_data = self.round_numeric_cols(el_data)[0]
        ##add if not eligible for growth not eligible for prof
        
        return el_data
        
    def add_suffices(self, el_data):
        ##add suffixes to identify cols
        common_cols = ['EntityID', 'Model', 'FiscalYear']
        summary_cols = ['TotalELProficiencyPoints', 'TotalELGrowthPoints','ELProficiencyandGrowth', 'ELProficiencyandGP', 'ELProficiencyandGrowthPoints']
        drill_down_cols = ['TotalNumberELFay', 'NumberOfProficient',
                           'NumberTested', 'PercentProficient', 'StateWideMeanProf',
                           'NumberTestedW2Records','NumOfStudentsImp1ProfLevels'
                           ,'NumOfStudentsImp2ProfLevels','NumOfStudentsImp3ProfLevels'
                           ,'PercentGrowth', 'StateWideMeanGrowth']
        name_change=[]
        for i in el_data.columns:
            if i in drill_down_cols:
                name_change.append(str(i+'_DrillDown'))
            elif i in summary_cols:
                name_change.append(str(i+'_Summary'))
            elif i in common_cols:
                name_change.append(str(i+'_All'))
            else:
                name_change.append(str(i+'_Extra'))
        el_data.columns = name_change
        
        return el_data
    
    def calculate_el_prof(self, sf):

        ##--------------------------------- aggregate data to calculate EL prof
        group = sf.groupby(['Model', 'EntityID'])
        el_prof = group.agg(TotalNumberELFay = ('SAISID', 'nunique')
                              ,NumberOfProficient = ('ReClassified', 'sum')
                              ,NumberTested = ('ELTested', 'sum')).reset_index()
        ##calculate percent proficient (# proficient/# ELfay tested)
        ## Proficiency out of  tested
        el_prof['PercentProficient'] = el_prof['NumberOfProficient']/ el_prof['NumberTested']
        
        ## create function to assign points for prof
        def el_prof_points(x):
            if x['PercentProficient'].round(4) >= x['StateWideMeanProf'].round(4):
                return 5
            elif x['PercentProficient'].round(4)==0:
                return 0 
            elif x['PercentProficient'].round(4) >= (x['StateWideMeanProf']-(x['StateWideSTDProf']*0.5)).round(4):
                return 4
            elif x['PercentProficient'].round(4) >= (x['StateWideMeanProf']-(x['StateWideSTDProf'])).round(4):
                return 3
            elif x['PercentProficient'].round(4) >= (x['StateWideMeanProf']-(x['StateWideSTDProf']*2)).round(4):
                return 2
            elif x['PercentProficient'].round(4) >= (x['StateWideMeanProf']-(x['StateWideSTDProf']*3)).round(4):
                return 1
        # agg by model and assign points
        for m in el_prof.Model.unique():
            #calculate mean and std based on dist without outliers
            mask_mean = (el_prof.Model==m) & (el_prof.PercentProficient>0) & (el_prof.NumberTested >= self.n_count)
            dist = el_prof.loc[mask_mean, 'PercentProficient']
            q1 = np.percentile(dist, 25, interpolation = 'midpoint')
            q3 = np.percentile(dist, 75, interpolation = 'midpoint')
            iqr_out = (q3 - q1) * 1.5
            #remove outliers before mean calc
            no_outliers = dist[(dist > q1-iqr_out) & (dist < q3+iqr_out)]
            ## add mean and std columns
            mask = el_prof.Model==m
            el_prof.loc[mask, 'StateWideMeanProf'] = no_outliers.mean()
            el_prof.loc[mask, 'StateWideSTDProf'] = no_outliers.std()
            
            ## calculate points
            mask = (el_prof.NumberTested>=self.n_count) & (el_prof.Model==m)
            cols = ['PercentProficient', 'StateWideMeanProf', 'StateWideSTDProf']
            el_prof.loc[mask, 'TotalELProficiencyPoints'] = el_prof.loc[mask, cols].apply(el_prof_points, axis=1)

        return el_prof
    
    def calculate_el_growth(self, sf):
        #calc el growth
        sf = sf[sf.ELGrowth.notnull()]
        #group by EntityID and model
        index = ['Model', 'EntityID']
        group = sf.groupby(index)
        
        #count records eligible for growth
        el_growth = group.agg(NumberTestedW2Records = ('SAISID', 'nunique'))
        
        #calculate count of each growth level per school per model
        growth_breakdown = group['ELGrowth'].value_counts().reset_index(name='Count')
        #Using the actual growth categroy as weights
        growth_breakdown ['WeighedCount'] = growth_breakdown.ELGrowth * growth_breakdown.Count
    
        numerator = growth_breakdown.groupby(index).agg(Numerator = ('WeighedCount', 'sum'))
        
        #pivot to create wide data
        ## convert ELGrowth to strings that match col names in server
        prefix = 'NumOfStudentsImp'
        sufix = 'ProfLevels'
        growth_breakdown.ELGrowth = growth_breakdown.ELGrowth.astype(int).astype(str)
        growth_breakdown.ELGrowth = prefix+growth_breakdown.ELGrowth+sufix
        growth_breakdown = pd.pivot(data=growth_breakdown, index=index, values='Count', columns='ELGrowth')
        growth_breakdown.fillna(0, inplace=True)
        
        #bring in the 3 datasets togetther. 
        ## el_growth has number of ELFAY students with 2 test records
        ## numerator has the numertaor value to calculate growth % per school and model
        ## growth_breakdown has counts of students who improved certain prof levels
        el_growth = pd.concat([el_growth, numerator, growth_breakdown], axis=1).reset_index()
        
        ## get percent growth
        el_growth['PercentGrowth'] = el_growth.Numerator / el_growth.NumberTestedW2Records
        
        ## get mean and std per model and calculate points
        ## create function to assign points for grwoth
        def el_growth_points(x):
            if x['PercentGrowth'].round(2) >= x['StateWideMeanGrowth'].round(2):
                return 5
            elif x['PercentGrowth'].round(2)==0:
                return 0 
            elif x['PercentGrowth'].round(2) >= (x['StateWideMeanGrowth']-(x['StateWideSTDGrowth']*0.5)).round(2):
                return 4
            elif x['PercentGrowth'].round(2) >= (x['StateWideMeanGrowth']-(x['StateWideSTDGrowth'])).round(2):
                return 3
            elif x['PercentGrowth'].round(2) >= (x['StateWideMeanGrowth']-(x['StateWideSTDGrowth']*2)).round(2):
                return 2
            elif x['PercentGrowth'].round(2) >= (x['StateWideMeanGrowth']-(x['StateWideSTDGrowth']*3)).round(2):
                return 1
        # agg by model and assign points
        for m in el_growth.Model.unique():
            #calculate mean and std based on dist without outliers
            mask_mean = (el_growth.Model==m) & (el_growth.PercentGrowth>0) & (el_growth.NumberTestedW2Records>=self.n_count)
            dist = el_growth.loc[mask_mean, 'PercentGrowth']
            q1 = np.percentile(dist, 25, interpolation = 'midpoint')
            q3 = np.percentile(dist, 75, interpolation = 'midpoint')
            iqr_out = (q3 - q1) * 1.5
            #remove outliers before mean calc
            no_outliers = dist[(dist > q1-iqr_out) & (dist < q3+iqr_out)]
            ## add mean and std columns
            mask = el_growth.Model==m
            el_growth.loc[mask, 'StateWideMeanGrowth'] = no_outliers.mean()
            el_growth.loc[mask, 'StateWideSTDGrowth'] = no_outliers.std()
            
            ## calculate points
            mask = (el_growth.NumberTestedW2Records>=self.n_count) & (el_growth.Model==m)
            cols = ['PercentGrowth', 'StateWideMeanGrowth', 'StateWideSTDGrowth']
            el_growth.loc[mask, 'TotalELGrowthPoints'] = el_growth.loc[mask, cols].apply(el_growth_points, axis=1)

        return el_growth
    

# self = EL(2023)
# # #%% bring in staticfile
# fy= 2023
# run = 'PrelimV6'
# staticfile_raw = DATABASE(fiscal_year = fy
#                         ,run = run
#                         ,schema = 'Static'
#                         ,database = 'AccountabilityArchive').read_table(table_name ='StaticFile')
# schooltype = DATABASE(fiscal_year = fy
#                         ,run = run
#                         ,schema = 'Static'
#                         ,database = 'AccountabilityArchive').read_table(table_name ='schooltype')
# #%% calculate EL component
# el = EL(2022)
# el_data = el.calculate_component(staticfile_raw, schooltype)

# #%% Upload data
# fy= 2022
# table_name='StateEL'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').upload_table_to_db(df=el_data, table_name=table_name)
# #%%drop table
# fy= 2022
# table_name='StateEL'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').drop_tables_in_run(table_name, table_prefix=run)

        
        
        

        