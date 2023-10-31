# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 10:23:07 2023

@author: YFahmy
"""

from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np


class EL(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        self.atsi_col_name_map = {'EL':'EL'
                                 ,'K8TotalELProficiencyPoints':'TotalELPPK8'
                                 ,'K8TotalELGrowthPoints':'TotalELGPK8'
                                 ,'K8WeightedPercent':'WtdPctK8'
                                 ,'912TotalELProficiencyPoints':'TotalELPP912'
                                 ,'912TotalELGrowthPoints':'TotalELGP912'
                                 ,'912WeightedPercent':'WtdPct912'
                                 ,'OverallWtELProfGrowthPoints':'OWtdELPGP'
                                 
                                 ,'K8TotalNumberELFayStudents':'TotalNumELFAYK8'
                                 ,'K8NumberofProficientStudents':'NumProfK8'
                                 ,'K8TotalNumberTested':'TotalNTPPK8'
                                 ,'K8TransformedPercentProficient':'TransPctProfK8'
                                 ,'K8TransformedStatewidePcttProf':'TransStatePPK8'
                                 ,'K8StateWideSTDProf': 'TransStatePPSDK8'
                                 ,'K8NumofStudentsImp1ProfLevels':'NImp1PLK8'
                                 ,'K8NumofStudentsImp2ProfLevels':'NImp2PLK8'
                                 ,'K8NumofStudentsImp3ProfLevels':'NImp3PLK8'
                                 ,'K8ELTotalNumberTested':'TotalNTPGK8'
                                 ,'K8TransformedPercentGrowth':'TransPctGK8'
                                 ,'K8TransformedStatewidePctGrowth':'TransStatePGK8'
                                 ,'K8StateWideSTDGrowth': 'TransStatePGSDK8'
                                 
                                 ,'912TotalNumberELFayStudents':'TotalNumELFAY912'
                                 ,'912NumberofProficientStudents':'NumProf912'
                                 ,'912TotalNumberTested':'TotalNTPP912'
                                 ,'912TransformedPercentProficient':'TransPctProf912'
                                 ,'912TransformedStatewidePcttProf':'TransStatePP912'
                                 ,'912StateWideSTDProf':'TransStatePPSD912'
                                 ,'912NumofStudentsImp1ProfLevels':'NImp1PL912'
                                 ,'912NumofStudentsImp2ProfLevels':'NImp2PL912'
                                 ,'912NumofStudentsImp3ProfLevels':'NImp3PL912'
                                 ,'912ELTotalNumberTested':'TotalNTPG912'
                                 ,'912TransformedPercentGrowth':'TransPctG912'
                                 ,'912TransformedStatewidePctGrowth':'TransStatePG912'
                                 ,'912StateWideSTDGrowth':'TransStatePGSD912'
                                 }

        
    def calculate_component(self, staticfile):
        #make sure numeric cols are numeric
        cols = ['ELProf', 'ELFAY', 'SAISID', 'SchoolCode', 'StudentGrade', 'ELGrowth', 'SchoolTypeF', 'ADMIntegrity']
        staticfile = self.columns_to_numeric(staticfile, cols)

        ##---------------------------------- select relevant grades for EL according to buissness rules
        # staticfile manipulation done in loop to avoid altering original file
        sf = pd.DataFrame()
        ## EL point for k8 and 912 are calculated separetly
        for model, grades in self.el_grade_map.items():
            mask = (staticfile.StudentGrade.isin(grades)) & (staticfile.SchoolTypeF==model)
            sf_temp = staticfile [mask].copy()
            #rename cols
            cols = {'SchoolCode':'EntityID'}
            sf_temp.rename(cols, inplace=True, axis=1)
            ## only keep el students who are ELFAY
            mask = (sf_temp.EL==1) & (sf_temp.ELFAY==1) & (sf_temp.ADMIntegrity==1)
            sf_temp = sf_temp[mask].copy()
            ## since regardless of record subject, all EL score for same student should be same
            ## we want to keep only one record per kid per school, giving priority to highest grade
            sf_temp.sort_values(['SAISID', 'EntityID', 'StudentGrade', 'ELProf'], ascending =False, inplace=True)
            sf_temp = sf_temp [~sf_temp.duplicated(['SAISID', 'EntityID'])].copy()
            #add data to filtered staticfile
            sf = pd.concat([sf, sf_temp], axis=0)
        #clean up cached variables
        del sf_temp
        
        ##---------------------------------------------- create indicators
        sf['ReClassified'] = sf.ELProf==4
        #add an all col to aggregate by all subgroups
        sf['All'] = 1
        #setup an indicator for those with 2 test records
        sf['W2Records'] = sf.ELGrowth.notnull()
        #convert elgrowth to category type
        sf['ELGrowth'] = sf['ELGrowth'].astype('category')
        #create model column ('k-8'=2 and '9-12'=3)
        sf.loc[sf.StudentGrade.isin(self.el_grade_map[2]), 'Model'] = 'k-8'
        sf.loc[sf.StudentGrade.isin(self.el_grade_map[3]), 'Model'] = '9-12'
        
        
        ## make csi tables
        csi_summary, csi_drilldown = self.get_csi_tables(sf)
        
        #make atsi tables
        atsi = self.get_atsi_tables(sf)
        
        ##add ficalyear
        csi_drilldown['FiscalYear'] = self.fiscal_year
        
        return csi_summary, csi_drilldown, atsi
        
    def get_atsi_tables(self, sf):
        # loop through and aggregate each subgroup, then call functions to assemble atsi on each and append to a df to create atsi aggregates
        #add an all category to aggregation dict
        atsi_subgroups = self.atsi_subgroups_name_changes.copy()
        atsi_subgroups['All'] = {1:'All'}
        ##loop and aggregate
        atsi_long = pd.DataFrame()
        for col, col_values_dict in atsi_subgroups.items():
            for value, subg in col_values_dict.items():
                sf_temp = sf[sf[col]==value]
                
                # aggregate prof values
                el_prof_atsi = self.aggregate_prof_counts(sf_temp)
                ##add CSI mean and std prviosuly calculated for 912 and k8
                el_prof_atsi = self.add_prof_stats(el_prof_atsi)
                ## calculate points
                el_prof_atsi = self.calculate_prof_points(el_prof_atsi)
                
                # aggregate growth values
                el_growth_atsi = self.aggregate_growth_counts(sf_temp)
                ##add CSI mean and std prviosuly calculated for 912 and k8
                el_growth_atsi = self.add_growth_stats(el_growth_atsi)
                ## calculate points
                el_growth_atsi = self.calculate_growth_points(el_growth_atsi)
                
                ##get combined results in form of csi tables (atsi is similar in structure)
                summary, drilldown = self.produce_gui_tables(el_prof_atsi, el_growth_atsi)
                
                ## merge data
                data = pd.merge(summary, drilldown, on='EntityID', how='outer')
                ##add subgroup col
                data['SubGroup'] = subg
                
                ##add data to dataframe
                atsi_long = pd.concat([atsi_long, data], axis=0)
                
        ##rename cols
        atsi_long.rename(self.atsi_col_name_map, axis=1, inplace=True)
        
        #pivot to create wide data
        index = ['SchoolTypeF','EntityID']
        values = list(self.atsi_col_name_map.values())
        columns = 'SubGroup'
        atsi = pd.pivot(atsi_long, index=index, values=values, columns=columns).reset_index()
                         
        ## resolve multiindex
        atsi.columns = [i[0]+i[1] for i in atsi.columns]
        return atsi
        

    def add_prof_stats(self, el_prof_atsi):
        for model in el_prof_atsi.Model.unique():
            for key, value in self.prof_stats_by_el_model.items():
                if model in key:
                    mask = el_prof_atsi.Model==model
                    if 'mean' in key:
                        el_prof_atsi.loc[mask,'TransformedStatewidePcttProf'] = value
                    elif 'std' in key:
                        el_prof_atsi.loc[mask,'StateWideSTDProf'] = value
        return el_prof_atsi
    
    def add_growth_stats(self, el_growth_atsi):
        for model in el_growth_atsi.Model.unique():
            for key, value in self.growth_stats_by_el_model.items():
                if model in key:
                    mask = el_growth_atsi.Model==model
                    if 'mean' in key:
                        el_growth_atsi.loc[mask,'TransformedStatewidePctGrowth'] = value
                    elif 'std' in key:
                        el_growth_atsi.loc[mask,'StateWideSTDGrowth'] = value
        return el_growth_atsi
                    
    def get_csi_tables(self, sf):
        #get proficiency
        el_prof = self.get_el_prof(sf)
        #get growth
        el_growth = self.get_el_growth(sf)
        
        #format output into gui tables
        csi_summary, csi_drilldown = self.produce_gui_tables( el_prof, el_growth)

        # temporary change to remove "Transformed" from the drilldown table columns
        csi_drilldown = csi_drilldown.rename(columns={"K8TransformedPercentProficient":"K8PercentProficient",
            "K8TransformedStatewidePcttProf":"K8StatewidePcttProf", "K8TransformedPercentGrowth":"K8PercentGrowth",
            "K8TransformedStatewidePctGrowth":"K8StatewidePctGrowth", "912TransformedPercentProficient":"912PercentProficient",
            "912TransformedStatewidePcttProf":"912StatewidePcttProf", "912TransformedPercentGrowth":"912PercentGrowth",
            "912TransformedStatewidePctGrowth":"912StatewidePctGrowth"})
               
        
        return csi_summary, csi_drilldown
        
    def produce_gui_tables(self, el_prof, el_growth):
        ## get sum of fay students for each federal model type
        csi_drilldown = pd.merge(el_prof, el_growth, on=['SchoolTypeF', 'EntityID', 'Model'], how='outer')
        fay_count = csi_drilldown.groupby(['SchoolTypeF', 'EntityID']).agg(ELFAYTotalNumber = ('TotalNumberELFayStudents', 'sum'))
        
        ##get summary total point count for proficiency and growth
        points_col = ['TotalELProficiencyPoints', 'TotalELGrowthPoints']
        csi_drilldown['ELProficiencyandGrowthPoints'] = csi_drilldown[points_col].sum(axis=1)
        
        ##----------------------------------format results
        ##--- pivot proficiency
        #pivot to creat wide data where each school has one instance where the col nmae is prefixed by the model consistent with SQL SERVER
        values = ['TotalNumberELFayStudents'
                ,'NumberofProficientStudents'
                ,'TotalNumberTested'
                ,'TransformedPercentProficient'
                ,'TransformedStatewidePcttProf'
                ,'StateWideSTDProf'
                ,'TotalELProficiencyPoints'
                ,'NumofStudentsImp1ProfLevels'
                ,'NumofStudentsImp2ProfLevels'
                ,'NumofStudentsImp3ProfLevels'
                ,'ELTotalNumberTested'
                ,'TransformedPercentGrowth'
                ,'TransformedStatewidePctGrowth'
                ,'StateWideSTDGrowth'
                ,'TotalELGrowthPoints'
                ,'ELProficiencyandGrowthPoints']
        index = ['SchoolTypeF', 'EntityID']
        csi_drilldown.Model = csi_drilldown.Model.str.replace('-','').str.upper()
        csi_drilldown = pd.pivot(csi_drilldown, index=index, values=values, columns='Model')
        csi_drilldown.columns = [i[1]+str(i[0]) for i in csi_drilldown.columns]
        
        ##-------------------------------- separate summary and drilldown tables
        summary_cols = ['Points', 'TotalNumberELFayStudents'] + index
        cols = []
        for i in csi_drilldown.columns:
            for j in summary_cols:
                if j in i:
                    cols.append(i)
        ##make summary table
        csi_summary = pd.concat([csi_drilldown[cols], fay_count], axis=1).reset_index()

        #make summary columns for k-11 and k-12
        csi_summary['912WeightedPercent'] =(csi_summary['912TotalNumberELFayStudents']*100/csi_summary['ELFAYTotalNumber']).round(2)
        csi_summary['K8WeightedPercent'] = (csi_summary['K8TotalNumberELFayStudents']*100/csi_summary['ELFAYTotalNumber']).round(2)
        
        points=csi_summary[['912WeightedPercent', 'K8WeightedPercent']].copy()
        points['9-12'] = (csi_summary['912WeightedPercent']*csi_summary['912ELProficiencyandGrowthPoints'])/100
        points['k-8'] = (csi_summary['K8WeightedPercent']*csi_summary['K8ELProficiencyandGrowthPoints'])/100
        csi_summary['OverallWtELProfGrowthPoints'] = points[['k-8', '9-12']].sum(axis=1).round(2)
        
        #apply n-count
        points_col = [i for i in csi_summary.columns if 'Points' in i]
        csi_summary.loc[csi_summary.ELFAYTotalNumber<self.n_count, points_col] = np.nan
        #make final point col
        csi_summary['EL'] = csi_summary['OverallWtELProfGrowthPoints'].copy()
        csi_summary['ELPoints'+str(self.fiscal_year)] = csi_summary['OverallWtELProfGrowthPoints'].copy()
        
        ##--------------------------------- make a federalModel col for csi_drilldown
        csi_drilldown.reset_index(inplace=True)
        csi_drilldown['FederalModel'] = csi_drilldown.SchoolTypeF.map(self.federal_school_type_map)
        
        ##-------------------------------- drop extra cols from drilldown and summary
        extra_summary = [i for i in csi_summary.columns if 'Number' in i]
        extra_drilldown = [i for i in csi_drilldown.columns if 'Points' in i] + ['SchoolTypeF']
        csi_summary.drop(extra_summary, axis=1, inplace=True)
        csi_drilldown.drop(extra_drilldown, axis=1, inplace=True)
        return csi_summary, csi_drilldown
        
    def get_el_prof(self, sf):
        
        el_prof = self.aggregate_prof_counts(sf)
        #get statewide mean and std
        el_prof = self.calculate_prof_stats(el_prof)
        #get points
        el_prof = self.calculate_prof_points(el_prof)
        return el_prof
    
    def aggregate_prof_counts(self, sf):
        ##--------------------------------- aggregate data to calculate EL prof
        group = sf.groupby(['SchoolTypeF','Model','EntityID'])
        el_prof = group.agg(TotalNumberELFayStudents = ('SAISID', 'nunique')
                              ,NumberofProficientStudents = ('ReClassified', 'sum')
                              ,TotalNumberTested = ('ELTested', 'sum')).reset_index()
        ##calculate percent proficient (# proficient/# ELfay ) 
        ## Proficiency out of EL-FAY and not just tested
        el_prof['TransformedPercentProficient'] = (el_prof['NumberofProficientStudents']*100/ el_prof['TotalNumberELFayStudents']).round()
        return el_prof
        
    def calculate_prof_stats(self, el_prof):
        ## make class attributes dictionaries to hold prof stats
        self.prof_stats_by_el_model = {}
        #------------agg by model and calculate mean and std
        for m in el_prof.Model.unique():
            #calculate mean and std based on dist without outliers using only those that meet the n-count
            mask_mean = (el_prof.Model==m) & (el_prof.TransformedPercentProficient>0) & (el_prof.TotalNumberELFayStudents>=self.n_count)
            dist = el_prof.loc[mask_mean, 'TransformedPercentProficient']
            q1 = np.percentile(dist, 25, interpolation = 'midpoint')
            q3 = np.percentile(dist, 75, interpolation = 'midpoint')
            iqr_out = (q3 - q1) * 1.5
            #remove outliers before mean calc
            no_outliers = dist[(dist >= (q1-iqr_out)) & (dist <= (q3+iqr_out))]
            ## add mean and std columns
            mask = el_prof.Model==m
            mean = round(no_outliers.mean(),2)
            std = round(no_outliers.std(),2)
            el_prof.loc[mask, 'TransformedStatewidePcttProf'] = mean
            el_prof.loc[mask, 'StateWideSTDProf'] = std
            ## make a col of stats to be used by atsi
            self.prof_stats_by_el_model[m+'_mean'] = mean
            self.prof_stats_by_el_model[m+'_std'] = std
        
        return el_prof
    
    def calculate_prof_points(self, el_prof):
        ##---------------create function to assign points for prof
        def el_prof_points(x):
            if x['TransformedPercentProficient'] >= x['TransformedStatewidePcttProf']:
                return 5
            elif x['TransformedPercentProficient']==0:
                return 0 
            elif x['TransformedPercentProficient'] >= x['TransformedStatewidePcttProf']-(x['StateWideSTDProf']*0.5):
                return 4
            elif x['TransformedPercentProficient'] >= x['TransformedStatewidePcttProf']-(x['StateWideSTDProf']):
                return 3
            elif x['TransformedPercentProficient'] >= x['TransformedStatewidePcttProf']-(x['StateWideSTDProf']*2):
                return 2
            elif x['TransformedPercentProficient'] >= x['TransformedStatewidePcttProf']-(x['StateWideSTDProf']*3):
                return 1
        ##------------------------- calculate points
        for m in el_prof.Model.unique():
            ## calculate points
            mask = el_prof.Model==m
            cols = ['TransformedPercentProficient', 'TransformedStatewidePcttProf', 'StateWideSTDProf']
            el_prof.loc[mask, 'TotalELProficiencyPoints'] = el_prof.loc[mask, cols].apply(el_prof_points, axis=1)
        ##return results
        return el_prof
    
    def get_el_growth(self, sf):
        el_growth = self.aggregate_growth_counts(sf)
        
        el_growth= self.calculate_growth_stats(el_growth)
        
        el_growth= self.calculate_growth_points(el_growth)
        return el_growth
    
    def aggregate_growth_counts(self, sf):
        
        #group by EntityID and model
        group = sf.groupby(['SchoolTypeF', 'EntityID', 'Model'])
        #count records eligible for growth
        el_growth = group.agg(ELTotalNumberTested = ('W2Records','sum')
                              ,TotalNumberELFay = ('SAISID', 'nunique'))
        
        #calculate count of each growth level per school per model
        growth_breakdown = group['ELGrowth'].value_counts().reset_index(name='Count')
        #rename and convert datatypes from category to int
        growth_breakdown.rename({'level_3':'ELGrowth'}, axis=1, inplace=True)
        growth_breakdown['ELGrowth'] = pd.to_numeric(growth_breakdown['ELGrowth'], errors='coerce')
        
        #using the actual growth values as weights to get the numerator. 
        #Since we multiply kids who grew 2 levels by a weight of 2 and so on
        growth_breakdown ['WeighedCount'] = growth_breakdown.ELGrowth * growth_breakdown.Count
        numerator = growth_breakdown.groupby(['SchoolTypeF','EntityID', 'Model']).agg(Numerator = ('WeighedCount', 'sum'))
        #pivot to create wide data
        ## convert ELGrowth to strings that match col names in server
        prefix = 'NumofStudentsImp'
        sufix = 'ProfLevels'
        growth_breakdown.ELGrowth = prefix + growth_breakdown.ELGrowth.astype(int).astype(str) + sufix
        index=['SchoolTypeF','EntityID', 'Model']
        growth_breakdown = pd.pivot(data=growth_breakdown, index=index, values='Count', columns='ELGrowth')
        growth_breakdown.fillna(0, inplace=True)
        #bring in the 3 datasets togetther. 
        ## el_growth has number of ELFAY students with 2 test records
        ## numerator has the numertaor value to calculate growth % per school and model
        ## growth_breakdown has counts of students who improved certain prof levels
        el_growth = pd.concat([el_growth, growth_breakdown, numerator], axis=1).reset_index()
        
        ## get percent growth
        el_growth['TransformedPercentGrowth'] = (el_growth.Numerator*100 / el_growth.TotalNumberELFay).round()
        return el_growth
        
    def calculate_growth_stats(self, el_growth):
        ## get mean and std per model
        ## make class attributes dictionaries to hold prof stats
        self.growth_stats_by_el_model = {}
        for m in el_growth.Model.unique():
            #calculate mean and std based on dist without outliers
            mask_mean = (el_growth.Model==m) & (el_growth.TransformedPercentGrowth>0) & (el_growth.TotalNumberELFay>=self.n_count)
            dist = el_growth.loc[mask_mean, 'TransformedPercentGrowth']
            q1 = np.percentile(dist, 25, interpolation = 'midpoint')
            q3 = np.percentile(dist, 75, interpolation = 'midpoint')
            iqr_out = (q3 - q1) * 1.5
            #remove outliers before mean calc
            no_outliers = dist[(dist >= q1-iqr_out) & (dist <= q3+iqr_out)]
            ## add mean and std columns
            mask = el_growth.Model==m
            mean = round(no_outliers.mean(),2)
            std = round(no_outliers.std(),2)
            el_growth.loc[mask, 'TransformedStatewidePctGrowth'] = mean
            el_growth.loc[mask, 'StateWideSTDGrowth'] = std
            ## make a col of stats to be used by atsi
            self.growth_stats_by_el_model[m+'_mean'] = mean
            self.growth_stats_by_el_model[m+'_std'] = std
        return el_growth
            
    def calculate_growth_points(self, el_growth):
        ## create function to assign points for grwoth
        def el_growth_points(x):
            if x['TransformedPercentGrowth'] >= x['TransformedStatewidePctGrowth']:
                return 5
            elif x['TransformedPercentGrowth']==0:
                return 0 
            elif x['TransformedPercentGrowth'] >= x['TransformedStatewidePctGrowth']-(x['StateWideSTDGrowth']*0.5):
                return 4
            elif x['TransformedPercentGrowth'] >= x['TransformedStatewidePctGrowth']-(x['StateWideSTDGrowth']):
                return 3
            elif x['TransformedPercentGrowth'] >= x['TransformedStatewidePctGrowth']-(x['StateWideSTDGrowth']*2):
                return 2
            elif x['TransformedPercentGrowth'] >= x['TransformedStatewidePctGrowth']-(x['StateWideSTDGrowth']*3):
                return 1
        # agg by model and assign points
        for m in el_growth.Model.unique():
            ## calculate points
            mask = (el_growth.Model==m)
            cols = ['TransformedPercentGrowth', 'TransformedStatewidePctGrowth', 'StateWideSTDGrowth']
            el_growth.loc[mask, 'TotalELGrowthPoints'] = el_growth.loc[mask, cols].apply(el_growth_points, axis=1)
        return el_growth
    
#%%
# fy = 2023
# run = 'PrelimV4'
# self = EL(fy, run=run)


# staticfile = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive').read_table(table_name ='StaticFile')
# #%% calculate EL component
# self = EL(2022)
# csi_summary, csi_drilldown, atsi = self.calculate_component(staticfile)

# #Upload data
# fy= 2022
# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_el_drilldown_columns()
# ## select cols and produce ssi.growth
# csi_drilldown = csi_drilldown[cols]



# data = {'EL': [csi_drilldown, '', 'ssi']
#         ,'CSIEL':[csi_summary, 'Prelim', 'Results']
#         ,'ATSIEL': [atsi, 'Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][1]
#             ,schema = data[i][2]
#             ,database = 'AccountabilityArchive').upload_table_to_db(df=data[i][0], table_name=i)

# #%%upload 2023 table for Mayank
# fy= 2023
# self = EL(fy)
# csi_summary, csi_drilldown, atsi = self.calculate_component(staticfile)
# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_el_drilldown_columns()
# ## select cols and produce ssi.CA
# #find and select cols missing from results tables
# missing_cols = []
# for i in cols:
#     if i not in csi_drilldown.columns:
#         missing_cols.append(i)
#         print(i, 'is missing & will be recreated as an empty column')
# ##make empty cols to replace missing cols (temporarily)
# csi_drilldown.loc[:,missing_cols] = np.nan
# ## select all returned cols and upload to db
# csi_drilldown = csi_drilldown[cols]

# DATABASE(fiscal_year = fy
#         ,run = ''
#         ,schema = 'ssi'
#         ,database = 'AccountabilityArchive').upload_table_to_db(df=csi_drilldown, table_name='EL')
# #%%drop tables
# fy= 2022
# data = {'CSIEL':['Prelim', 'Results']
#         ,'EL': ['', 'ssi']
#         ,'ATSIEL': ['Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][0]
#             ,schema = data[i][1]
#             ,database = 'AccountabilityArchive').drop_tables_in_run(i, table_prefix=data[i][0])