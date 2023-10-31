# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 10:14:05 2023

@author: yfahmy
"""
from COMPONENTS import COMPONENTS
import pandas as pd
from DATABASE import DATABASE
import numpy as np
from TABLES import TABLES

class GROWTH(COMPONENTS):
    
    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        self.histogram_bins = [0, 19, 39, 59, 79, 100]
        self.hist_bins_map = {'(-0.001, 19.0]':'0thru19'
                             ,'(19.0, 39.0]':'20thru39'
                             ,'(39.0, 59.0]':'40thru59'
                             ,'(59.0, 79.0]':'60thru79'
                             ,'(79.0, 100.0]':'80thru100'}
        ## get growth weights for each model from component dict
        self.growth_weights_by_model = {}
        for model, components in self.federal_model_weights.items():
            for component, weight in components.items():
                if 'growth' in component.lower():
                    self.growth_weights_by_model[model] = weight/100
        
    def calculate_component(self, staticfile_raw):
        staticfile = staticfile_raw.copy()
        
        ##make sure numeric cols are numeric
        numeric_cols = ['FAY', 'StudentGrade', 'SGP_CCR', 'SAISID', 'SchoolCode', 'SchoolTypeF', 'ADMIntegrity']
        staticfile = self.columns_to_numeric(staticfile, numeric_cols)

        ##only include FAY records
        staticfile = staticfile [staticfile.FAY > 0]
        ## only include Math and ELA records
        staticfile = staticfile [staticfile.Subject.isin(self.growth_subjects)]
        ## only include students with SGP_CCR value (an SGP score)
        staticfile = staticfile[staticfile.SGP_CCR.notnull()]
        ## turn all act cohort students to grade 11 and non-cohort 2023 that are grade 11 to 111
        staticfile.loc[staticfile.StudentGrade==self.cy_act_grade, 'StudentGrade']=111
        staticfile.loc[staticfile.Cohort==self.cy_act_cohort, 'StudentGrade']=self.cy_act_grade
        ## Make a federal modeltype column
        # staticfile['FederalModel'] = staticfile.SchoolTypeF.map(self.federal_school_type_map)
        
        ##---------------------------------- select relevant data for Growth according to buissness rules
        sf = pd.DataFrame()
        #make a list of relevant cols to keep (including cols needed for ATSI)
        relevant_cols = ['SAISID', 'SchoolCode', 'StudentGrade', 'Subject', 'SGP_CCR', 'FAY', 'SchoolTypeF'] + self.subgroups
        ## select relevant files from staticfile
        for model, grades in self.growth_grade_map.items():
            #select the models that contain growth only
            #select the grades relevant to the selected model
            ## select Relevant columns only
            growth_mask = (staticfile.SchoolTypeF == model) & (staticfile.StudentGrade.isin(grades)) & (staticfile.ADMIntegrity==1)
            sf_temp = staticfile.loc[growth_mask, relevant_cols].copy()
            #build up the filtered staticfile
            sf = pd.concat([sf, sf_temp], axis=0)
        #clean up cache
        del sf_temp, staticfile
            
        ##replace names in relevant subgroups for consistency
        sf.replace(self.atsi_subgroups_name_changes, inplace=True)
        
        #rename cols
        #change SchoolCode to Entity ID
        name_change = {'SchoolCode':'EntityID'}
        sf.rename(name_change, axis=1, inplace=True)
        
        #add an all col
        sf['All'] = 'All'
        
        ## CSI summary has an Extra 'TotalFAYCount' col
        ## Need to remove grade 11 data since its not in the GUI 'Grade11'
        csi_summary, csi_drilldown = self.get_csi(sf)
        
        ## in profatsi table (look for Growth) in  Growth table (look for MSGP)
        ##change school code to EntityID and label FederalModel as Extra for ATSI (its not for CSI)
        atsi_1, atsi_2 = self.get_atsi(sf)
        
        ##merge both ATSI table cols into one
        atsi = pd.merge(atsi_1, atsi_2, on=['EntityID', 'SchoolTypeF'], how='outer')

        return csi_summary, csi_drilldown, atsi
        
    def get_atsi(self, sf):
        
        #make an all col to aggregate for the all col in ATSI
        subgroups_w_all = self.subgroups + ['All']
        
        ## aggregate by subgroup to produce median SGP and count
        data_all = pd.DataFrame()
        data_subject = pd.DataFrame()
        for subg in subgroups_w_all:
            ##============================================================ get aggregations at school level
            by_subgroup = sf.groupby(['SchoolTypeF', 'EntityID', subg]).agg(MSGP= ('SGP_CCR', 'median')
                                                                               ,TotalFAYCount = ('SAISID', 'nunique')).reset_index()
            #remove un intended aggregates and rename col
            by_subgroup = by_subgroup[by_subgroup[subg] !=0]
            by_subgroup = by_subgroup[by_subgroup[subg] !='U'].copy()
            by_subgroup.rename({subg : 'SubGroup'}, inplace=True, axis=1)
            
            # calculate growth points
            by_subgroup['Growth'] = (by_subgroup.SchoolTypeF.map(self.growth_weights_by_model)) * by_subgroup.MSGP
            #apply n-count
            by_subgroup.loc[ by_subgroup.TotalFAYCount<self.n_count, 'Growth'] = np.nan
            ## add subgroup data
            data_all = pd.concat([data_all, by_subgroup])
            
            ##============================================================ get aggregations at school and subject level
            ###FAYStuGS is count of FAY students with growth score (named this way to match SQL table)
            by_subject_subgroup = sf.groupby(['SchoolTypeF', 'EntityID', 'Subject', subg]).agg(MSGP= ('SGP_CCR', 'median')
                                                                                                  ,FAYStuGS = ('SAISID', 'nunique')).reset_index()
            #remove un intended aggregates and rename col
            by_subject_subgroup = by_subject_subgroup[by_subject_subgroup[subg] !=0]
            by_subject_subgroup = by_subject_subgroup[by_subject_subgroup[subg] !='U'].copy()
            by_subject_subgroup.rename({subg : 'SubGroup'}, inplace=True, axis=1)
            ## add subgroup data
            data_subject = pd.concat([data_subject, by_subject_subgroup])
        
        #----------------------------------------------------------------------pivot to create wide data
        #create wide data_all
        index=['SchoolTypeF', 'EntityID']
        values=['MSGP', 'Growth']
        atsi_1 = pd.pivot(data_all, index=index, columns='SubGroup', values=values)
        #resolve multiindex name col names to match SQL server
        msgp_prefix = 'FY'+str(self.fiscal_year)
        atsi_1.columns = [i[0]+i[1] if 'MSGP' not in i[0] else msgp_prefix+i[0]+i[1] for i in atsi_1.columns]
        
        #create wide data_subject
        index=['SchoolTypeF', 'EntityID']
        values=['MSGP', 'FAYStuGS']
        cols = ['Subject', 'SubGroup']
        atsi_2 = pd.pivot(data_subject, index=index, columns=cols, values=values)
        #resolve multiindex
        atsi_2.columns = [i[0]+i[1]+i[2] for i in atsi_2.columns]
        
        ###-------------------------------------------------------------------- merge data in both tables
        #make a list of ATSI 2 table col to extract it from ATSI 1 table
        atsi_2_cols_to_move = [i for i in atsi_1.columns if 'MSGP' in i]
        #add those cols data to atsi_2
        atsi_2 = pd.concat([atsi_1[atsi_2_cols_to_move], atsi_2], axis=1).reset_index()
        #remove cols from atsi_1
        atsi_1= atsi_1.drop(atsi_2_cols_to_move, axis=1).reset_index()
    
        return atsi_1, atsi_2
            
    def get_csi(self, sf):
        ##make a str type student grade col for ease of naming
        sf['Grade'] = 'Grade' + sf.StudentGrade.astype(int).astype(str)
        csi_drilldown = self.get_csi_drilldown(sf)
        csi_summary = self.get_csi_summary(sf)
        
        return csi_summary, csi_drilldown
        
    def get_csi_drilldown(self,sf):
        ##--------------------------------------------------------------------- start aggregations for drill down page
        subject_by_grade_group = sf.groupby(['SchoolTypeF', 'EntityID', 'Grade', 'Subject'])
        subject_by_grade = subject_by_grade_group.agg(FAYStdGrowthScore = ('SAISID', 'nunique')
                                                      ,MedianSGP = ('SGP_CCR', 'median')).reset_index()
        ##---------------------pivot aggregations to make wide DrillDown table data
        index = ['SchoolTypeF', 'EntityID']
        columns = ['Subject', 'Grade']
        values = ['FAYStdGrowthScore', 'MedianSGP']
        subject_by_grade = pd.pivot(subject_by_grade, index=index, columns=columns, values=values)
        #resolve multiindex and name each col to match sql server
        subject_by_grade.columns = [i[1]+i[0]+i[2] for i in subject_by_grade.columns]
        
        ##------------------get the data for the histogram in the UI
        hist_data = subject_by_grade_group['SGP_CCR'].value_counts(bins=self.histogram_bins).rename('BinCounts').reset_index()
        hist_data['Bins'] = hist_data.SGP_CCR.astype(str).map(self.hist_bins_map)
        ##pivot aggregations to make wide DrillDown data
        index = ['SchoolTypeF', 'EntityID']
        columns = ['Bins', 'Subject', 'Grade']
        values = 'BinCounts'
        hist_data = pd.pivot(hist_data, index=index, columns=columns, values=values)
        hist_data.fillna(0, inplace=True)
        #resolve multiindex and name each col to match sql server
        hist_data.columns = [i[2]+i[1]+i[0] for i in hist_data.columns]
        
        ##------------------get median of all grades by school, subject and model
        subject_all_grades = sf.groupby(['SchoolTypeF', 'EntityID', 'Subject']).agg(MedianSGPAllStudents = ('SGP_CCR', 'median')).reset_index()
        ##pivot aggregations to make wide DrillDown data
        index = ['SchoolTypeF', 'EntityID']
        columns = 'Subject'
        values = 'MedianSGPAllStudents'
        subject_all_grades = pd.pivot(subject_all_grades, index=index, columns=columns, values=values)
        #resolve multiindex and name each col to match sql server
        subject_all_grades.columns = [i+'MedianSGPAllStudents' for i in subject_all_grades.columns]
        
        ##merge all data
        csi_drilldown = pd.concat([subject_by_grade, subject_all_grades, hist_data], axis=1).reset_index()
        #make a fiscalyear col
        csi_drilldown['FiscalYear'] = self.fiscal_year
        #add FederalModel col
        csi_drilldown['FederalModel'] = csi_drilldown.SchoolTypeF.map(self.federal_school_type_map)
        
        return csi_drilldown
    
    def get_csi_summary(self, sf):
        ##--------------------------------------------------------------------- start aggregations for summary page
        by_grade = sf.groupby(['SchoolTypeF', 'EntityID', 'Grade']).agg(StudentswithGrowthScore = ('SAISID', 'nunique')
                                                                           ,MedianSGP = ('SGP_CCR', 'median')).reset_index()
        ##pivot data to create wide form
        index =  ['SchoolTypeF', 'EntityID']
        columns = 'Grade'
        values = ['StudentswithGrowthScore', 'MedianSGP']
        csi_summary = pd.pivot(by_grade, index=index, columns=columns, values=values)
        csi_summary.columns = [i[0]+i[1] for i in csi_summary.columns]
        
        ## get median of the whole distribution for each school 
        all_grades = sf.groupby(['SchoolTypeF', 'EntityID']).agg(TotalFAYCount = ('SAISID', 'nunique')
                                                                    ,MedianSGPAllPoints = ('SGP_CCR', 'median'))
        
        ##combine datasets
        csi_summary = pd.concat([csi_summary, all_grades], axis=1).reset_index()
        ## apply n-count
        csi_summary.loc [csi_summary.TotalFAYCount<self.n_count, 'MedianSGPAllPoints'] = np.nan
        
        ##--------------------------- calculate growth points by model type

        csi_summary['GrowthPoints'] = (csi_summary.SchoolTypeF.map(self.growth_weights_by_model)) * csi_summary.MedianSGPAllPoints
        csi_summary['Growth'] = csi_summary['GrowthPoints'].copy()
        csi_summary['GrowthPoints'+str(self.fiscal_year)] = csi_summary['GrowthPoints'].copy()
        
        return csi_summary
    

#%% bring in the static file
# fy= 2023
# run = 'Prelimv4'
# staticfile_raw = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive').read_table(table_name ='StaticFile')
# self = GROWTH(fy, run=run)

# #%% calculate growth component
# fy= 2022
# self = GROWTH(fy)
# csi_summary, csi_drilldown, atsi = self.calculate_component(staticfile_raw)

# #Upload data
# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_growth_drilldown_columns()
# ## select cols and produce ssi.growth
# csi_drilldown = csi_drilldown[cols]



# data = {'Growth': [csi_drilldown, '', 'ssi']
#         ,'CSIGrowth':[csi_summary, 'Prelim', 'Results']
#         ,'ATSIGrowth': [atsi, 'Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][1]
#             ,schema = data[i][2]
#             ,database = 'AccountabilityArchive').upload_table_to_db(df=data[i][0], table_name=i)

# #%%upload 2023 table for Mayank
# fy= 2023
# self = GROWTH(fy)
# csi_summary, csi_drilldown, atsi = self.calculate_component(staticfile_raw)
# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_growth_drilldown_columns()
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
#         ,database = 'AccountabilityArchive').upload_table_to_db(df=csi_drilldown, table_name='Growth')
# #%%drop tables
# fy= 2022
# data = {'CSIGrowth':['Prelim', 'Results']
#         ,'Growth': ['', 'ssi']
#         ,'ATSIGrowth': ['Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][0]
#             ,schema = data[i][1]
#             ,database = 'AccountabilityArchive').drop_tables_in_run(i, table_prefix=data[i][0])