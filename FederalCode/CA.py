# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 11:13:48 2023

@author: YFahmy
"""

from COMPONENTS import COMPONENTS
import pandas as pd
from DATABASE import DATABASE
import numpy as np
from TABLES import TABLES
import traceback

class CA(COMPONENTS):
    
    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        
    def calculate_component(self, staticfile):
        ##make sure numeric cols are numeric
        numeric_cols = ['ELAMathWindow', 'StudentGrade', 'ChronicAbsent', 'SAISID', 'SchoolCode', 'SchoolTypeF', 'ADMIntegrity']
        staticfile = self.columns_to_numeric(staticfile, numeric_cols)

        ## Make a federal modeltype column
        # staticfile['SchoolTypeF'] = staticfile.SchoolTypeF.map(self.federal_school_type_map)
        
        ##---------------------------------- select relevant data for CA according to buissness rules
        sf = pd.DataFrame()
        #make a list of relevant cols to keep (including cols needed for ATSI)
        relevant_cols = ['SAISID', 'SchoolCode', 'StudentGrade', 'ChronicAbsent', 'SchoolTypeF'] + self.subgroups
        ## select relevant files from staticfile
        for model, grades in self.ca_grade_map.items():
            #select the models that contain ca exclusivly
            #select the grades relevant to the selected model make sure kids are enrolled during testing window
            ## select Relevant columns only
            ca_mask = (staticfile.SchoolTypeF == model) & (staticfile.StudentGrade.isin(grades)) & (staticfile.ADMIntegrity==1)
            # ca_mask = ca_mask & (staticfile.ELAMathWindow==1)
            sf_temp = staticfile.loc[ca_mask, relevant_cols].copy()
            ## change SchoolCode to EntityID
            sf_temp.rename({'SchoolCode':'EntityID'}, inplace=True, axis=1)
            ## deduplicate to keep one record per student per school
            sf_temp = sf_temp[~sf_temp.duplicated(['SAISID', 'EntityID'])]
            #build up the filtered staticfile
            sf = pd.concat([sf, sf_temp], axis=0)
        #clean up cache
        del sf_temp
        
        ## get growth weights for each model from component dict
        self.ca_weights_by_model = {}
        for model in sf.SchoolTypeF.unique():
            weight = self.federal_model_weights[model]['CA']
            self.ca_weights_by_model[model] = weight/100
            
        ##replace names in relevant subgroups for consistency
        sf.replace(self.atsi_subgroups_name_changes, inplace=True)
        ## add all column to aggregate all
        sf['All'] = 'All'
        
        #get results tables
        # csi_summary has an extra column used for n-count called StudentCount
        csi_summary, csi_drilldown = self.get_csi_tables(sf)
        atsi_1, atsi_2 = self.get_atsi_tables(sf)
        
        ##merge Atsi tables
        atsi = pd.merge(atsi_1, atsi_2, on=['EntityID', 'SchoolTypeF'], how='outer')
        
        ##round all numeric cols
        data = [csi_summary, csi_drilldown, atsi]
        csi_summary, csi_drilldown, atsi = self.round_numeric_cols(*data)
        
        ##add missing cols
        csi_drilldown['FiscalYear'] = self.fiscal_year
        csi_drilldown['FederalModel'] = csi_drilldown.SchoolTypeF.map(self.federal_school_type_map)
        
        return csi_summary, csi_drilldown, atsi
        
        
    def get_atsi_tables(self, sf):
        ##===================================================== Make tables for ATSI
        #make an all col to aggregate for the all col in ATSI
        subgroups_w_all = self.subgroups + ['All']
        
        ## aggregate by subgroup and calc percent CA and count
        data = pd.DataFrame()
        for subg in subgroups_w_all:
            ##---------- get aggregations at school level
            by_subgroup = sf.groupby(['SchoolTypeF', 'EntityID', subg]).agg(NumCA= ('ChronicAbsent', 'sum')
                                                                               ,PctCA = ('ChronicAbsent', 'mean')
                                                                               ,StudentCount = ('SAISID', 'nunique')).reset_index()
            by_subgroup['PctCA'] = (by_subgroup['PctCA']*100).round()
            #remove un intended aggregates and rename col
            by_subgroup = by_subgroup[by_subgroup[subg] !=0]
            by_subgroup = by_subgroup[by_subgroup[subg] !='U'].copy()
            by_subgroup.rename({subg : 'SubGroup'}, inplace=True, axis=1)
            
            #----------- calculate CA points
            by_subgroup['CA'] = by_subgroup.SchoolTypeF.map(self.ca_weights_by_model) * (100-by_subgroup.PctCA)
            #apply n-count
            by_subgroup.loc[ by_subgroup.StudentCount<self.n_count, 'CA'] = np.nan
            ## add subgroup data
            data = pd.concat([data, by_subgroup])
            
        ##----------------- make a copy of pctCA for current data in istoric table of atsi on CA
        hist_col_name = 'FY'+str(self.fiscal_year)+'CA'
        data[hist_col_name] = data.PctCA.copy()
            
        ##-------------- create wide data
        index = ['SchoolTypeF', 'EntityID']
        values = ['CA', 'NumCA', 'PctCA', hist_col_name]
        atsi = pd.pivot(data, index=index, values=values, columns='SubGroup')
        # resolve multi index
        atsi.columns = [i[0]+i[1] for i in atsi.columns]
        
        #-------------- separate out atsi_1 and atsi_2
        atsi_1 = atsi.iloc[:, :11].copy().reset_index()
        atsi_2 = atsi.iloc[:, 11:].copy().reset_index()
        
        #-------------- bring in historic data for atsi 2
# =============================================================================
#         historic_atsi = self.get_historic_atsi_ca()
#         atsi_2 = pd.merge(atsi_2, historic_atsi, on='EntityID', how='left' )
# =============================================================================
        
        return atsi_1, atsi_2
        
    def get_historic_atsi_ca(self):
        
        ##---------------------------------------------------------------- get historic data
        
        
        ##connect to db
        db = DATABASE(fiscal_year = self.previous_fiscal_year
                      ,run = 'Final'
                      ,schema = 'Results'
                      ,database = 'AccountabilityArchive')
        try:
            historic_atsi = {}
            for i, j in self.py_atsi_tables.items():
                #read historic drilldown historic_atsi
                temp = db.read_table(table_name = j)
                historic_atsi[i] = temp
        except Exception:
            traceback.print_exc()
            print('')
        
        #=====================================  historic_atsi wrangling
        ## in this case we are only interested in atsi_2
        historic_atsi = historic_atsi['atsi_2'].copy()
        ## pull entity id in index
        historic_atsi = historic_atsi.set_index('EntityID')
        
        ##get columns of interest
        cols = []
        for i in range(self.fiscal_year-3, self.fiscal_year):
            regex = 'FY'+str(i)+'CA'
            cols.append(historic_atsi.columns.str.contains(regex))
        historic_atsi = historic_atsi.loc[:, cols[0] | cols[1] | cols[2]].reset_index()
        ## convert cols to numeric historic_atsi
        for i in historic_atsi.columns:
            historic_atsi[i] = historic_atsi[i].astype(str).str.replace('%','')
            historic_atsi[i] = pd.to_numeric(historic_atsi[i], errors='coerce')
            
        return historic_atsi
        
    def get_csi_tables(self, sf):
        ##===================================================== Make Drilldowns
        #-----------group and start calculations
        cols_to_grp = ['SchoolTypeF', 'EntityID', 'StudentGrade']
        ca_long = sf.groupby(cols_to_grp).agg(NumberChronicallyAbsent = ('ChronicAbsent', 'sum')
                                              ,PercentChronicallyAbsent = ('ChronicAbsent', 'mean')).reset_index()
        #get number and percent of chronic absent kids at each grade level in one row
        index = ['SchoolTypeF', 'EntityID']
        values=['NumberChronicallyAbsent', 'PercentChronicallyAbsent']
        ca = pd.pivot(ca_long, index=index, values=values, columns='StudentGrade')
        #resolve multi-index
        ca.columns = [i[0]+str(int(i[1])) for i in ca.columns]
        
        ##--------- replicate Pct cols (for GUI)
        var = 'PercentChronicallyAbsent'
        cols = [i for i in ca.columns if var in i]
        ca[cols] = (ca[cols]*100).copy()
        replica = ca[cols].copy()
        #modify col names to match sql tables
        new_name = 'PctChronicallyAbsent'
        new_cols = [new_name+str(self.fiscal_year)+'Grade'+i[len(var):] for i in cols]
        replica.columns = new_cols
        
        ##---------- get the data for the all cols
        cols_to_grp = ['SchoolTypeF', 'EntityID']
        ca_all = sf.groupby(cols_to_grp).agg(NumberChronicallyAbsentAll = ('ChronicAbsent', 'sum')
                                              ,PercentChronicallyAbsentAll = ('ChronicAbsent', 'mean'))
        ## replicate pct col for sql tables
        ca_all.PercentChronicallyAbsentAll = (ca_all.PercentChronicallyAbsentAll*100).round()
        ca_all['PctChronicallyAbsent'+str(self.fiscal_year)+'All'] = ca_all.PercentChronicallyAbsentAll.copy()
        
        #merge all drilldown dfs
        ca = pd.concat([ca, replica, ca_all], axis=1)        
        ca.reset_index(inplace=True)
        
        #---------- get and merge historic data
        # historic_dd_ca, historic_summary_ca = self.get_historic_csi_ca(sf)
        historic_dd_ca = self.get_historic_csi_ca(sf)
        ## merge data
        csi_drilldown = pd.merge(ca, historic_dd_ca, on='EntityID', how='left')
        
        ##==================================================== make CSI_summary
        #points and pct
        ##-------- calculate Pct chronically absent and points
        cols_to_grp = ['SchoolTypeF', 'EntityID']
        csi_summary = sf.groupby(cols_to_grp).agg(StudentCount = ('SAISID', 'nunique')
                                                   ,ChronicAbsenteeism10Pct = ('ChronicAbsent', 'mean')).reset_index()
        csi_summary['ChronicAbsenteeism10Pct'] = (csi_summary['ChronicAbsenteeism10Pct']*100).round()
        csi_summary['ChronicAbsenteeism10Points'] = (100-csi_summary.ChronicAbsenteeism10Pct)*csi_summary.SchoolTypeF.map(self.ca_weights_by_model)
        ##apply n-count
        csi_summary.loc[csi_summary.StudentCount<self.n_count, 'ChronicAbsenteeism10Points'] = np.nan
        #make a duplicate points cols for summary page
        csi_summary['ChronicAbsenteeism'] = csi_summary['ChronicAbsenteeism10Points'].copy()
        csi_summary['ChronicAbsenteeismPoints'+str(self.fiscal_year)]= csi_summary['ChronicAbsenteeism'].copy()
        #rename cols to include current year
        cols_rename = {'ChronicAbsenteeism10Pct':'ChronicAbsenteeism10Pct'+str(self.fiscal_year)
                       ,'ChronicAbsenteeism10Points':'ChronicAbsenteeism10Points'+str(self.fiscal_year)}
        csi_summary.rename(cols_rename, inplace=True, axis=1)
        
        ##-------- bring in historic summary page data
# =============================================================================
#         csi_summary = pd.merge(csi_summary, historic_summary_ca, on='EntityID', how='left')
# =============================================================================
        
        return csi_summary, csi_drilldown
        
    def get_historic_csi_ca(self, sf):
        ##---------------------------------------------------------------- get historic data
        ##connect to db
        db = DATABASE(fiscal_year = self.previous_fiscal_year
                      ,run = ''
                      ,schema = 'ssi'
                      ,database = 'REDATA_UAT')
        
        historic_dd_ca = pd.DataFrame()
        # historic_summary_ca = pd.DataFrame()
        for i in sf.SchoolTypeF.unique():
            #read historic drilldown data
            if i in self.py_ca_dd_tables_by_model.keys(): # check that model i was used historically
                dd_data = db.read_table(table_name = self.py_ca_dd_tables_by_model[i])
                historic_dd_ca = pd.concat([historic_dd_ca, dd_data])
            # #read historic summary data
            # sum_data = db.read_table(table_name = self.py_summary_tables_by_model[i])
            # #merge data
            # historic_summary_ca = pd.concat([historic_summary_ca, sum_data])
        
        ## pull entity ids into index
        historic_dd_ca = historic_dd_ca.set_index('EntityID')
        # historic_summary_ca = historic_summary_ca.set_index('EntityID')
        
        #===================================== drilldown data wrangling
        ## get cols with PctChronicallyAbsent[YR] in title over past 3 years to carry over
        cols = []
        regex = 'PctChronicallyAbsent'
        for i in range(self.fiscal_year-3, self.fiscal_year):
            cols.append(historic_dd_ca.columns.str.contains(regex+str(i)))
        historic_dd_ca = historic_dd_ca.loc[:, cols[0] | cols[1] | cols[2]].reset_index()
        ## convert cols to numeric data
        for i in historic_dd_ca.columns:
            historic_dd_ca[i] = historic_dd_ca[i].astype(str).str.replace('%','')
            historic_dd_ca[i] = pd.to_numeric(historic_dd_ca[i], errors='coerce')
            
        #===================================== summary data wrangling
        regex = 'ChronicAbsenteeismPoints('
        for i in range(self.fiscal_year-3, self.fiscal_year):
            regex = regex + str(i)
            if i != self.fiscal_year-1:
                regex = regex + '|'
            else:
                regex = regex + ')'
        ## get the historic points cols
        # historic_summary_ca = historic_summary_ca.loc[:,historic_summary_ca.columns.str.contains(regex)].reset_index()
        #convert to numeric
        # for i in historic_summary_ca.columns:
        #     historic_summary_ca[i] = historic_summary_ca[i].astype(str).str.replace('%','')
        #     historic_summary_ca[i] = pd.to_numeric(historic_summary_ca[i], errors='coerce')
        
        # return historic_dd_ca, historic_summary_ca
        return historic_dd_ca

 #%% bring in the static file   
# self = CA(2023)

# fy= 2023
# run = 'Prelimv4'
# staticfile = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive').read_table(table_name ='StaticFile')

# csi_summary, csi_drilldown, atsi = self.calculate_component(staticfile)


# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_ca_drilldown_columns()
# ## select cols and produce ssi.CA
# #find and select cols missing from results tables
# missing_cols = []
# for i in cols:
#     if i not in csi_drilldown.columns:
#         missing_cols.append(i)
#         print(i, 'is missing from results')
# ##make empty cols to replace missing cols (temporarily)
# csi_drilldown.loc[:,missing_cols] = np.nan

# ##--------------------------------------- select all returned cols and upload to db
# csi_drilldown = csi_drilldown[cols]


# data = {'ChronicAbsenteeism': [csi_drilldown, '', 'ssi']
#         ,'CSIChronicAbsenteeism':[csi_summary, 'Prelim', 'Results']
#         ,'ATSIChronicAbsenteeism': [atsi, 'Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][1]
#             ,schema = data[i][2]
#             ,database = 'AccountabilityArchive').upload_table_to_db(df=data[i][0], table_name=i)
   
# #%%upload 2023 table for Mayank
# fy= 2023
# self = CA(fy)
# csi_summary, csi_drilldown, atsi = self.calculate_component(staticfile)
# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_ca_drilldown_columns()
# ## select cols and produce ssi.CA
# #find and select cols missing from results tables
# missing_cols = []
# for i in cols:
#     if i not in csi_drilldown.columns:
#         missing_cols.append(i)
#         print(i, 'is missing from results and will be recreated as an empty column')
# ##make empty cols to replace missing cols (temporarily)
# csi_drilldown.loc[:,missing_cols] = np.nan
# ## select all returned cols and upload to db
# csi_drilldown = csi_drilldown[cols]

# DATABASE(fiscal_year = fy
#         ,run = ''
#         ,schema = 'ssi'
#         ,database = 'AccountabilityArchive').upload_table_to_db(df=csi_drilldown, table_name='ChronicAbsenteeism')

# #%%drop tables
# fy= 2022
# data = {'CSIChronicAbsenteeism':['Prelim', 'Results']
#         ,'ChronicAbsenteeism': ['', 'ssi']
#         ,'ATSIChronicAbsenteeism': ['Prelim', 'Results']}
# try:
#     for i in data.keys():
#         DATABASE(fiscal_year = fy
#                 ,run = data[i][0]
#                 ,schema = data[i][1]
#                 ,database = 'AccountabilityArchive').drop_tables_in_run(i, table_prefix=data[i][0])
# except :
#     pass
